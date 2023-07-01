using Newtonsoft.Json.Linq;
using Dyconit.Helper;
using Newtonsoft.Json;
using Confluent.Kafka;
using System.Net.Sockets;
using System.Net;
using Serilog;
using Serilog.Events;

namespace Dyconit.Overlord
{
    public class DyconitAdmin
    {
        public readonly IAdminClient _adminClient;
        private readonly int _listenPort;
        private readonly AdminClientConfig _adminClientConfig;
        private RootObject _dyconitCollections;
        private List<ConsumeResult<Null, string>> ?_localData = new List<ConsumeResult<Null, string>>();
        private List<ConsumeResult<Null, string>> ?_receivedData;
        private bool _synced;

        public DyconitAdmin(string bootstrapServers, int adminPort, JObject conitCollection)
        {
            ConfigureLogging();

            _listenPort = adminPort;
            _adminClientConfig = new AdminClientConfig
            {
                BootstrapServers = bootstrapServers,
            };
            _adminClient = new AdminClientBuilder(_adminClientConfig).Build();
            _dyconitCollections = CreateDyconitCollections(conitCollection, adminPort);

            Log.Information("Created DyconitAdmin with bootstrap servers {BootstrapServers} and admin port {AdminPort}", bootstrapServers, adminPort);
            Log.Debug("DyconitCollections: {DyconitCollections}", JsonConvert.SerializeObject(_dyconitCollections, Formatting.Indented));

            ListenForMessagesAsync();
        }

        static public void ConfigureLogging()
        {
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .WriteTo.Console()
                .WriteTo.File("log.txt")
                .CreateLogger();
        }

        private async void ListenForMessagesAsync()
        {
            Log.Information("Listening for messages on port {Port}", _listenPort);

            _ = SendSyncCountAsync();
            var listener = new TcpListener(IPAddress.Any, _listenPort);
            listener.Start();

            while (true)
            {
                var client = await listener.AcceptTcpClientAsync().ConfigureAwait(false);
                var reader = new StreamReader(client.GetStream());
                var message = await reader.ReadToEndAsync().ConfigureAwait(false);

                // Parse message and act accordingly
                await ParseMessageAsync(message);

                reader.Close();
                client.Close();

            }
        }

        private Task ParseMessageAsync(string message)
        {

            var messageObject = JObject.Parse(message);
            var eventType = messageObject["eventType"]?.ToString();

            Log.Debug("Event Type: {EventType}", eventType);

            switch (eventType)
            {
                case "newNodeEvent":
                    return HandleNewNodeEventAsync(messageObject);
                case "removeNodeEvent":
                    return HandleRemoveNodeEventAsync(messageObject);
                case "updateConitEvent":
                    return HandleUpdateConitEventAsync(messageObject);
                case "heartbeatEvent":
                    return HandleHeartbeatEventAsync(messageObject);
                case "syncRequest":
                    return HandleSyncRequestAsync(messageObject);
                case "syncResponse":
                    return HandleSyncResponseAsync(messageObject);
                default:
                    Log.Warning("Unknown eventType: {EventType}", eventType);
                    return Task.CompletedTask;
            }
        }

        private Task HandleSyncResponseAsync(JObject messageObject)
        {
            var data = messageObject["data"]?.ToString();
            var collectionName = messageObject["collection"]?.ToString();
            var senderPort = messageObject["port"]?.ToObject<int>();

            // check for null
            if (data == null || collectionName == null || senderPort == null)
            {
                Log.Warning("SyncResponse data, collectionName, or senderPort is null");
                return Task.CompletedTask;
            }

            // deserialize the data
            List<ConsumeResultWrapper<Null, string>> deserializedList = JsonConvert.DeserializeObject<List<ConsumeResultWrapper<Null, string>>>(data)!;

            // Convert back to original format
            _receivedData = deserializedList.Select(dw => new ConsumeResult<Null, string>
            {
                Topic = dw.Topic ?? string.Empty, // Handle possible null value
                Partition = new Partition(dw.Partition),
                Offset = new Offset(dw.Offset),
                Message = new Message<Null, string>
                {
                    Key = dw.Message?.Key!, // Handle possible null value
                    Value = dw.Message?.Value ?? string.Empty, // Handle possible null value
                    Timestamp = dw.Message?.Timestamp ?? default, // Handle possible null value
                    Headers = new Headers(),
                },
                IsPartitionEOF = dw.IsPartitionEOF
            }).ToList();

            if (_localData != null)
            {
                UpdateLocalData(_localData, collectionName);
            }
            else
            {
                Log.Error("Local data is null");
            }


            // update the last time since pull for the sender port and collection combination
            var collection = _dyconitCollections.Collections
                ?.FirstOrDefault(c => c.Name == collectionName);
            if (collection != null)
            {
                var sender = collection.Nodes
                    ?.FirstOrDefault(s => s.Port == senderPort);
                if (sender != null)
                {
                    sender.LastTimeSincePull = DateTime.Now;
                }
            }

            // update the syn counter for the _listenPort and collection combination
            var node = _dyconitCollections.Collections
                ?.FirstOrDefault(c => c.Name == collectionName)
                ?.Nodes
                ?.FirstOrDefault(n => n.Port == _listenPort);

            if (node != null)
            {
                node.SyncCount++;
            }

            return Task.CompletedTask;
        }

        private SyncResult UpdateLocalData(List<ConsumeResult<Null, string>> localData, string collectionName)
        {
            // Filter localData to keep only the items with matching topic
            if (_receivedData != null){
                _receivedData = _receivedData.Where(item => item.Topic == collectionName).ToList();
            }

            // check if the topic of the _receivedData is the same as the topic of localData
            if(_receivedData == null || _receivedData.Count() < localData.Count() || _receivedData.First().Topic != localData.First().Topic)
            {
                Log.Debug("Received data is old.. Skipping update");
                _localData = localData;

                SyncResult earlyResult = new SyncResult
                {
                    changed = false || _synced,
                    Data = localData,
                };
                _synced = _synced ? false : _synced;
                return earlyResult;
            }

            // Merge the received data with the local data
            var mergedData = _receivedData.Union(localData, new ConsumeResultComparer()).ToList();

            foreach (var item in localData)
            {
                Log.Debug($"[{_listenPort}] - localData: {item.Message.Value}");
            }
            Log.Debug("--------------------------------------------------");
            foreach (var item in _receivedData)
            {
                Log.Debug($"[{_listenPort}] - _receivedData: {item.Message.Value}");
            }
            Log.Debug("--------------------------------------------------");
            foreach (var item in mergedData)
            {
                Log.Debug($"[{_listenPort}] - mergedData: {item.Message.Value}");
            }

            bool isNotSame = mergedData.Count() != localData.Count();

            Log.Debug("isNotSame: {IsNotSame}, localData.Count: {LocalDataCount}, _receivedData.Count: {ReceivedDataCount}, mergedData.Count: {MergedDataCount}", isNotSame, localData.Count(), _receivedData.Count(), mergedData.Count());

            SyncResult result = new SyncResult
            {
                changed = isNotSame || _synced,
                Data = mergedData,
            };

            _localData = mergedData;
            _synced = _synced ? false : _synced;
            return result;
        }

        private async Task HandleSyncRequestAsync(JObject messageObject)
        {
            Log.Information("Handling syncRequest");
            var syncRequestPort = messageObject["port"]?.ToObject<int>();
            var collectionName = messageObject["collectionName"]?.ToString();

            // check for null
            if (syncRequestPort == null || collectionName == null)
            {
                Log.Warning("SyncRequest port or collectionName is null");
                return;
            }

            List<ConsumeResultWrapper<Null, string>> wrapperList = _localData!.Select(cr => new ConsumeResultWrapper<Null, string>(cr)).ToList();
            string consumeResultWrapped = JsonConvert.SerializeObject(wrapperList, Formatting.Indented);

            var syncResponse = new JObject
            {
                { "eventType", "syncResponse" },
                { "port", _listenPort },
                { "data", consumeResultWrapped },
                { "collection", collectionName}
            };

            await SendMessageOverTcp(syncResponse.ToString(), syncRequestPort.Value);

            Log.Information("Sent syncResponse to port {Port}", syncRequestPort);
            _synced = true;

            // increment the sync for the port and colleciton combination
            var collection = _dyconitCollections.Collections
                ?.FirstOrDefault(c => c.Name == collectionName);
            var node = collection?.Nodes
                ?.FirstOrDefault(n => n.Port == _listenPort);
            if (node != null)
            {
                node.SyncCount++;
            }
        }

        private async Task HandleHeartbeatEventAsync(JObject messageObject)
        {
            Log.Information("Handling heartbeatEvent");

            var heartbeatResponse = new JObject
            {
                ["eventType"] = "heartbeatResponse",
                ["port"] = _listenPort
            };

            var heartbeatResponseString = heartbeatResponse.ToString();
            Log.Debug("Sending heartbeatResponse: {HeartbeatResponse}", heartbeatResponseString);

            await SendMessageOverTcp(heartbeatResponseString, 6666);
        }

        private Task HandleUpdateConitEventAsync(JObject messageObject)
        {
            Log.Information("Handling updateConitEvent}");

            // get the collection name and the corresponding node based on the port
            var collectionName = messageObject["collectionName"]?.ToString();
            var collection = _dyconitCollections.Collections
                ?.FirstOrDefault(c => c.Name == collectionName);

            // get the node based on the port
            var nodePort = messageObject["port"]?.ToObject<int>();
            var node = collection?.Nodes
                ?.FirstOrDefault(n => n.Port == nodePort);

            // update the node's bounds
            node!.Bounds!.Staleness = messageObject["bounds"]!["Staleness"]?.ToObject<int>();
            node.Bounds.NumericalError = messageObject["bounds"]!["NumericalError"]?.ToObject<int>();

            Log.Debug("Updated node: {Node}", JsonConvert.SerializeObject(node, Formatting.Indented));

            return Task.CompletedTask;
        }


        private Task HandleRemoveNodeEventAsync(JObject messageObject)
        {
            Log.Information("Handling removeNodeEvent");

            var removeNodePort = messageObject["port"]?.ToObject<int>();
            var collectionName = messageObject["collectionName"]?.ToString();

            // remove the node from the collection
            var collection = _dyconitCollections.Collections
                ?.FirstOrDefault(c => c.Name == collectionName);
            collection?.Nodes?.RemoveAll(n => n.Port == removeNodePort);

            Log.Debug("Removed node with port {Port} from collection {CollectionName}", removeNodePort, collectionName);
            Log.Debug("Collection: {Collection}", JsonConvert.SerializeObject(collection, Formatting.Indented));

            return Task.CompletedTask;
        }

        private Task HandleNewNodeEventAsync(JObject messageObject)
        {
            Log.Information("Handling newNodeEvent");

            var newNodePort = messageObject["port"]?.ToObject<int>();
            var collectionName = messageObject["collectionName"]?.ToString();

            // create a new node and add it to the collection
            var newNode = new Node
            {
                Port = newNodePort,
                LastHeartbeatTime = DateTime.Now,
                LastTimeSincePull = DateTime.Now,
                SyncCount = 0,
                Bounds = new Bounds
                {
                    Staleness = messageObject["staleness"]?.ToObject<int>(),
                    NumericalError = messageObject["numericalError"]?.ToObject<int>()
                }
            };

            // add the new node to the collection
            var collection = _dyconitCollections.Collections
                ?.FirstOrDefault(c => c.Name == collectionName);
            collection?.Nodes?.Add(newNode);

            Log.Debug("Added node with port {Port} to collection {CollectionName}", newNodePort, collectionName);
            Log.Debug("Collection: {Collection}", JsonConvert.SerializeObject(collection, Formatting.Indented));

            return Task.CompletedTask;

        }

        private async Task SendSyncCountAsync()
        {
            while (true)
            {
                var message = new JObject
                {
                    ["eventType"] = "overheadMessage",
                    ["port"] = _listenPort,
                    ["data"] = new JObject()
                };

                // delay for 5 seconds
                await Task.Delay(20000);
                var syncNodes = new List<Node>();

                // loop through each collection and get the total sync count
                foreach (var collection in _dyconitCollections.Collections ?? Enumerable.Empty<Collection>())
                {
                    var syncCount = 0;

                    foreach (var node in collection.Nodes ?? Enumerable.Empty<Node>())
                    {
                        if (node.SyncCount.HasValue)
                        {
                            syncCount += node.SyncCount.Value;
                            if (node.SyncCount.Value > 0)
                            {
                                syncNodes.Add(node);
                            }
                        }
                    }

                    // add the sync count to the message
                    message["data"]![collection.Name ?? string.Empty] = syncCount;
                }

                await SendMessageOverTcp(message.ToString(), 6666);

                // Reset the sync count
                foreach (var node in syncNodes)
                {
                    node.SyncCount = 0;
                }
            }
        }


        private RootObject CreateDyconitCollections(JObject conitCollection, int adminPort)
        {
            var dyconitCollections = new RootObject
            {
                Collections = new List<Collection>()
            };
            if (conitCollection != null)
            {
                foreach (var conit in conitCollection)
                {
                    var collection = new Collection
                    {
                        Name = conit.Key,
                        Nodes = new List<Node>(),
                        Rules = new List<Rule>(),
                    };
                    dyconitCollections.Collections.Add(collection);

                    var node = new Node
                    {
                        Port = adminPort,
                        LastHeartbeatTime = DateTime.Now,
                        LastTimeSincePull = DateTime.Now,
                        SyncCount = 0,
                        Bounds = new Bounds
                        {
                            Staleness = conit.Value?["Staleness"]?.ToObject<int>(),
                            NumericalError = conit.Value?["NumericalError"]?.ToObject<int>()
                        }
                    };
                    collection.Nodes.Add(node);
                }
            }
            return dyconitCollections;
        }

        private async Task SendMessageOverTcp(string message, int port)
        {
            try
            {
                using (var client = new TcpClient())
                {
                    client.Connect("localhost", port);

                    using (var stream = client.GetStream())
                    using (var writer = new StreamWriter(stream))
                    {
                        await writer.WriteLineAsync(message);
                        await writer.FlushAsync();
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Failed to send message over TCP");
            }
        }

        public async Task<SyncResult> BoundStaleness(DateTime consumedTime, List<ConsumeResult<Null, string>> localData, string collectionName)
        {
            Log.Information("Bounding staleness for collection {CollectionName} and port {Port}", collectionName, _listenPort);

            // Check if we have received new data from an other node since the last time we checked
            SyncResult result = UpdateLocalData(localData, collectionName);

            // retrieve the staleness bound for the collection name and port combination
            var stalenessBound = _dyconitCollections.Collections
                ?.FirstOrDefault(c => c.Name == collectionName)
                ?.Nodes
                ?.FirstOrDefault(n => n.Port == _listenPort)
                ?.Bounds
                ?.Staleness;

            // if the staleness bound is null, we can't do anything
            if (!stalenessBound.HasValue)
            {
                Log.Warning("Staleness bound is null for collection {CollectionName} and port {Port}", collectionName, _listenPort);
                return result;
            }

            // retrieve all the other nodes in the collection
            var otherNodes = _dyconitCollections.Collections
                ?.FirstOrDefault(c => c.Name == collectionName)
                ?.Nodes
                ?.Where(n => n.Port != _listenPort)
                ?.ToList();

            // if there are no other nodes, we can't do anything
            if (otherNodes == null || !otherNodes.Any())
            {
                Log.Warning("No other nodes found for collection {CollectionName} and port {Port}", collectionName, _listenPort);
                return result;
            }

            Log.Debug("other nodes: {OtherNodes}", JsonConvert.SerializeObject(otherNodes, Formatting.Indented));

            // Compare the consume time with the LastTimeSincePull for each node.
            // If the difference is greater than the staleness bound, we need to pull data from the other node
            foreach (var node in otherNodes)
            {
                var timeSincePull = consumedTime - node.LastTimeSincePull;

                Log.Debug("Consumed time: {ConsumedTime}", consumedTime);
                Log.Debug("Last time since pull: {LastTimeSincePull}", node.LastTimeSincePull);
                Log.Debug("Difference for node with port {Port}: {TimeSincePull}", node.Port, timeSincePull!.Value.TotalMilliseconds);
                Log.Debug("Staleness bound: {StalenessBound}", stalenessBound.Value);

                if (timeSincePull.HasValue && timeSincePull.Value.TotalMilliseconds > stalenessBound.Value)
                {
                    Log.Information("Pulling data from node with port {Port}", node.Port);

                    // send a pull request to the other node
                    var message = new JObject
                    {
                        ["eventType"] = "syncRequest",
                        ["port"] = _listenPort,
                        ["collectionName"] = collectionName,
                    };

                    await SendMessageOverTcp(message.ToString(), node.Port!.Value);

                    // update the LastTimeSincePull for the node
                    node.LastTimeSincePull = DateTime.Now;
                }
            }

            // Return the local data
            return result;
        }
    }
}
