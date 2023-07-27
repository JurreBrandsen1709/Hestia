// TODO: Toevoegen latency van berichten (events en dyconit events)
// TODO: Toevoegen wat de bounds zijn elk moment een bericht wordt consumed.
// Ik heb het namelijk over latency in de NFRs. Dus daar moet ik eigenlijk ook op testen.
// Wat ik voor me zie is een coole grafiek waarbij je de bounds hebt en dan daaronder de werkelijke latency.

using Newtonsoft.Json.Linq;
using Dyconit.Helper;
using Newtonsoft.Json;
using Confluent.Kafka;
using System.Net.Sockets;
using System.Net;
using Serilog;
using Serilog.Events;
using Confluent.Kafka.Admin;

namespace Dyconit.Overlord
{
    public class DyconitAdmin
    {
        public readonly IAdminClient _adminClient;
        private readonly int _listenPort;
        private readonly string _host;
        private readonly AdminClientConfig _adminClientConfig;
        private RootObject _dyconitCollections;
        private List<ConsumeResult<Null, string>> ?_localData = new List<ConsumeResult<Null, string>>();
        private List<ConsumeResult<Null, string>> ?_receivedData;
        private bool _synced;
        private HashSet<int> _syncResponses = new HashSet<int>();
        private Dictionary<string, List<ConsumeResult<Null, string>>> _buffer = new Dictionary<string, List<ConsumeResult<Null, string>>>();

        public DyconitAdmin(string bootstrapServers, int adminPort, JObject conitCollection, string host)
        {
            ConfigureLogging();

            _listenPort = adminPort;
            _host = host;
            _adminClientConfig = new AdminClientConfig
            {
                BootstrapServers = bootstrapServers,
            };
            _adminClient = new AdminClientBuilder(_adminClientConfig).Build();
            _adminClient.CreateTopicsAsync(new List<TopicSpecification> { new TopicSpecification { Name = "trans_topic_priority", NumPartitions = 1, ReplicationFactor = 1 } });
            _adminClient.CreateTopicsAsync(new List<TopicSpecification> { new TopicSpecification { Name = "trans_topic_normal", NumPartitions = 1, ReplicationFactor = 1 } });
            _adminClient.CreateTopicsAsync(new List<TopicSpecification> { new TopicSpecification { Name = "topic_normal", NumPartitions = 1, ReplicationFactor = 1 } });
            _adminClient.CreateTopicsAsync(new List<TopicSpecification> { new TopicSpecification { Name = "topic_priority", NumPartitions = 1, ReplicationFactor = 1 } });
            _dyconitCollections = CreateDyconitCollections(conitCollection, adminPort, host);
            ListenForMessagesAsync();
        }

        static public void ConfigureLogging()
        {
            string logFileName = $"log_{DateTime.Now:yyyyMMdd_HHmmss}.txt";

            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .WriteTo.Console()
                .WriteTo.File(logFileName, rollingInterval: RollingInterval.Infinite)
                .CreateLogger();
        }

        private async void ListenForMessagesAsync()
        {

            _ = SendSyncCount();
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
                return Task.CompletedTask;
            }

            // deserialize the data
            List<ConsumeResultWrapper<Null, string>> deserializedList = JsonConvert.DeserializeObject<List<ConsumeResultWrapper<Null, string>>>(data)!;

            // Convert back to original format
            _receivedData = deserializedList.Select(dw => new ConsumeResult<Null, string>
            {
                Topic = dw.Topic ?? string.Empty, // Handle possible null value
                Partition = dw.Partition,
                Offset = dw.Offset,
                Message = new Message<Null, string>
                {
                    Key = dw.Message?.Key!, // Handle possible null value
                    Value = dw.Message?.Value ?? string.Empty, // Handle possible null value
                    Timestamp = dw.Message?.Timestamp ?? default, // Handle possible null value
                    Headers = dw.Message?.Headers != null ? ConvertToKafkaHeaders(dw.Message.Headers) : null
                },
                IsPartitionEOF = dw.IsPartitionEOF
            }).ToList();

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

            _syncResponses.Add(senderPort.Value);

            Log.Information("Handled sync request from {SenderPort} for data {collectionName}", senderPort, collectionName);

            return Task.CompletedTask;
        }

        private Headers ConvertToKafkaHeaders(List<HeaderWrapper> headerWrappers)
        {
            var headers = new Headers();
            foreach (var headerWrapper in headerWrappers)
            {
                headers.Add(headerWrapper.Key, headerWrapper.Value);
            }
            return headers;
        }

        private SyncResult UpdateLocalData(List<ConsumeResult<Null, string>> localData, string collectionName)
        {
            _localData = localData;
            var topic = localData.First().Topic;

            if (_receivedData == null)
            {

                return new SyncResult
                {
                    changed = false,
                    Data = _localData,
                };
            }

            if (_receivedData.Count == 0)
            {

                if (_buffer.ContainsKey(topic))
                {
                    _receivedData = _buffer[topic];

                    _buffer.Remove(topic);
                }
                else
                {
                    return new SyncResult
                    {
                        changed = false,
                        Data = _localData,
                    };
                }
            }

            if (_receivedData.First().Topic != localData.First().Topic)
            {
                if (_buffer.ContainsKey(topic))
                {
                    _receivedData = _buffer[topic];
                    _buffer.Remove(topic);
                }
                else
                {
                    var key = _receivedData.First().Topic;

                    if (_buffer.ContainsKey(key))
                    {
                        _buffer[key] = _receivedData;
                    }
                    else
                    {
                        _buffer.Add(key, _receivedData);
                    }

                    return new SyncResult
                    {
                        changed = false,
                        Data = _localData,
                    };
                }
            }

            // check if the topic of the _receivedData is the same as the topic of localData
            if (_receivedData == null || _receivedData.Count < localData.Count)
            {
                SyncResult earlyResult = new SyncResult
                {
                    changed = false || _synced,
                    Data = _localData,
                };
                _synced = _synced ? false : _synced;
                return earlyResult;
            }

            // Merge the received data with the local data
            var mergedData = _receivedData.Union(localData, new ConsumeResultComparer()).ToList();

            // foreach (var item in localData)
            // {
            //     Log.Debug($"[{_listenPort}] - localData: {item.Message.Value}");
            // }
            // Log.Debug("--------------------------------------------------");
            // foreach (var item in _receivedData)
            // {
            //     Log.Debug($"[{_listenPort}] - _receivedData: {item.Message.Value}");
            // }
            // Log.Debug("--------------------------------------------------");
            // foreach (var item in mergedData)
            // {
            //     Log.Debug($"[{_listenPort}] - mergedData: {item.Message.Value}");
            // }

            bool isNotSame = mergedData.Count() != localData.Count();

            Log.Debug("isNotSame: {IsNotSame}, localData.Count: {LocalDataCount}, _receivedData.Count: {ReceivedDataCount}, mergedData.Count: {MergedDataCount}", isNotSame, localData.Count(), _receivedData.Count(), mergedData.Count());

            // check if the merged data has the same topic as the collection name. if this is not the case, store the merged data in the buffer
            if (mergedData.First().Topic != collectionName)
            {
                if (_buffer.ContainsKey(topic))
                {
                    _buffer[topic] = mergedData;
                }
                else
                {
                    _buffer.Add(topic, mergedData);
                }

                return new SyncResult
                {
                    changed = false,
                    Data = _localData,
                };
            }

            SyncResult result = new SyncResult
            {
                changed = isNotSame || _synced,
                Data = mergedData
            };

            _localData = mergedData;
            _synced = _synced ? false : _synced;

            // Clear the received data
            _receivedData = null;
            return result;
        }

        private async Task HandleSyncRequestAsync(JObject messageObject)
        {

            var syncRequestPort = messageObject["port"]?.ToObject<int>();
            var collectionName = messageObject["collectionName"]?.ToString();
            var syncRequestHost = messageObject["host"]?.ToString();

            // check for null
            if (syncRequestPort == null || collectionName == null)
            {
                Log.Error("SyncRequest port or collectionName is null");
                return;
            }

            List<ConsumeResultWrapper<Null, string>> wrapperList = _localData!.Select(cr => new ConsumeResultWrapper<Null, string>(cr)).ToList();
            string consumeResultWrapped = JsonConvert.SerializeObject(wrapperList, Formatting.Indented);

            var syncResponse = new JObject
            {
                { "eventType", "syncResponse" },
                { "port", _listenPort },
                { "host", _host},
                { "data", consumeResultWrapped },
                { "collection", collectionName}
            };

            Log.Information("Sending Sync Request to port {Port} for data in {collectionName}", syncRequestPort.Value, collectionName);

            await SendMessageOverTcp(syncResponse.ToString(), syncRequestPort.Value, syncRequestHost!);

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
            var heartbeatResponse = new JObject
            {
                ["eventType"] = "heartbeatResponse",
                ["port"] = _listenPort,
                ["host"] = _host
            };

            var heartbeatResponseString = heartbeatResponse.ToString();
            await SendMessageOverTcp(heartbeatResponseString, 6666, "app1");
        }

        private Task HandleUpdateConitEventAsync(JObject messageObject)
        {
            // Get the collection name and the corresponding node based on the port
            var collectionName = messageObject["collectionName"]?.ToString();
            var collection = _dyconitCollections.Collections
                ?.FirstOrDefault(c => c.Name == collectionName);

            // Get the node based on the port
            var nodePort = messageObject["port"]?.ToObject<int>();
            var node = collection?.Nodes
                ?.FirstOrDefault(n => n.Port == nodePort);

            // Update the node's bounds
            var newStaleness = messageObject["bounds"]?["Staleness"]?.ToObject<double>();
            if (newStaleness != null && node != null && node.Bounds != null)
            {
                node.Bounds.Staleness = newStaleness;
            }

            var newNumericalError = messageObject["bounds"]?["NumericalError"]?.ToObject<double>();
            if (newNumericalError != null && node != null && node.Bounds != null)
            {
                node.Bounds.NumericalError = newNumericalError;
            }

            return Task.CompletedTask;
        }



        private Task HandleRemoveNodeEventAsync(JObject messageObject)
        {
            var removeNodePort = messageObject["port"]?.ToObject<int>();
            var collectionName = messageObject["collectionName"]?.ToString();

            // remove the node from the collection
            var collection = _dyconitCollections.Collections
                ?.FirstOrDefault(c => c.Name == collectionName);
            collection?.Nodes?.RemoveAll(n => n.Port == removeNodePort);

            return Task.CompletedTask;
        }

        private Task HandleNewNodeEventAsync(JObject messageObject)
        {
            var newNodePort = messageObject["port"]?.ToObject<int>();
            var collectionName = messageObject["collectionName"]?.ToString();
            var host = messageObject["host"]?.ToString();

            // create a new node and add it to the collection
            var newNode = new Node
            {
                Port = newNodePort,
                Host = host,
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

            return Task.CompletedTask;

        }

        private async Task SendSyncCount()
        {
            while (true)
            {
                var startTime = DateTime.UtcNow;

                await SendSyncCountAsync();

                var endTime = DateTime.UtcNow;
                var elapsed = endTime - startTime;
                var delay = TimeSpan.FromSeconds(20) - elapsed;

                if (delay > TimeSpan.Zero)
                {
                    await Task.Delay(delay);
                }
            }
        }

        private async Task SendSyncCountAsync()
        {
            var message = new JObject
            {
                ["eventType"] = "overheadMessage",
                ["port"] = _listenPort,
                ["host"] = _host,
                ["data"] = new JObject()
            };

            var syncNodes = new List<Node>();

            // loop through each collection and get the total sync count
            foreach (var collection in _dyconitCollections.Collections ?? Enumerable.Empty<Collection>())
            {
                double syncCount = 0;

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
                Log.Information($"Collection name: {collection.Name} overhead throughput {syncCount} messages/s");
            }

            await SendMessageOverTcp(message.ToString(), 6666, "app1");

            // Reset the sync count
            foreach (var node in syncNodes)
            {
                node.SyncCount = 0;
            }
        }


        private RootObject CreateDyconitCollections(JObject conitCollection, int adminPort, string host)
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
                        Host = host,
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

        private async Task SendMessageOverTcp(string message, int port, string host)
        {
            try
            {
                using (var client = new TcpClient())
                {
                    client.Connect(host, port);

                    using (var stream = client.GetStream())
                    using (var writer = new StreamWriter(stream))
                    {
                        await writer.WriteLineAsync(message);
                        await writer.FlushAsync();
                    }
                }
            }
            catch (Exception)
            {
                // remove this node from every collection
                foreach (var collection in _dyconitCollections.Collections ?? Enumerable.Empty<Collection>())
                {
                    collection.Nodes?.RemoveAll(n => n.Port == port);
                }

            }
        }

        public async Task<SyncResult> BoundStaleness(List<ConsumeResult<Null, string>> localData, string collectionName)
        {
            var portsStalenessExceeded = new List<int>();

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
            {;
                return result;
            }

            // retrieve all the other nodes in the collection
            var otherNodes = _dyconitCollections.Collections
                ?.FirstOrDefault(c => c.Name == collectionName)
                ?.Nodes
                ?.Where(n => n.Port != _listenPort)
                ?.ToList();

            Log.Information($"There are {otherNodes?.Count} other nodes in the collection {collectionName}");

            // if there are no other nodes, we can't do anything
            if (otherNodes == null || !otherNodes.Any())
            {
                return result;
            }

            var consumedTime = DateTime.Now;

            // Compare the consume time with the LastTimeSincePull for each node.
            // If the difference is greater than the staleness bound, we need to pull data from the other node
            foreach (var node in otherNodes)
            {
                var timeSincePull = consumedTime - node.LastTimeSincePull;

                Log.Information($"Time since pull for port {node.Port} is {timeSincePull!.Value.TotalMilliseconds} milliseconds, staleness bound is {stalenessBound.Value} milliseconds");

                if (timeSincePull.HasValue && timeSincePull.Value.TotalMilliseconds > stalenessBound.Value)
                {
                    portsStalenessExceeded.Add(node.Port!.Value);
                    // send a pull request to the other node
                    var message = new JObject
                    {
                        ["eventType"] = "syncRequest",
                        ["port"] = _listenPort,
                        ["host"] = _host,
                        ["collectionName"] = collectionName,
                    };

                    await SendMessageOverTcp(message.ToString(), node.Port!.Value, node.Host!);

                    // update the LastTimeSincePull for the node
                    node.LastTimeSincePull = DateTime.Now;
                }
            }

            await WaitForResponseAsync(portsStalenessExceeded);

            var lateResult = UpdateLocalData(localData, collectionName);

            // comapre result with lateResult. Return the result with the highest data count
            if (lateResult.Data!.Count > result.Data!.Count)
            {
                return lateResult;
            }

            // Return the local data
            return result;
        }

        private async Task WaitForResponseAsync(List<int> portsStalenessExceeded)
        {
            int timeout = 5000; // Set timeout to 5 seconds
            var startTime = DateTime.UtcNow;

            while (true)
            {
                if (_syncResponses.Count >= portsStalenessExceeded.Count)
                {
                    _syncResponses.Clear();
                    break;
                }

                // Check if timeout has been exceeded
                if (DateTime.UtcNow - startTime > TimeSpan.FromMilliseconds(timeout))
                {
                    Log.Warning($"Timeout exceeded while waiting for sync responses from ports: {string.Join(",", portsStalenessExceeded)}");
                    break;
                }

                await Task.Delay(100);
            }
        }

        public bool BoundNumericalError(List<ConsumeResult<Null, string>> list, string topic, double totalLocalWeight)
        {
            // get the number of nodes in the collection
            var numberOfNodes = _dyconitCollections.Collections
                ?.FirstOrDefault(c => c.Name == topic)
                ?.Nodes
                ?.Count;

            Log.Information($"There are {numberOfNodes} nodes in the collection {topic}");

            // if the number of nodes is null, we can't do anything
            if (!numberOfNodes.HasValue || numberOfNodes.Value == 0 || numberOfNodes.Value == 1)
            {
                return false || _synced;
            }
            else
            {
                var otherNodes = _dyconitCollections.Collections
                    ?.FirstOrDefault(c => c.Name == topic)
                    ?.Nodes
                    ?.Where(n => n.Port != _listenPort).ToList();

                // go through the numerical error bounds for each node which is not the current node
                foreach (var node in otherNodes!)
                {
                    double numericalErrorBound = node.Bounds!.NumericalError!.Value;

                    // calculate the average weight per node
                    double averageWeightPerNode = totalLocalWeight / (numberOfNodes.Value - 1);

                    Log.Information($"Average weight per node is {averageWeightPerNode}, numerical error bound is {numericalErrorBound}");

                    // if the average weight per node is greater than the numerical error bound, we need to send data to the other node
                    if (averageWeightPerNode > numericalErrorBound)
                    {
                        // send a sync request to the other node
                        var message = new JObject
                        {
                            ["eventType"] = "syncRequest",
                            ["port"] = node.Port,
                            ["host"] = node.Host,
                            ["collectionName"] = topic,
                        };

                        SendMessageOverTcp(message.ToString(), _listenPort, _host).Wait();
                    }
                }

                return false || _synced;
            }
        }

    }
}
