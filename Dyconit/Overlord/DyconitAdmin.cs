using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Dyconit.Message;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace Dyconit.Overlord
{
    public class DyconitAdmin
    {
        private readonly AdminClientConfig _adminClientConfig;
        private readonly IAdminClient _adminClient;
        private readonly int _throughputThreshold;
        private readonly int _type;
        private readonly int _listenPort;
        private readonly Dictionary<string, object> _conit;
        private readonly string _collection;
        private readonly int _staleness;
        private readonly int _orderError;
        private readonly double _numericalError;
        private List<ConsumeResult<Null, string>> _receivedData;
        private List<ConsumeResult<Null, string>>_localData = new List<ConsumeResult<Null, string>>();
        private TaskCompletionSource<bool> _stalenessEventReceived;
        // private readonly Dictionary<string, object> _localCollections;

        private Dictionary<string, Dictionary<string, object>> _localCollections;
        private bool _optimisticMode = true;
        private HashSet<int> _syncResponses = new HashSet<int>();
        private long _previousOffset = 0;
        private bool _isFirstCall = true;
        private double _throughput = 0.0;

        private bool _synced = false;

        public DyconitAdmin(string bootstrapServers, int type, int listenPort, Dictionary<string, Dictionary<string, object>> conitCollection)
        {
            _type = type;
            _listenPort = listenPort;
            _localCollections = conitCollection;

            // Initialize local data for each collection
            foreach (var collection in _localCollections)
            {
                collection.Value.Add("ports", new List<int> { });
                collection.Value.Add("ltsp", new List<Tuple<int, DateTime>> { }); // Last time since pull for each port
            }

            // print local collections
            foreach (var collection in _localCollections)
            {
                Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Local collection: {collection.Key}");
                foreach (var kvp in collection.Value)
                {
                    Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Key = {kvp.Key}, Value = {kvp.Value}");
                }
            }

            // _localCollections = new Dictionary<string, object>
            // {
            //     { "Staleness", _staleness },
            //     { "OrderError", _orderError },
            //     { "NumericalError", _numericalError },
            //     { "ports", new List<int> { } },
            //     { "ltsp", new List<Tuple<int, DateTime>> {}} // Last time since pull for each port
            // };

            _adminClientConfig = new AdminClientConfig
            {
                BootstrapServers = bootstrapServers,
            };

            _adminClient = new AdminClientBuilder(_adminClientConfig).Build();

            ListenForMessagesAsync();
            calculateThroughputAysnc();
        }

        private async Task calculateThroughputAysnc()
        {
            while (true)
            {
                // send it to the dyconit overlord every 5 seconds
                await Task.Delay(5000);

                // calculate Throughput
                await CalculateThroughput();
            }
        }

        private async Task CalculateThroughput()
        {
            // get current offset or 0 if no messages have been received
            long currentOffset = _localData.Count > 0 ? _localData.Last().Offset : 0;

            if (_isFirstCall)
            {
                _isFirstCall = false;
            }
            else
            {

                long offsetDifference = currentOffset - _previousOffset;

                Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Current offset: {currentOffset}, Previous offset: {_previousOffset}, Offset difference: {offsetDifference}");

                // check if the offsetDifference is negative
                if (offsetDifference <= 0)
                {
                    return;
                }

                double throughput = offsetDifference / 5.0;

                Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Throughput: {throughput} messages per second");

                var throughputMessage = new JObject
                {
                    { "eventType", "throughput" },
                    { "throughput", throughput },
                    { "adminPort", _listenPort }
                };

                SendMessageOverTcp(throughputMessage.ToString(), 6666);
            }
            _previousOffset = currentOffset;
        }

        private async void ListenForMessagesAsync()
        {
            var listener = new TcpListener(IPAddress.Any, _listenPort);
            listener.Start();

            while (true)
            {
                var client = await listener.AcceptTcpClientAsync().ConfigureAwait(false);
                var reader = new StreamReader(client.GetStream());
                var message = await reader.ReadToEndAsync().ConfigureAwait(false);

                // Parse message and act accordingly
                ParseMessageAsync(message);

                reader.Close();
                client.Close();
            }
        }

        private async void ParseMessageAsync(string message)
        {
            await Task.Run(() =>
            {
                    JObject json = JObject.Parse(message);
                try
                {

                    // Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Message received: {message}");
                    var eventType = json["eventType"]?.ToString();
                    var collectionName = json["collection"]?.ToString();

                    var collection = default(Dictionary<string, object>); // Replace CollectionType with the actual type of 'collection'

                    if (!string.IsNullOrEmpty(collectionName) && _localCollections.ContainsKey(collectionName))
                    {
                        collection = _localCollections[collectionName];
                    }

                    if (eventType == null)
                    {
                        Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Invalid message received: missing eventType. Message: {message}");
                        return;
                    }

                    switch (eventType)
                    {

                        case "syncResponse":

                            // retrieve the collection from the _localCollections dictionary
                            var data = Convert.ToString(json["data"]);
                            var timestamp = json["timestamp"];
                            var senderPort = Convert.ToInt32(json["port"]);

                            List<ConsumeResultWrapper<Null, string>> deserializedList = JsonConvert.DeserializeObject<List<ConsumeResultWrapper<Null, string>>>(data);

                            // Convert back to original format
                            _receivedData = deserializedList.Select(dw => new ConsumeResult<Null, string>
                            {
                                Topic = dw.Topic,
                                Partition = new Partition(dw.Partition),
                                Offset = new Offset(dw.Offset),
                                Message = new Message<Null, string>
                                {
                                    Key = dw.Message.Key,
                                    Value = dw.Message.Value,
                                    Timestamp = dw.Message.Timestamp,
                                    Headers = new Headers(),
                                },
                                IsPartitionEOF = dw.IsPartitionEOF
                            }).ToList();

                            // Call the method to process the completed staleness event
                            UpdateLocalData(_localData, collectionName);

                            Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Updated local data with syncResponse from port {senderPort}");

                            // update the last time since pull for the sender port
                            var ltsp = collection["ltsp"] as List<Tuple<int, DateTime>>;
                            var index = ltsp.FindIndex(x => x.Item1 == senderPort);
                            ltsp[index] = new Tuple<int, DateTime>(senderPort, DateTime.Now);

                            // Add the senderPort to the set of received sync responses
                            _syncResponses.Add(senderPort);

                            break;

                        case "newNodeEvent":

                            Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Received newNodeEvent");
                            var newNodePort = Convert.ToInt32(json["port"]);


                            // Add the new node to the local collection
                            var ports = collection["ports"] as List<int>;
                            ports.Add(newNodePort);
                            collection["ports"] = ports;

                            // Add the new node to the last time since pull list
                            var ltsp2 = collection["ltsp"] as List<Tuple<int, DateTime>>;
                            ltsp2.Add(new Tuple<int, DateTime>(newNodePort, DateTime.Now));
                            collection["ltsp"] = ltsp2;

                            break;

                        case "removeNodeEvent":

                            Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Received removeNodeEvent");

                            var removeNodePort = Convert.ToInt32(json["adminClientPort"]);

                            // Remove the node from the local collection
                            var ports2 = collection["ports"] as List<int>;
                            ports2.Remove(removeNodePort);
                            collection["ports"] = ports2;

                            break;

                        case "updateConitEvent":
                            Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Received updateConitEvent");
                            var newStaleness = Convert.ToInt32(json["staleness"]);
                            var newOrderError = Convert.ToInt32(json["orderError"]);
                            var newNumericalError = Convert.ToInt32(json["numericalError"]);

                            // Update the local collection
                            collection["Staleness"] = newStaleness;
                            collection["OrderError"] = newOrderError;
                            collection["NumericalError"] = newNumericalError;

                            Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Updated local collection with new staleness values: Staleness: {newStaleness}, OrderError: {newOrderError}, NumericalError: {newNumericalError}");

                            break;

                        case "updateOptimisticModeEvent":
                            Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Received updateOptimisticModeEvent");
                            var newOptimisticMode = Convert.ToBoolean(json["optimisticMode"]);

                            // Update the local collection
                            _optimisticMode = newOptimisticMode;
                            break;

                        case "syncRequest":
                            var syncRequestPort = Convert.ToInt32(json["port"]);
                            Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Received syncRequest from port {syncRequestPort}");

                            List<ConsumeResultWrapper<Null, string>> wrapperList = _localData.Select(cr => new ConsumeResultWrapper<Null, string>(cr)).ToList();
                            string consumeResultWrapped = JsonConvert.SerializeObject(wrapperList, Formatting.Indented);

                            var syncResponse = new JObject
                            {
                                { "eventType", "syncResponse" },
                                { "port", _listenPort },
                                { "data", consumeResultWrapped },
                                { "collection", collectionName}
                            };

                            SendMessageOverTcp(syncResponse.ToString(), syncRequestPort);

                            Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Sent syncResponse to port {syncRequestPort}");
                            _synced = true;
                            break;

                        case "heartbeatEvent":
                            // var heartbeatPort = Convert.ToInt32(json["port"]);

                            // Send a heartbeat response to the requesting node
                            var heartbeatResponse = new JObject
                            {
                                { "eventType", "heartbeatResponse" },
                                { "adminClientPort", _listenPort }
                            };

                            SendMessageOverTcp(heartbeatResponse.ToString(), 6666);
                            break;

                        default:
                            Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Unknown message received with eventType '{eventType}': {message}");
                            break;
                    }
                }
                catch (JsonReaderException ex)
                {
                    Console.WriteLine($"Error parsing JSON: {ex.Message}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error occurred while parsing message: {ex.Message}");
                    if (json != null)
                    {
                        Console.WriteLine("The JSON object that caused the exception:");
                        Console.WriteLine(json.ToString());
                        Console.WriteLine("The following keys were found in the JSON object:");
                        foreach (var key in json.Properties())
                        {
                            Console.WriteLine(key.Name);
                        }
                    }
                }
            });
        }

        private SyncResult UpdateLocalData(List<ConsumeResult<Null, string>> localData, string collectionName)
        {
            // Filter localData to keep only the items with matching topic
            if (_receivedData != null){
                _receivedData = _receivedData.Where(item => item.Topic == collectionName).ToList();
                Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} _receivedData.Count: {_receivedData.Count()}");
            }

            // check if the topic of the _receivedData is the same as the topic of localData
            if(_receivedData == null || _receivedData.Count() < localData.Count() || _receivedData.First().Topic != localData.First().Topic)
            {
                Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} _receivedData is null or old");
                Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} localData.Count: {localData.Count}");
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

            // print the content of localData, _receivedData and mergedData
            foreach (var item in localData)
            {
                Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} localData: {item.Message.Value}");
            }
            Console.WriteLine("--------------------------------------------------");
            foreach (var item in _receivedData)
            {
                Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} _receivedData: {item.Message.Value}");
            }
            Console.WriteLine("--------------------------------------------------");
            foreach (var item in mergedData)
            {
                Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} mergedData: {item.Message.Value}");
            }

            bool isNotSame = mergedData.Count() != localData.Count();// && mergedData.Count() == _receivedData.Count();

            Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} localData.Count: {localData.Count}, _receivedData.Count: {_receivedData.Count}, mergedData.Count: {mergedData.Count}");

            SyncResult result = new SyncResult
            {
                changed = isNotSame || _synced,
                Data = mergedData,
            };

            // if (isNotSame)
            // {
                // Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} _receivedData is not null and is different from local data");
                _localData = mergedData;
            // }
            // else {
            //     Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} _receivedData is not null and is the same as local data");
            //     _localData = localData;
            // }

            _synced = _synced ? false : _synced;
            return result;
        }

        public async Task<SyncResult> BoundStaleness(DateTime consumedTime, List<ConsumeResult<Null, string>> localData, Dictionary<string, object> collection, string collectionName)
        {
            Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} BoundStaleness called for collection {collectionName}");

            // Check if we have received new data from an other node since the last time we checked
            SyncResult result = UpdateLocalData(localData, collectionName);

            Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Updated local data with local data");

            // Retrieve the current staleness bound from the local collection
            var staleness = Convert.ToInt32(collection["Staleness"]);

            // Go through all of the ports in the local collection and check if they have a last time since pull that is older than the staleness bound
            var ports = collection["ports"] as List<int>;
            var ltsp = collection["ltsp"] as List<Tuple<int, DateTime>>;
            var portsStalenessExceeded = new List<int>();

            foreach (var port in ports)
            {
                var lastTimeSincePull = ltsp.FirstOrDefault(x => x.Item1 == port).Item2;
                var timeDifference = consumedTime - lastTimeSincePull;

                if (timeDifference.TotalMilliseconds > staleness)
                {
                    Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Port {port} exceeded the staleness bound {timeDifference.TotalMilliseconds} > {staleness}");
                    portsStalenessExceeded.Add(port);
                }
            }

            // If there are no ports that exceeded, return the local data
            if (!portsStalenessExceeded.Any())
            {
                return result;
            }

            if (_optimisticMode)
            {
                // Call the async mode that will send a sync request to the ports that exceeded the staleness bound
                await WaitForResponseAsync(portsStalenessExceeded, collectionName);

                return result;
            }

            WaitForResponse(portsStalenessExceeded, collectionName);

            // Update the local data
            UpdateLocalData(localData, collectionName);

            // Return the local data
            return result;
        }

        private async Task WaitForResponseAsync(List<int> portsStalenessExceeded, string collectionName)
        {
            // If there are ports that exceeded, send a sync request to those ports
            var message = new Dictionary<string, object>
            {
                { "eventType", "syncRequest" },
                { "port", _listenPort },
                { "collection", collectionName }
            };

            var json = JsonConvert.SerializeObject(message);

            Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Sending sync request to ports {string.Join(", ", portsStalenessExceeded)}");

            foreach (var port in portsStalenessExceeded)
            {
                await SendMessageOverTcp(json, port);
                Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Sent sync request to port {port}");
            }

            // Wait for all sync responses to be received
            while (true)
            {
                if (_syncResponses.Count == portsStalenessExceeded.Count)
                {
                    break;
                }

                await Task.Delay(100);
            }
        }

        private void WaitForResponse(List<int> portsStalenessExceeded, string collectionName)
        {
            // If there are ports that exceeded, send a sync request to those ports
            var message = new Dictionary<string, object>
            {
                { "eventType", "syncRequest" },
                { "port", _listenPort },
                { "collection", collectionName }
            };

            var json = JsonConvert.SerializeObject(message);

            Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Sending sync request to ports {string.Join(", ", portsStalenessExceeded)}");

            foreach (var port in portsStalenessExceeded.ToList())
            {
                SendMessageOverTcp(json, port).Wait();
                Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Sent sync request to port {port}");
            }

            // Wait for all sync responses to be received
            var completionSource = new TaskCompletionSource<object>();

            while (true)
            {
                if (_syncResponses.Count == portsStalenessExceeded.Count)
                {
                    completionSource.SetResult(null);
                    break;
                }

                Thread.Sleep(100);
            }

            completionSource.Task.Wait();
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
                Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Failed to send message over TCP: {ex.Message}");
            }
        }

       public async Task<SyncResult> BoundNumericalError(List<ConsumeResult<Null, string>> localData, Dictionary<string, object> collection, string collectionName)
        {
            Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} BoundNumericalError called for collection {collectionName}");
            List<int> ports = collection["ports"] as List<int>;
            double res = calculateNumericalOrderError(collection);


            if (res > _numericalError)
            {
                Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Numerical error result exceeds the bound. Sending sync request to ports {string.Join(", ", ports)}");
                WaitForResponse(ports, collectionName);
            }

            // Update the local data
            SyncResult result = UpdateLocalData(localData, collectionName);

            // Return the local data
            return result;

        }

        public void BoundOrderError(int orderError)
        {
            // send message to dyconit overlord with orderError
        }

        private double calculateNumericalOrderError(Dictionary<string, object> collection)
        {
            int numberOfNodes = ((List<int>)collection["ports"]).Count() + 1;
            Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Number of nodes is {numberOfNodes}");

            return  (2 * (int)Math.Pow((numberOfNodes - 1), 2) * _throughput) / _numericalError;

        }
    }

    public class SyncResult
    {
        public bool changed { get; set; }
        public List<ConsumeResult<Null, string>> ?Data { get; set; }

    }

    // Custom comparer to compare ConsumeResult based on timestamps
    class ConsumeResultComparer : IEqualityComparer<ConsumeResult<Null, string>>
    {
        public bool Equals(ConsumeResult<Null, string> x, ConsumeResult<Null, string> y)
        {
            return x.Offset == y.Offset && x.Topic == y.Topic;
        }

        public int GetHashCode(ConsumeResult<Null, string> obj)
        {
            return obj.Offset.GetHashCode();
        }
    }

public class ConsumeResultWrapper<TKey, TValue>
{
    public ConsumeResultWrapper()
    {
    }

    public ConsumeResultWrapper(ConsumeResult<TKey, TValue> result)
    {
        Topic = result.Topic;
        Partition = result.Partition.Value;
        Offset = result.Offset.Value;
        Message = new MessageWrapper<TKey, TValue>(result.Message);
        IsPartitionEOF = result.IsPartitionEOF;
    }

    public string Topic { get; set; }
    public int Partition { get; set; }
    public long Offset { get; set; }
    public MessageWrapper<TKey, TValue> Message { get; set; }
    public bool IsPartitionEOF { get; set; }
}

public class MessageWrapper<TKey, TValue>
{
    public MessageWrapper()
    {
    }

    public MessageWrapper(Message<TKey, TValue> message)
    {
        Key = message.Key;
        Value = message.Value;
        Timestamp = message.Timestamp;
        Headers = message.Headers?.Select(header => new HeaderWrapper(header.Key, header.GetValueBytes())).ToList();
    }

    public TKey Key { get; set; }
    public TValue Value { get; set; }
    public Timestamp Timestamp { get; set; }
    public List<HeaderWrapper> Headers { get; set; }
}

public class HeaderWrapper
{
    public HeaderWrapper()
    {
    }

    public HeaderWrapper(string key, byte[] value)
    {
        Key = key;
        Value = value;
    }

    public string Key { get; set; }
    public byte[] Value { get; set; }
}
}
