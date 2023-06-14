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
        private readonly int _numericalError;
        private List<ConsumeResult<Null, string>> _receivedData;
        private List<ConsumeResult<Null, string>>_localData = new List<ConsumeResult<Null, string>>();
        private TaskCompletionSource<bool> _stalenessEventReceived;
        private readonly Dictionary<string, object> _localCollection;
        private bool _optimisticMode = true;
        private HashSet<int> _syncResponses = new HashSet<int>();
        private double _previousAppOffset = 0.0;
        private bool _isFirstCall = true;

        public DyconitAdmin(string bootstrapServers, int type, int listenPort, Dictionary<string, object> conit)
        {
            _type = type;
            _listenPort = listenPort;
            _conit = conit;
            _collection = conit.ContainsKey("collection") ? conit["collection"].ToString() : null;
            _staleness = conit.ContainsKey("Staleness") ? Convert.ToInt32(conit["Staleness"]) : 0;
            _orderError = conit.ContainsKey("OrderError") ? Convert.ToInt32(conit["OrderError"]) : 0;
            _numericalError = conit.ContainsKey("NumericalError") ? Convert.ToInt32(conit["NumericalError"]) : 0;

            // Initialize local collection. The Dyconit overlord will update this collection with the latest data.
            _localCollection = new Dictionary<string, object>
            {
                { "Staleness", _staleness },
                { "OrderError", _orderError },
                { "NumericalError", _numericalError },
                { "ports", new List<int> { } },
                { "ltsp", new List<Tuple<int, DateTime>> {}} // Last time since pull for each port
            };

            _adminClientConfig = new AdminClientConfig
            {
                BootstrapServers = bootstrapServers,
            };

            _adminClient = new AdminClientBuilder(_adminClientConfig).Build();

            ListenForMessagesAsync();
        }

        public void ProcessConsumerStatistics(string json, ClientConfig config)
        {
            try
            {
                var statistics = JObject.Parse(json);


                // Save the app_offset for the first time.
                // Compare the app_offset with the previous app_offset.
                // Calculate the throughput.


                var inputTopic = statistics?["topics"]?["input_topic"];
                // Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Received statistics: {statistics}");
                var partition = inputTopic?["partitions"]?["0"];
                var appOffset = partition?["app_offset"];
                // Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} inputTopic: {inputTopic}, partition: {partition}, appOffset: {appOffset}");

                Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Received appOffset: {appOffset}");

                if (appOffset != null && double.TryParse(appOffset.ToString(), out double currentOffsetValue))
                {

                    if (_isFirstCall)
                    {
                        _isFirstCall = false;
                    } else {
                        // Calculate throughput
                        var offsetDifference = currentOffsetValue - _previousAppOffset;

                        Console.WriteLine($"currentOffsetValue: {currentOffsetValue}, _previousAppOffset: {_previousAppOffset}, offsetDifference: {offsetDifference}");

                        var throughput = offsetDifference / config.StatisticsIntervalMs * 1000;

                        var throughputMessage = new JObject
                        {
                            { "eventType", "throughput" },
                            { "throughput", throughput },
                            { "port", _listenPort }
                        };

                    }


                    // Update the previous app_offset
                    _previousAppOffset = currentOffsetValue;
                }
                else
                {
                    Console.WriteLine("Unable to retrieve app_offset or it has an invalid value.");
                }
            }
            catch (JsonReaderException ex)
            {
                Console.WriteLine($"Error parsing JSON: {ex.Message}");
            }
        }

        private async void ListenForMessagesAsync()
        {
            var listener = new TcpListener(IPAddress.Any, _listenPort);
            listener.Start();

            // var client = await listener.AcceptTcpClientAsync();
            // var reader = new StreamReader(client.GetStream());

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
                try
                {
                    JObject json = JObject.Parse(message);

                    // Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Received message: {message}");

                    var eventType = json["eventType"]?.ToString();
                    if (eventType == null)
                    {
                        Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Invalid message received: missing eventType. Message: {message}");
                        return;
                    }

                    switch (eventType)
                    {
                        case "syncResponse":
                            // JArray dataArray = (JArray)json["data"];
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

                            // // Verify deserialized data
                            // Console.WriteLine("Deserialized Data:");

                            // // Accessing deserialized data
                            // foreach (var resultWrapper in deserializedList)
                            // {
                            //     Console.WriteLine($"Topic: {resultWrapper.Topic}");
                            //     Console.WriteLine($"Partition: {resultWrapper.Partition}");
                            //     Console.WriteLine($"Offset: {resultWrapper.Offset}");
                            //     Console.WriteLine($"Key: {resultWrapper.Message.Key}");
                            //     Console.WriteLine($"Value: {resultWrapper.Message.Value}");
                            //     Console.WriteLine($"Timestamp: {resultWrapper.Message.Timestamp}");
                            //     Console.WriteLine($"IsPartitionEOF: {resultWrapper.IsPartitionEOF}");
                            //     Console.WriteLine("Headers:");
                            //     foreach (var header in resultWrapper.Message.Headers)
                            //     {
                            //         Console.WriteLine($"  Key: {header.Key}");
                            //         Console.WriteLine($"  Value: {BitConverter.ToDouble(header.Value)}");
                            //     }
                            //     Console.WriteLine();
                            // }

                            // Call the method to process the completed staleness event
                            UpdateLocalData(_localData);

                            Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Updated local data with syncResponse from port {senderPort}");

                            // update the last time since pull for the sender port
                            var ltsp = _localCollection["ltsp"] as List<Tuple<int, DateTime>>;
                            var index = ltsp.FindIndex(x => x.Item1 == senderPort);
                            ltsp[index] = new Tuple<int, DateTime>(senderPort, DateTime.Now);

                            // Add the senderPort to the set of received sync responses
                            _syncResponses.Add(senderPort);

                            break;

                        case "newNodeEvent":

                            Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Received newNodeEvent");

                            var newNodePort = Convert.ToInt32(json["port"]);

                            // Add the new node to the local collection
                            var ports = _localCollection["ports"] as List<int>;
                            ports.Add(newNodePort);
                            _localCollection["ports"] = ports;

                            // Add the new node to the last time since pull list
                            var ltsp2 = _localCollection["ltsp"] as List<Tuple<int, DateTime>>;
                            ltsp2.Add(new Tuple<int, DateTime>(newNodePort, DateTime.Now));
                            _localCollection["ltsp"] = ltsp2;

                            break;

                        case "removeNodeEvent":

                            Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Received removeNodeEvent");

                            var removeNodePort = Convert.ToInt32(json["adminClientPort"]);

                            // Remove the node from the local collection
                            var ports2 = _localCollection["ports"] as List<int>;
                            ports2.Remove(removeNodePort);
                            _localCollection["ports"] = ports2;

                            break;

                        case "updateConitEvent":
                            Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Received updateConitEvent");


                            var newStaleness = Convert.ToInt32(json["Staleness"]);
                            var newOrderError = Convert.ToInt32(json["OrderError"]);
                            var newNumericalError = Convert.ToInt32(json["NumericalError"]);

                            // Update the local collection
                            _localCollection["Staleness"] = newStaleness;
                            _localCollection["OrderError"] = newOrderError;
                            _localCollection["NumericalError"] = newNumericalError;

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
                                { "data", consumeResultWrapped }
                            };

                            SendMessageOverTcp(syncResponse.ToString(), syncRequestPort);

                            Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Sent syncResponse to port {syncRequestPort}");

                            break;

                        case "heartbeatEvent":
                            var heartbeatPort = Convert.ToInt32(json["port"]);
                            // Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Received heartbeat from port {heartbeatPort}");

                            // Send a heartbeat response to the requesting node
                            var heartbeatResponse = new JObject
                            {
                                { "eventType", "heartbeatResponse" },
                                { "adminClientPort", _listenPort },
                                { "collection", _collection }
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
                }
            });
        }

        private SyncResult UpdateLocalData(List<ConsumeResult<Null, string>> localData)
        {

            // check if _receivedData is null
            if(_receivedData == null)
            {
                Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} _receivedData is null");
                Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} localData.Count: {localData.Count}");
                _localData = localData;
                return new SyncResult
                {
                    changed = false,
                    Data = localData,
                };
            }

            // Merge the received data with the local data
            var mergedData = localData.Union(_receivedData, new ConsumeResultComparer()).ToList();
            bool isSame = mergedData.Count() != localData.Count && mergedData.Count() == _receivedData.Count;

            Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} localData.Count: {localData.Count}, _receivedData.Count: {_receivedData.Count}, mergedData.Count: {mergedData.Count}");

            SyncResult result = new SyncResult
            {
                changed = isSame,
                Data = mergedData,
            };

            if (!isSame)
            {
                Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} _receivedData is not null and is different from local data");
                _localData = mergedData;
            }
            else {
                Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} _receivedData is not null and is the same as local data");
                _localData = localData;
            }

            return result;
        }

        public async Task<SyncResult> BoundStaleness(DateTime consumedTime, List<ConsumeResult<Null, string>> localData)
        {

            // Check if we have received new data from an other node since the last time we checked
            SyncResult result = UpdateLocalData(localData);

            Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Updated local data with local data");

            // Retrieve the current staleness bound from the local collection
            var staleness = Convert.ToInt32(_localCollection["Staleness"]);

            // Go through all of the ports in the local collection and check if they have a last time since pull that is older than the staleness bound
            var ports = _localCollection["ports"] as List<int>;
            var ltsp = _localCollection["ltsp"] as List<Tuple<int, DateTime>>;
            var portsStalenessExceeded = new List<int>();

            foreach (var port in ports)
            {
                var lastTimeSincePull = ltsp.FirstOrDefault(x => x.Item1 == port).Item2;
                var timeDifference = consumedTime - lastTimeSincePull;

                if (timeDifference.TotalMilliseconds > staleness)
                {
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
                await WaitForResponseAsync(portsStalenessExceeded);

                return result;
            }

            WaitForResponse(portsStalenessExceeded);

            // Update the local data
            UpdateLocalData(localData);

            // Return the local data
            return result;
        }

        private async Task WaitForResponseAsync(List<int> portsStalenessExceeded)
        {
            // If there are ports that exceeded, send a sync request to those ports
            var message = new Dictionary<string, object>
            {
                { "eventType", "syncRequest" },
                { "port", _listenPort }
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

        private void WaitForResponse(List<int> portsStalenessExceeded)
        {
            // If there are ports that exceeded, send a sync request to those ports
            var message = new Dictionary<string, object>
            {
                { "eventType", "syncRequest" },
                { "port", _listenPort }
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

    //    public async Task<List<string>> BoundNumericalError(List<string> localData)
    //     {

    //         // generate a random throughput between 100 and 1000
    //         Random rnd = new Random();
    //         int throughput = rnd.Next(100, 1000);

    //         List<int> ports = _localCollection["ports"] as List<int>;
    //         int res = calculateNumericalOrderError(throughput, _numericalError);


    //         if (res > _numericalError)
    //         {
    //             Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Numerical error result exceeds the bound. Sending sync request to ports {string.Join(", ", ports)}");
    //             WaitForResponse(ports);
    //         }

    //         // Update the local data
    //         UpdateLocalData(localData);

    //         // Return the local data
    //         return _localData;

    //     }

        public void BoundOrderError(int orderError)
        {
            // send message to dyconit overlord with orderError
        }

        private int calculateNumericalOrderError(int throughput, int numericalError)
        {
            int numberOfNodes = ((List<int>)_localCollection["ports"]).Count() + 1;
            Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Number of nodes is {numberOfNodes}");

            return  (2 * (int)Math.Pow((numberOfNodes - 1), 2) * throughput) / numericalError;

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
            return x.Offset == y.Offset;
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
