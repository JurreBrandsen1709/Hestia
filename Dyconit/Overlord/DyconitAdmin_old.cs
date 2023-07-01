// using Confluent.Kafka;
// using System.Net;
// using System.Net.Sockets;
// using Newtonsoft.Json.Linq;
// using Newtonsoft.Json;
// using System.Text;

// namespace Dyconit.Overlord
// {
//     public class DyconitAdmin2
//     {
//         private readonly AdminClientConfig _adminClientConfig;
//         public readonly IAdminClient _adminClient;
//         private readonly int _throughputThreshold;
//         private readonly int _type;
//         private readonly int _listenPort;
//         private readonly Dictionary<string, object> _conit;
//         private readonly string _collection;
//         private readonly int _staleness;
//         private readonly int _orderError;
//         private readonly double _numericalError;
//         private List<ConsumeResult<Null, string>> _receivedData;
//         private List<ConsumeResult<Null, string>>_localData = new List<ConsumeResult<Null, string>>();
//         private TaskCompletionSource<bool> _stalenessEventReceived;
//         // private Dictionary<string, Dictionary<string, object>> _localCollections;
//         private static JObject _localCollections = new JObject();
//         private bool _optimisticMode = true;
//         private HashSet<int> _syncResponses = new HashSet<int>();
//         private long _previousOffset = 0;
//         private bool _isFirstCall = true;
//         private double _throughput = 0.0;
//         private bool _synced = false;
//         private Dictionary<string, int> _syncCounters = new Dictionary<string, int>();
//         public IConsumer<Null, string> _consumer;

//         public DyconitAdmin2(string bootstrapServers, int type, int listenPort, JObject conitCollection)
//         {
//             _type = type;
//             _listenPort = listenPort;
//             _localCollections = conitCollection;

//             foreach (var collection in _localCollections)
//             {
//                 var collectionValue = (JObject)collection.Value;
//                 collectionValue.Add("ports", new JArray());
//                 collectionValue.Add("ltsp", new JArray());
//             }

//             _adminClientConfig = new AdminClientConfig
//             {
//                 BootstrapServers = bootstrapServers,
//             };

//             _adminClient = new AdminClientBuilder(_adminClientConfig).Build();

//             ListenForMessagesAsync();
//         }

//         private async void ListenForMessagesAsync()
//         {
//             _ = SendSyncCountAsync();
//             var listener = new TcpListener(IPAddress.Any, _listenPort);
//             listener.Start();

//             while (true)
//             {
//                 var client = await listener.AcceptTcpClientAsync().ConfigureAwait(false);
//                 var reader = new StreamReader(client.GetStream());
//                 var message = await reader.ReadToEndAsync().ConfigureAwait(false);

//                 // Parse message and act accordingly
//                 await ParseMessageAsync(message);

//                 reader.Close();
//                 client.Close();

//             }
//         }

//         private async Task SendSyncCountAsync()
//         {
//             while (true)
//             {
//                 await Task.Delay(10000); // Delay for 10 seconds

//                 // print the sync counters
//                 foreach (var counter in _syncCounters)
//                 {
//                     Console.WriteLine($"*************************** Sync counter for {counter.Key}: {counter.Value}");
//                 }

//                 // check if there are any sync counters that are greater than 0
//                 if (_syncCounters.Any(c => c.Value < 0))
//                 {
//                     Console.WriteLine($"*************************** Sync counter is less than 0");
//                     continue;
//                 }

//                 try
//                 {
//                     using (var client = new TcpClient())
//                     {
//                         await client.ConnectAsync("localhost", 6666); // Connect to port 6666

//                         using (var stream = client.GetStream())
//                         using (var writer = new StreamWriter(stream, Encoding.ASCII, leaveOpen: true))
//                         {
//                             var data = JsonConvert.SerializeObject(_syncCounters);
//                             var overheadMessage = new JObject
//                             {
//                                 { "eventType", "overheadMessage" },
//                                 { "adminPort", _listenPort },
//                                 { "data", data }
//                             };

//                             await writer.WriteLineAsync(overheadMessage.ToString()); // Send the _syncCount as a string
//                             await writer.FlushAsync();
//                         }
//                     }
//                     _syncCounters = new Dictionary<string, int>();
//                 }
//                 catch (Exception ex)
//                 {
//                     Console.WriteLine($"Error sending sync count: {ex.Message}");
//                 }
//             }
//         }


//         private async Task ParseMessageAsync(string message)
//         {
//             await Task.Run(() =>
//             {
//                     JObject json = JObject.Parse(message);
//                 try
//                 {

//                     // Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Message received: {message}");
//                     var eventType = json["eventType"]?.ToString();
//                     var collectionName = json["collection"]?.ToString();

//                     // var collection = default(Dictionary<string, object>); // Replace CollectionType with the actual type of 'collection'

//                     // if (!string.IsNullOrEmpty(collectionName) && _localCollections.ContainsKey(collectionName))
//                     // {
//                     //     collection = _localCollections[collectionName];
//                     // }

//                     // retrieve the collection from the _localCollections dictionary
//                     var collection = _localCollections[collectionName] as JObject;


//                     if (eventType == null)
//                     {
//                         Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Invalid message received: missing eventType. Message: {message}");
//                         return;
//                     }

//                     switch (eventType)
//                     {

//                         case "syncResponse":

//                             // retrieve the collection from the _localCollections dictionary
//                             var data = Convert.ToString(json["data"]);
//                             var timestamp = json["timestamp"];
//                             var senderPort = Convert.ToInt32(json["port"]);

//                             List<ConsumeResultWrapper<Null, string>> deserializedList = JsonConvert.DeserializeObject<List<ConsumeResultWrapper<Null, string>>>(data);

//                             // Convert back to original format
//                             _receivedData = deserializedList.Select(dw => new ConsumeResult<Null, string>
//                             {
//                                 Topic = dw.Topic,
//                                 Partition = new Partition(dw.Partition),
//                                 Offset = new Offset(dw.Offset),
//                                 Message = new Message<Null, string>
//                                 {
//                                     Key = dw.Message.Key,
//                                     Value = dw.Message.Value,
//                                     Timestamp = dw.Message.Timestamp,
//                                     Headers = new Headers(),
//                                 },
//                                 IsPartitionEOF = dw.IsPartitionEOF
//                             }).ToList();

//                             // Call the method to process the completed staleness event
//                             UpdateLocalData(_localData, collectionName);

//                             Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Updated local data with syncResponse from port {senderPort}");

//                             // update the last time since pull for the sender port
//                             var ltsp = collection["ltsp"] as JArray;
//                             var ltspItem = ltsp.FirstOrDefault(i => i["port"].ToString() == senderPort.ToString());
//                             if (ltspItem != null)
//                             {
//                                 ltspItem["timestamp"] = DateTime.Now;
//                             }
//                             else
//                             {
//                                 ltsp.Add(new JObject
//                                 {
//                                     { "port", senderPort },
//                                     { "timestamp", DateTime.Now }
//                                 });
//                             }

//                             // Add the senderPort to the set of received sync responses
//                             _syncResponses.Add(senderPort);

//                             // retrieve the current value of synccount for this collection
//                             if (!_syncCounters.ContainsKey(collectionName))
//                             {
//                                 _syncCounters[collectionName] = 0;
//                             }

//                             var tmp = _syncCounters[collectionName];
//                             _syncCounters[collectionName] = tmp + 1;

//                             break;

//                         case "newNodeEvent":

//                             // Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Received newNodeEvent");
//                             // var newNodePort = Convert.ToInt32(json["port"]);


//                             // // Add the new node to the local collection
//                             // var ports = collection["ports"] as List<int>;
//                             // ports.Add(newNodePort);
//                             // collection["ports"] = ports;

//                             // // Add the new node to the last time since pull list
//                             // var ltsp2 = collection["ltsp"] as List<Tuple<int, DateTime>>;
//                             // ltsp2.Add(new Tuple<int, DateTime>(newNodePort, DateTime.Now));
//                             // collection["ltsp"] = ltsp2;

//                             Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Received newNodeEvent");
//                             var newNodePort = Convert.ToInt32(json["port"]);

//                             var ports = collection["ports"] as JArray;
//                             ports.Add(newNodePort);

//                             var ltsp2 = collection["ltsp"] as JArray;
//                             ltsp2.Add(new JArray { newNodePort, DateTime.Now });

//                             break;

//                         case "removeNodeEvent":

//                             Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Received removeNodeEvent");

//                             var removeNodePort = Convert.ToInt32(json["adminClientPort"]);

//                             var ports2 = collection["ports"] as JArray;
//                             ports2.Remove(removeNodePort);

//                             break;

//                         case "updateConitEvent":
//                             Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Received updateConitEvent");
//                             var newStaleness = Convert.ToInt32(json["staleness"]);
//                             var newOrderError = Convert.ToInt32(json["orderError"]);
//                             var newNumericalError = Convert.ToInt32(json["numericalError"]);

//                             // Update the local collection
//                             collection["Staleness"] = newStaleness;
//                             collection["OrderError"] = newOrderError;
//                             collection["NumericalError"] = newNumericalError;

//                             Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Updated local collection with new staleness values: Staleness: {newStaleness}, OrderError: {newOrderError}, NumericalError: {newNumericalError}");

//                             break;

//                         case "updateOptimisticModeEvent":
//                             Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Received updateOptimisticModeEvent");
//                             var newOptimisticMode = Convert.ToBoolean(json["optimisticMode"]);

//                             // Update the local collection
//                             _optimisticMode = newOptimisticMode;
//                             break;

//                         case "syncRequest":
//                             var syncRequestPort = Convert.ToInt32(json["port"]);
//                             Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Received syncRequest from port {syncRequestPort}");

//                             List<ConsumeResultWrapper<Null, string>> wrapperList = _localData.Select(cr => new ConsumeResultWrapper<Null, string>(cr)).ToList();
//                             string consumeResultWrapped = JsonConvert.SerializeObject(wrapperList, Formatting.Indented);

//                             var syncResponse = new JObject
//                             {
//                                 { "eventType", "syncResponse" },
//                                 { "port", _listenPort },
//                                 { "data", consumeResultWrapped },
//                                 { "collection", collectionName}
//                             };

//                             SendMessageOverTcp(syncResponse.ToString(), syncRequestPort);

//                             Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Sent syncResponse to port {syncRequestPort}");
//                             _synced = true;

//                             if (!_syncCounters.ContainsKey(collectionName))
//                             {
//                                 _syncCounters[collectionName] = 0;
//                             }

//                             var tmp2 = _syncCounters[collectionName];
//                             _syncCounters[collectionName] = tmp2 + 1;



//                             break;

//                         case "heartbeatEvent":
//                             // var heartbeatPort = Convert.ToInt32(json["port"]);

//                             // Send a heartbeat response to the requesting node
//                             var heartbeatResponse = new JObject
//                             {
//                                 { "eventType", "heartbeatResponse" },
//                                 { "adminClientPort", _listenPort }
//                             };

//                             SendMessageOverTcp(heartbeatResponse.ToString(), 6666);
//                             break;

//                         default:
//                             Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Unknown message received with eventType '{eventType}': {message}");
//                             break;
//                     }
//                 }
//                 catch (JsonReaderException ex)
//                 {
//                     Console.WriteLine($"Error parsing JSON: {ex.Message}");
//                 }
//                 catch (Exception ex)
//                 {
//                     Console.WriteLine($"Error occurred while parsing message: {ex.Message}");
//                     if (json != null)
//                     {
//                         Console.WriteLine("The JSON object that caused the exception:");
//                         Console.WriteLine(json.ToString());
//                         Console.WriteLine("The following keys were found in the JSON object:");
//                         foreach (var key in json.Properties())
//                         {
//                             Console.WriteLine(key.Name);
//                         }
//                     }
//                 }
//             });
//         }

//         private SyncResult UpdateLocalData(List<ConsumeResult<Null, string>> localData, string collectionName)
//         {
//             // Filter localData to keep only the items with matching topic
//             if (_receivedData != null){
//                 _receivedData = _receivedData.Where(item => item.Topic == collectionName).ToList();
//                 Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} _receivedData.Count: {_receivedData.Count()}");
//             }

//             // check if the topic of the _receivedData is the same as the topic of localData
//             if(_receivedData == null || _receivedData.Count() < localData.Count() || _receivedData.First().Topic != localData.First().Topic)
//             {
//                 Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} _receivedData is null or old");
//                 Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} localData.Count: {localData.Count}");
//                 _localData = localData;

//                 SyncResult earlyResult = new SyncResult
//                 {
//                     changed = false || _synced,
//                     Data = localData,
//                 };
//                 _synced = _synced ? false : _synced;
//                 return earlyResult;
//             }

//             // Merge the received data with the local data
//             var mergedData = _receivedData.Union(localData, new ConsumeResultComparer()).ToList();

//             // print the content of localData, _receivedData and mergedData
//             foreach (var item in localData)
//             {
//                 Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} localData: {item.Message.Value}");
//             }
//             Console.WriteLine("--------------------------------------------------");
//             foreach (var item in _receivedData)
//             {
//                 Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} _receivedData: {item.Message.Value}");
//             }
//             Console.WriteLine("--------------------------------------------------");
//             foreach (var item in mergedData)
//             {
//                 Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} mergedData: {item.Message.Value}");
//             }

//             bool isNotSame = mergedData.Count() != localData.Count();// && mergedData.Count() == _receivedData.Count();

//             Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} localData.Count: {localData.Count}, _receivedData.Count: {_receivedData.Count}, mergedData.Count: {mergedData.Count}");

//             SyncResult result = new SyncResult
//             {
//                 changed = isNotSame || _synced,
//                 Data = mergedData,
//             };

//             _localData = mergedData;
//             _synced = _synced ? false : _synced;
//             return result;
//         }

//         // Houdt rekening mee met de network delay.
//         public async Task<SyncResult> BoundStaleness(DateTime consumedTime, List<ConsumeResult<Null, string>> localData, Dictionary<string, object> collection, string collectionName)
//         {
//             Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} BoundStaleness called for collection {collectionName}");

//             // Check if we have received new data from an other node since the last time we checked
//             SyncResult result = UpdateLocalData(localData, collectionName);

//             Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Updated local data with local data");

//             // Retrieve the current staleness bound from the local collection
//             var staleness = Convert.ToInt32(collection["Staleness"]);

//             // Go through all of the ports in the local collection and check if they have a last time since pull that is older than the staleness bound
//             var ports = collection["ports"] as List<int>;
//             var ltsp = collection["ltsp"] as List<Tuple<int, DateTime>>;
//             var portsStalenessExceeded = new List<int>();

//             Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Checking staleness for ports: {string.Join(", ", ports)}");

//             foreach (var port in ports)
//             {
//                 var lastTimeSincePull = ltsp.FirstOrDefault(x => x.Item1 == port).Item2;
//                 var timeDifference = consumedTime - lastTimeSincePull;

//                 Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Consumed time: {consumedTime.ToString("HH:mm:ss.fff")}, last time since pull: {lastTimeSincePull.ToString("HH:mm:ss.fff")}, time difference: {timeDifference.TotalMilliseconds}");

//                 if (timeDifference.TotalMilliseconds > staleness)
//                 {
//                     Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Port {port} exceeded the staleness bound {timeDifference.TotalMilliseconds} > {staleness}");
//                     portsStalenessExceeded.Add(port);
//                 }
//             }

//             // If there are no ports that exceeded, return the local data
//             if (!portsStalenessExceeded.Any())
//             {
//                 return result;
//             }

//             if (_optimisticMode)
//             {
//                 // Call the async mode that will send a sync request to the ports that exceeded the staleness bound
//                 await WaitForResponseAsync(portsStalenessExceeded, collectionName);

//                 return result;
//             }

//             WaitForResponse(portsStalenessExceeded, collectionName);

//             // Update the local data
//             UpdateLocalData(localData, collectionName);

//             // Return the local data
//             return result;
//         }

//         private async Task WaitForResponseAsync(List<int> portsStalenessExceeded, string collectionName)
//         {
//             // If there are ports that exceeded, send a sync request to those ports
//             var message = new Dictionary<string, object>
//             {
//                 { "eventType", "syncRequest" },
//                 { "port", _listenPort },
//                 { "collection", collectionName }
//             };

//             var json = JsonConvert.SerializeObject(message);

//             Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Sending sync request to ports {string.Join(", ", portsStalenessExceeded)}");

//             foreach (var port in portsStalenessExceeded)
//             {
//                 await SendMessageOverTcp(json, port);
//                 Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Sent sync request to port {port}");
//             }

//             // Wait for all sync responses to be received
//             while (true)
//             {
//                 if (_syncResponses.Count == portsStalenessExceeded.Count)
//                 {
//                     break;
//                 }

//                 await Task.Delay(100);
//             }
//         }

//         private void WaitForResponse(List<int> portsStalenessExceeded, string collectionName)
//         {
//             // If there are ports that exceeded, send a sync request to those ports
//             var message = new Dictionary<string, object>
//             {
//                 { "eventType", "syncRequest" },
//                 { "port", _listenPort },
//                 { "collection", collectionName }
//             };

//             var json = JsonConvert.SerializeObject(message);

//             Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Sending sync request to ports {string.Join(", ", portsStalenessExceeded)}");

//             foreach (var port in portsStalenessExceeded.ToList())
//             {
//                 SendMessageOverTcp(json, port).Wait();
//                 Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Sent sync request to port {port}");
//             }

//             // Wait for all sync responses to be received
//             var completionSource = new TaskCompletionSource<object>();

//             while (true)
//             {
//                 if (_syncResponses.Count == portsStalenessExceeded.Count)
//                 {
//                     completionSource.SetResult(null);
//                     _syncResponses.Clear();
//                     break;
//                 }

//                 Thread.Sleep(100);
//             }

//             completionSource.Task.Wait();
//         }


//         private async Task SendMessageOverTcp(string message, int port)
//         {
//             try
//             {
//                 using (var client = new TcpClient())
//                 {
//                     client.Connect("localhost", port);

//                     using (var stream = client.GetStream())
//                     using (var writer = new StreamWriter(stream))
//                     {
//                         await writer.WriteLineAsync(message);
//                         await writer.FlushAsync();
//                     }
//                 }
//             }
//             catch (Exception ex)
//             {
//                 Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Failed to send message over TCP: {ex.Message}");
//             }
//         }

//         public async Task<SyncResult> BoundNumericalError(List<ConsumeResult<Null, string>> localData, Dictionary<string, object> collection, string collectionName, double localWeight)
//         {


//             Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} BoundNumericalError called for collection {collectionName}");

//             // Retrieve the current NumericalError bound from the local collection
//             var numericalError = collection["NumericalError"] as double?;
//             List<int> ports = collection["ports"] as List<int>;
//             double res = calculateNumericalOrderError(ports.Count);

//             Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} result: {res}, numerical error bound: {_numericalError}, ports: {string.Join(", ", ports)}");

//             return null;

//             // if (res > _numericalError)
//             // {
//             //     Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Numerical error result exceeds the bound. Sending sync request to ports {string.Join(", ", ports)}");
//             //     WaitForResponse(ports, collectionName);
//             // }

//             // // Update the local data
//             // SyncResult result = UpdateLocalData(localData, collectionName);

//             // // Return the local data
//             // return result;

//         }

//         private void printCollection(Dictionary<string, object> collection, string collectionName)
//         {
//             Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Collection {collectionName}:");

//             foreach (var item in collection)
//             {
//                 Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} {item.Key}: {item.Value}");
//             }
//         }

//         private double calculateNumericalOrderError(int numberOfNodes)
//         {
//             numberOfNodes += 1; // Add the current node
//             Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Number of nodes is {numberOfNodes}");

//             return  (2 * (double)Math.Pow((numberOfNodes - 1), 2) * _throughput) / _numericalError;

//         }
//     }

//     public class SyncResult
//     {
//         public bool changed { get; set; }
//         public List<ConsumeResult<Null, string>> ?Data { get; set; }

//     }

//     // Custom comparer to compare ConsumeResult based on timestamps
//     class ConsumeResultComparer : IEqualityComparer<ConsumeResult<Null, string>>
//     {
//         public bool Equals(ConsumeResult<Null, string> x, ConsumeResult<Null, string> y)
//         {
//             return x.Offset == y.Offset && x.Topic == y.Topic;
//         }

//         public int GetHashCode(ConsumeResult<Null, string> obj)
//         {
//             return obj.Offset.GetHashCode();
//         }
//     }
// }

// // public class ConsumeResultWrapper<TKey, TValue>
// // {
// //     public ConsumeResultWrapper()
// //     {
// //     }

// //     public ConsumeResultWrapper(ConsumeResult<TKey, TValue> result)
// //     {
// //         Topic = result.Topic;
// //         Partition = result.Partition.Value;
// //         Offset = result.Offset.Value;
// //         Message = new MessageWrapper<TKey, TValue>(result.Message);
// //         IsPartitionEOF = result.IsPartitionEOF;
// //     }

// //     public string Topic { get; set; }
// //     public int Partition { get; set; }
// //     public long Offset { get; set; }
// //     public MessageWrapper<TKey, TValue> Message { get; set; }
// //     public bool IsPartitionEOF { get; set; }
// // }

// // public class MessageWrapper<TKey, TValue>
// // {
// //     public MessageWrapper()
// //     {
// //     }

// //     public MessageWrapper(Message<TKey, TValue> message)
// //     {
// //         Key = message.Key;
// //         Value = message.Value;
// //         Timestamp = message.Timestamp;
// //         Headers = message.Headers?.Select(header => new HeaderWrapper(header.Key, header.GetValueBytes())).ToList();
// //     }

// //     public TKey Key { get; set; }
// //     public TValue Value { get; set; }
// //     public Timestamp Timestamp { get; set; }
// //     public List<HeaderWrapper> Headers { get; set; }
// // }

// // public class HeaderWrapper
// // {
// //     public HeaderWrapper()
// //     {
// //     }

// //     public HeaderWrapper(string key, byte[] value)
// //     {
// //         Key = key;
// //         Value = value;
// //     }

// //     public string Key { get; set; }
// //     public byte[] Value { get; set; }
// // }
// // }
