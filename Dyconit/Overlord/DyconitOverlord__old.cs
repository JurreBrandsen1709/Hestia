// using Confluent.Kafka;
// using Confluent.Kafka.Admin;
// using Newtonsoft.Json;
// using Newtonsoft.Json.Linq;
// using System;
// using System.Net;
// using System.Net.Sockets;
// using System.Threading.Tasks;

// namespace Dyconit.Overlord
// {
//     public class DyconitOverlord
//     {
//         private readonly int _listenPort = 6666;

//         private Dictionary<string, Dictionary<string, object>> _dyconitCollections;

//         public DyconitOverlord()
//         {
//             _dyconitCollections = new Dictionary<string, Dictionary<string, object>>();
//             Console.WriteLine("- Dyconit overlord started.");
//             parsePolicies();
//             ListenForMessagesAsync();
//             SendHeartbeatAsync();
//             KeepTrackOfNodesAsync();
//         }

//         private void parsePolicies()
//         {
//             string policiesFolderPath = "..\\Policies";

//             if (Directory.Exists(policiesFolderPath))
//             {
//                 Console.WriteLine("Policies folder found.");

//                 string[] policyFiles = Directory.GetFiles(policiesFolderPath, "*policy*.json");

//                 Console.WriteLine($"Found {policyFiles.Length} policy files.");

//                 foreach (string policyFile in policyFiles)
//                 {
//                     string jsonContent = File.ReadAllText(policyFile);
//                     JObject policyJson = JObject.Parse(jsonContent);


//                     JArray collections = policyJson["collections"] as JArray;
//                     JToken thresholds = policyJson["thresholds"];
//                     JArray rules = policyJson["rules"] as JArray;

//                     // add some checks to see if collections, thresholds and rules are not null
//                     if (collections == null || thresholds == null || rules == null)
//                     {
//                         Console.WriteLine("Policy file is not valid.");
//                         continue;
//                     }

//                     foreach (string collection in collections)
//                     {
//                         if (_dyconitCollections.ContainsKey(collection))
//                         {
//                             Dictionary<string, object> dyconitCollectionData = _dyconitCollections[collection];
//                             dyconitCollectionData["thresholds"] = thresholds;
//                             dyconitCollectionData["rules"] = rules;
//                         }
//                         else
//                         {
//                             _dyconitCollections.Add(collection, new Dictionary<string, object>
//                             {
//                                 { "thresholds", thresholds },
//                                 { "rules", rules }
//                             });
//                         }
//                     }
//                 }
//             }
//             else
//             {
//                 Console.WriteLine("Policies folder does not exist.");
//             }
//         }
//         private async void SendHeartbeatAsync()
//         {
//             // Send a heartbeat response to the requesting node
//             var heartbeatEvent = new JObject
//             {
//                 { "eventType", "heartbeatEvent" },
//                 { "port", _listenPort}
//             };


//             while (true)
//             {
//                 await Task.Delay(1000).ConfigureAwait(false);

//                 // Send heartbeat to all admin clients
//                 foreach (var dyconitCollection in _dyconitCollections.ToList())
//                 {
//                     var dyconitCollectionData = dyconitCollection.Value;
//                     if (dyconitCollectionData.ContainsKey("ports")) {
//                         var adminClientPorts = (List<Tuple<int, DateTime>>)dyconitCollectionData["ports"];

//                         foreach (var adminClientPort in adminClientPorts.ToList())
//                         {
//                             // send heartbeat to this node
//                             SendMessageOverTcp(heartbeatEvent.ToString(), adminClientPort.Item1);
//                         }
//                     }
//                 }
//             }
//         }

//         // Every 5 seconds, check if we have received a heartbeat from all nodes
//         private async void KeepTrackOfNodesAsync()
//         {
//             while (true)
//             {
//                 await Task.Delay(5000).ConfigureAwait(false);

//                 // Check if we have received a heartbeat from all nodes
//                 foreach (var dyconitCollection in _dyconitCollections.ToList())
//                 {
//                     var dyconitCollectionData = dyconitCollection.Value;
//                     var dyconitCollectionName = dyconitCollection.Key;
//                     if (dyconitCollectionData.ContainsKey("ports")) {
//                         var adminClientPorts = (List<Tuple<int, DateTime>>)dyconitCollectionData["ports"];

//                         foreach (var adminClientPort in adminClientPorts.ToList())
//                         {
//                             // check if we have received a heartbeat from this node
//                             // if not, remove it from the list
//                             if (adminClientPort.Item2.AddSeconds(10) < DateTime.Now)
//                             {
//                                 adminClientPorts.Remove(adminClientPort);

//                                 // notify all other nodes that this node has been removed
//                                 await RemoveNodeAtAdmins(adminClientPort.Item1, dyconitCollectionName);

//                                 Console.WriteLine($"- Removed node {adminClientPort.Item1} from collection {dyconitCollection.Key}");
//                             }
//                         }
//                     }
//                 }
//             }
//         }


//         private async Task RemoveNodeAtAdmins(int adminClientPort, string dyconitCollectionName)
//         {
//             // Send a heartbeat response to the requesting node
//             var removeNodeEvent = new JObject
//             {
//                 { "eventType", "removeNodeEvent" },
//                 { "adminClientPort", adminClientPort },
//                 { "collection", dyconitCollectionName }
//             };

//             // Send heartbeat to all admin clients
//             foreach (var dyconitCollectionData in _dyconitCollections.ToList())
//             {
//                 var adminClientPorts = (List<Tuple<int, DateTime>>)dyconitCollectionData.Value["ports"];

//                 foreach (var otherAdminClientPort in adminClientPorts.ToList())
//                 {
//                     Console.WriteLine($"--- Sending removeNodeEvent to {otherAdminClientPort.Item1}");
//                     // send heartbeat to this node
//                     await SendMessageOverTcp(removeNodeEvent.ToString(), otherAdminClientPort.Item1);
//                 }
//             }
//         }

//         private async void ListenForMessagesAsync()
//         {
//             var listener = new TcpListener(IPAddress.Any, _listenPort);
//             listener.Start();

//             // var client = await listener.AcceptTcpClientAsync();
//             // var reader = new StreamReader(client.GetStream());

//             while (true)
//             {
//                 var client = await listener.AcceptTcpClientAsync().ConfigureAwait(false);
//                 var reader = new StreamReader(client.GetStream());
//                 var message = await reader.ReadToEndAsync().ConfigureAwait(false);

//                 // Parse message and act accordingly
//                 ParseMessageAsync(message);

//                 reader.Close();
//                 client.Close();
//             }
//         }

//         private async void ParseMessageAsync(string message)
//         {
//             await Task.Run(() =>
//             {
//                 // Console.WriteLine($"Received message: {message}");
//                 // Check if message is a newAdminEvent
//                 var json = JObject.Parse(message);


//                 var adminClientPort = json["adminClientPort"]?.ToObject<int>();
//                 var dyconitCollectionName = json["conits"]?["collection"]?.ToString();

//                 var eventType = json["eventType"]?.ToString();
//                 if (eventType == null)
//                 {
//                     Console.WriteLine($"Invalid message received: missing eventType. Message: {message}");
//                     return;
//                 }

//                 switch (eventType)
//                 {
//                     case "newAdminEvent":

//                         if (adminClientPort == null)
//                         {
//                             Console.WriteLine($"-- Invalid newAdminEvent message received: missing adminClientPort. Message: {message}");
//                             break;
//                         }

//                         Console.WriteLine($"-- Received newAdminEvent message. Admin client listening on port {adminClientPort}.");

//                         // Check if we already have a dyconit collection in the dyconitCollections dictionary
//                         // If not, create a new one
//                         if (!_dyconitCollections.ContainsKey(dyconitCollectionName))
//                         {
//                             _dyconitCollections.Add(dyconitCollectionName, new Dictionary<string, object>
//                             {
//                                 // add a ports list containing the port and the last time we got a heartbeat from it
//                                 { "ports", new List<Tuple<int, DateTime>>() },
//                                 { "bounds", new Dictionary<string, int>() }
//                             });
//                             Console.WriteLine($"--- Added dyconit collection '{dyconitCollectionName}' to the dyconit collections.");
//                         }

//                         else
//                         {
//                             Console.WriteLine($"--- Dyconit collection '{dyconitCollectionName}' already exists.");

//                             // check if we already have ports and bounds for this dyconit collection
//                             var dyconitCollectionDat = (Dictionary<string, object>)_dyconitCollections[dyconitCollectionName];

//                             if (!dyconitCollectionDat.ContainsKey("ports"))
//                             {
//                                 dyconitCollectionDat.Add("ports", new List<Tuple<int, DateTime>>());
//                             }
//                             if (!dyconitCollectionDat.ContainsKey("bounds"))
//                             {
//                                 dyconitCollectionDat.Add("bounds", new Dictionary<string, int>());
//                             }
//                         }

//                         var dyconitCollectionData = (Dictionary<string, object>)_dyconitCollections[dyconitCollectionName];

//                         // Add the admin client port to the dyconit collection
//                         ((List<Tuple<int, DateTime>>)dyconitCollectionData["ports"]).Add(new Tuple<int, DateTime>(adminClientPort.Value, DateTime.Now));

//                         Console.WriteLine($"--- Added new admin client listening on port '{adminClientPort}' to dyconit collection '{dyconitCollectionName}'.");

//                         // Add the bounds to the dyconit collection
//                         var conits = json["conits"];

//                         // Check if bounds is still empty
//                         if (((Dictionary<string, int>)dyconitCollectionData["bounds"]).Count == 0)
//                         {
//                             var staleness = conits["Staleness"]?.ToObject<int>();
//                             var orderError = conits["OrderError"]?.ToObject<int>();
//                             var numericalError = conits["NumericalError"]?.ToObject<int>();

//                             ((Dictionary<string, int>)dyconitCollectionData["bounds"]).Add("Staleness", staleness.Value);
//                             ((Dictionary<string, int>)dyconitCollectionData["bounds"]).Add("OrderError", orderError.Value);
//                             ((Dictionary<string, int>)dyconitCollectionData["bounds"]).Add("NumericalError", numericalError.Value);

//                             Console.WriteLine($"--- Added bounds to dyconit collection '{dyconitCollectionName}'.");
//                         }
//                         else
//                         {
//                             Console.WriteLine($"--- Bounds for dyconit collection '{dyconitCollectionName}' already exist.");
//                         }

//                         var ports = ((List<Tuple<int, DateTime>>)dyconitCollectionData["ports"]).ToList();
//                         if (ports.Count > 1)
//                         {

//                             // Send the NewAdminEvent to existing ports
//                             foreach (var port in ports)
//                             {
//                                 if (port.Item1 != adminClientPort)
//                                 {
//                                     var messageToExistingPort = new JObject
//                                     {
//                                         { "eventType", "newNodeEvent" },
//                                         { "port", adminClientPort },
//                                         { "collection", dyconitCollectionName }
//                                     };

//                                     SendMessageOverTcp(messageToExistingPort.ToString(), port.Item1);

//                                     // Send the existing ports to the new admin client

//                                     var xx = new JObject
//                                     {
//                                         { "eventType", "newNodeEvent" },
//                                         { "port", port.Item1 },
//                                         { "collection", dyconitCollectionName }
//                                     };

//                                     SendMessageOverTcp(xx.ToString(), adminClientPort.Value);
//                                 }
//                             }
//                         }




//                         break;
//                     case "heartbeatResponse":
//                         // update the heartbeat time of the admin client
//                         var heartbeatTime = DateTime.Now;
//                         foreach (var collection in _dyconitCollections.ToList())
//                         {
//                             foreach (var port in ((List<Tuple<int, DateTime>>)collection.Value["ports"]).ToList())
//                             {
//                                 if (port.Item1 == adminClientPort)
//                                 {
//                                     // remove the tuple and add a new one with the new heartbeat time
//                                     ((List<Tuple<int, DateTime>>)collection.Value["ports"]).Remove(port);
//                                     ((List<Tuple<int, DateTime>>)collection.Value["ports"]).Add(new Tuple<int, DateTime>(adminClientPort.Value, heartbeatTime));
//                                 }
//                             }
//                         }

//                         break;

//                     case "throughput":
//                         var throughput = json["throughput"]?.ToObject<double>();
//                         var adminPort = json["adminPort"]?.ToObject<int>();
//                         var topic = json["topic"]?.ToString();

//                         if (throughput <= 0)
//                         {
//                             Console.WriteLine($"-- Received throughput message from admin client listening on port {adminPort}, topic {topic}, Throughput: {throughput}. Ignoring message.");
//                             break;
//                         }

//                         Console.WriteLine($"-- Received throughput message from admin client listening on port {adminPort}, topic {topic}, Throughput: {throughput}.");
//                             if (_dyconitCollections[topic].ContainsKey("thresholds") && _dyconitCollections[topic].ContainsKey("rules"))
//                             {
//                                 var throughputThreshold = (int)((JToken)_dyconitCollections[topic]["thresholds"])["throughput"];
//                                 var rules = (JArray)_dyconitCollections[topic]["rules"];

//                                 foreach (var rule in rules)
//                                 {
//                                     var condition = rule["condition"].ToString();
//                                     var actions = rule["actions"];

//                                     var isConditionMet = EvaluateCondition(condition, throughput.Value, throughputThreshold);
//                                     if (isConditionMet)
//                                     {
//                                         foreach (var action in actions)
//                                         {
//                                             var type = action["type"].ToString();
//                                             var value = (double)action["value"];

//                                             var bounds = (Dictionary<string, int>)_dyconitCollections[topic]["bounds"];
//                                             var newBounds = new Dictionary<string, int>();

//                                             foreach (var bound in bounds)
//                                             {
//                                                 int newValue = type == "multiply" ? (int)(bound.Value * value) : (int)(bound.Value - value);
//                                                 newValue = Math.Max(1, newValue); // Ensure value is not less than 1
//                                                 newBounds[bound.Key] = newValue;
//                                             }

//                                             _dyconitCollections[topic]["bounds"] = newBounds;

//                                             var newMessage = new JObject
//                                             {
//                                                 { "eventType", "updateConitEvent" },
//                                                 { "staleness", newBounds["Staleness"] },
//                                                 { "orderError", newBounds["OrderError"] },
//                                                 { "numericalError", newBounds["NumericalError"] },
//                                                 { "collection", topic }
//                                             };

//                                             foreach (var port in ((List<Tuple<int, DateTime>>)_dyconitCollections[topic]["ports"]).ToList())
//                                             {
//                                                 SendMessageOverTcp(newMessage.ToString(), port.Item1);
//                                             }

//                                             Console.WriteLine($"--- Changed bounds of dyconit collection '{topic}' to: {string.Join(", ", newBounds)}.");
//                                         }
//                                     }
//                                 }
//                         }

//                         break;

//                     case "overheadMessage":

//                         var syncThroughput = JsonConvert.DeserializeObject<Dictionary<string, int>>(json["data"].ToString());
//                         adminPort = json["adminPort"]?.ToObject<int>();



//                         foreach (var collection in syncThroughput)
//                         {

//                             Console.WriteLine($"-- Received overhead message from admin client listening on port {adminPort}, collection {collection.Key}, Throughput: {collection.Value}.");

//                             if (_dyconitCollections.ContainsKey(collection.Key))
//                             {
//                                 if (_dyconitCollections[collection.Key].ContainsKey("thresholds") && _dyconitCollections[collection.Key].ContainsKey("rules"))
//                                 {
//                                     var throughputThreshold = (int)((JToken)_dyconitCollections[collection.Key]["thresholds"])["overhead_throughput"];

//                                     Console.WriteLine($"--- Collection: {collection.Key}, Throughput: {collection.Value}, Overhead Threshold: {throughputThreshold}.");

//                                     var rules = (JArray)_dyconitCollections[collection.Key]["rules"];

//                                     foreach (var rule in rules)
//                                     {
//                                         var condition = rule["condition"].ToString();
//                                         var actions = rule["actions"];

//                                         var isConditionMet = EvaluateCondition(condition, collection.Value, throughputThreshold);
//                                         if (isConditionMet)
//                                         {
//                                             foreach (var action in actions)
//                                             {
//                                                 var type = action["type"].ToString();
//                                                 var value = (double)action["value"];

//                                                 var bounds = (Dictionary<string, int>)_dyconitCollections[collection.Key]["bounds"];
//                                                 var newBounds = new Dictionary<string, int>();

//                                                 foreach (var bound in bounds)
//                                                 {
//                                                     int newValue = type == "multiply" ? (int)(bound.Value * value) : (int)(bound.Value - value);
//                                                     newValue = Math.Max(1, newValue); // Ensure value is not less than 1
//                                                     newBounds[bound.Key] = newValue;
//                                                 }

//                                                 _dyconitCollections[collection.Key]["bounds"] = newBounds;

//                                                 var newMessage = new JObject
//                                                 {
//                                                     { "eventType", "updateConitEvent" },
//                                                     { "staleness", newBounds["Staleness"] },
//                                                     { "orderError", newBounds["OrderError"] },
//                                                     { "numericalError", newBounds["NumericalError"] },
//                                                     { "collection", collection.Key }
//                                                 };

//                                                 foreach (var port in ((List<Tuple<int, DateTime>>)_dyconitCollections[collection.Key]["ports"]).ToList())
//                                                 {
//                                                     SendMessageOverTcp(newMessage.ToString(), port.Item1);
//                                                 }

//                                                 Console.WriteLine($"--- Changed bounds of dyconit collection '{collection.Key}' to: {string.Join(", ", newBounds)}.");
//                                             }
//                                         }
//                                     }
//                                 }
//                             }
//                         }
//                         break;

//                     default:
//                         Console.WriteLine($"Unknown message received with eventType '{eventType}': {message}");
//                         break;
//                 }
//             });
//         }

//         bool EvaluateCondition(string condition, double throughput, int threshold)
//         {
//             // Assuming the condition format is "{variable} {operator} {threshold}"
//             var parts = condition.Split(' ');
//             var operatorSymbol = parts[1];

//             switch (operatorSymbol)
//             {
//                 case ">":
//                     return throughput > threshold;
//                 case "<":
//                     return throughput < threshold;
//                 case ">=":
//                     return throughput >= threshold;
//                 case "<=":
//                     return throughput <= threshold;
//                 case "==":
//                     return throughput == threshold;
//                 case "!=":
//                     return throughput != threshold;
//                 default:
//                     return false;
//             }
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
//                 Console.WriteLine($"Failed to send message over TCP: {ex.Message}");

//                 // Remove the admin client from the dyconit collections
//                 foreach (var collection in _dyconitCollections.ToList())
//                 {
//                     foreach (var adminClient in ((List<Tuple<int, DateTime>>)collection.Value["ports"]).ToList())
//                     {
//                         if (adminClient.Item1 == port)
//                         {


//                             ((List<Tuple<int, DateTime>>)collection.Value["ports"]).Remove(adminClient);
//                             // notify all other nodes that this node has been removed
//                             await RemoveNodeAtAdmins(port, collection.Key);

//                             Console.WriteLine($"--- Removed admin client listening on port '{port}' from dyconit collection '{collection.Key}'.");
//                         }
//                     }
//                 }
//             }
//         }}}