// // todo there must be a way to let the dyconits subscribe to each other so they can send messages.
// // todo create a collection of dyconits where we know what these bounds are and which port the admin clients are listening on.
// // todo We must know whether it is a producer or consumer or both.

// using Confluent.Kafka;
// using Confluent.Kafka.Admin;
// using Newtonsoft.Json.Linq;
// using System;
// using System.IO;
// using System.Net;
// using System.Net.Sockets;
// using System.Threading.Tasks;

// namespace Dyconit.Overlord
// {
//     public class DyconitOverlord2
//     {
//         private readonly int _listenPort = 6666;

//         private JObject _dyconitCollections;

//         public DyconitOverlord2()
//         {
//             _dyconitCollections = new JObject();
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
//                             JObject dyconitCollectionData = (JObject)_dyconitCollections[collection];
//                             dyconitCollectionData["thresholds"] = thresholds;
//                             dyconitCollectionData["rules"] = rules;
//                         }
//                         else
//                         {
//                             _dyconitCollections.Add(collection, new JObject(
//                                 new JProperty("thresholds", thresholds),
//                                 new JProperty("rules", rules)
//                             ));
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
//             var heartbeatEvent = new JObject(
//                 new JProperty("eventType", "heartbeatEvent"),
//                 new JProperty("port", _listenPort)
//             );

//             while (true)
//             {
//                 await Task.Delay(1000).ConfigureAwait(false);

//                 // Send heartbeat to all admin clients
//                 foreach (var dyconitCollection in _dyconitCollections)
//                 {
//                     var dyconitCollectionData = (JObject)dyconitCollection.Value;
//                     if (dyconitCollectionData.ContainsKey("ports"))
//                     {
//                         var adminClientPorts = dyconitCollectionData["ports"].ToObject<JArray>();

//                         foreach (var adminClientPort in adminClientPorts)
//                         {
//                             // send heartbeat to this node
//                             SendMessageOverTcp(heartbeatEvent.ToString(), adminClientPort.Value<int>());
//                         }
//                     }
//                 }
//             }
//         }

//         // Every second send a heartbeat event to the DyconitCollection nodes that this overlord is responsible for.
//         private async void KeepTrackOfNodesAsync()
//         {
//             while (true)
//             {
//                 await Task.Delay(5000).ConfigureAwait(false);

//                 // retrieve all unique ports in the _dyconitCollections
//                 var uniquePorts = new JArray();

//                 // TODO: create the unique port array at the new node event.
//                 foreach (var dyconitCollection in _dyconitCollections)
//                 {
//                     var dyconitCollectionData = (JObject)dyconitCollection.Value;
//                     if (dyconitCollectionData.ContainsKey("ports"))
//                     {
//                         var adminClientPorts = dyconitCollectionData["ports"].ToObject<JArray>();

//                         foreach (var adminClientPort in adminClientPorts)
//                         {
//                             if (!uniquePorts.Contains(adminClientPort))
//                             {
//                                 uniquePorts.Add(adminClientPort);
//                             }
//                         }
//                     }
//                 }

//                 // send a request to each node to check its status
//                 // Unique ports is a list of ports that we need to send a request to.
//                 // Each entry in the list is a JObject with the following properties:
//                 // - port: the port number
//                 // - lastHeartbeat: the timestamp of the last heartbeat
//                 foreach (var port in uniquePorts)
//                 {
//                     // check if the last heartbeat is older than 5 seconds
//                     // if so, send a request to this node to check its status
//                     // if not, do nothing
//                     var lastHeartbeat = port["lastHeartbeat"].Value<DateTime>();
//                     var now = DateTime.Now;

//                     if (now.Subtract(lastHeartbeat).TotalSeconds > 10)
//                     {
//                         // remove this port from every dyconitCollection
//                         foreach (var dyconitCollection in _dyconitCollections)
//                         {
//                             var dyconitCollectionData = (JObject)dyconitCollection.Value;
//                             if (dyconitCollectionData.ContainsKey("ports"))
//                             {
//                                 var adminClientPorts = dyconitCollectionData["ports"].ToObject<JArray>();

//                                 foreach (var adminClientPort in adminClientPorts)
//                                 {
//                                     if (adminClientPort.Value<int>() == port["port"].Value<int>())
//                                     {
//                                         adminClientPorts.Remove(adminClientPort);
//                                         break;
//                                     }
//                                 }
//                             }
//                         }

//                         // remove this port from the uniquePorts list
//                         uniquePorts.Remove(port);

//                         // Send a heartbeat response to the requesting node
//                         var removeNodeEvent = new JObject
//                         {
//                             { "eventType", "removeNodeEvent" },
//                             { "adminClientPort", port["port"] },
//                             { "collectionName", _dyconitCollections["collectionName"] }
//                         };

//                         // send the removeNodeEvent to all admin clients in uniquePorts
//                         foreach (var uniquePort in uniquePorts)
//                         {
//                             SendMessageOverTcp(removeNodeEvent.ToString(), uniquePort.Value<int>());
//                         }
//                     }
//                 }
//             }
//         }

// private async void ListenForMessagesAsync()
// {
//     var listener = new TcpListener(IPAddress.Any, _listenPort);
//     listener.Start();

//     while (true)
//     {
//         var client = await listener.AcceptTcpClientAsync().ConfigureAwait(false);
//         var reader = new StreamReader(client.GetStream());
//         var message = await reader.ReadToEndAsync().ConfigureAwait(false);

//         // Parse message and act accordingly
//         await ParseMessageAsync(message);

//         reader.Close();
//         client.Close();
//     }
// }

// private async Task ParseMessageAsync(string message)
// {
//     await Task.Run(() =>
//     {
//         var json = JObject.Parse(message);
//         var adminClientPort = json["adminClientPort"]?.ToObject<int>();
//         var dyconitCollection = json["conits"]?["collection"]?.ToString();
//         var eventType = json["eventType"]?.ToString();

//         if (eventType == null)
//         {
//             Console.WriteLine($"Invalid message received: missing eventType. Message: {message}");
//             return;
//         }

//         switch (eventType)
//         {
//             case "newAdminEvent":
//                 if (adminClientPort == null)
//                 {
//                     Console.WriteLine($"-- Invalid newAdminEvent message received: missing adminClientPort. Message: {message}");
//                     break;
//                 }
//                 Console.WriteLine($"-- Received newAdminEvent message. Admin client listening on port {adminClientPort}.");

//                 if (!_dyconitCollections.ContainsKey(dyconitCollection))
//                 {
//                     _dyconitCollections.Add(dyconitCollection, new JObject
//                     {
//                         { "ports", new JArray() },
//                         { "bounds", new JObject() }
//                     });
//                     Console.WriteLine($"--- Added dyconit collection '{dyconitCollection}' to the dyconit collections.");
//                 }
//                 else
//                 {
//                     Console.WriteLine($"--- Dyconit collection '{dyconitCollection}' already exists.");

//                     var dyconitCollectionData = (JObject)_dyconitCollections[dyconitCollection];

//                     if (!dyconitCollectionData.ContainsKey("ports"))
//                     {
//                         dyconitCollectionData["ports"] = new JArray();
//                     }
//                     if (!dyconitCollectionData.ContainsKey("bounds"))
//                     {
//                         dyconitCollectionData["bounds"] = new JObject();
//                     }
//                 }

//                 var dyconitCollectionData = (JObject)_dyconitCollections[dyconitCollection];

//                 ((JArray)dyconitCollectionData["ports"]).Add(new JObject
//                 {
//                     { "port", adminClientPort.Value },
//                     { "time", DateTime.Now }
//                 });

//                 Console.WriteLine($"--- Added new admin client listening on port '{adminClientPort}' to dyconit collection '{dyconitCollection}'.");

//                 var conits = json["conits"];

//                 if (((JObject)dyconitCollectionData["bounds"]).Count == 0)
//                 {
//                     var staleness = conits["Staleness"]?.ToObject<int>();
//                     var orderError = conits["OrderError"]?.ToObject<int>();
//                     var numericalError = conits["NumericalError"]?.ToObject<int>();

//                     ((JObject)dyconitCollectionData["bounds"]).Add("Staleness", staleness.Value);
//                     ((JObject)dyconitCollectionData["bounds"]).Add("OrderError", orderError.Value);
//                     ((JObject)dyconitCollectionData["bounds"]).Add("NumericalError", numericalError.Value);

//                     Console.WriteLine($"--- Added bounds to dyconit collection '{dyconitCollection}'.");
//                 }
//                 else
//                 {
//                     Console.WriteLine($"--- Bounds for dyconit collection '{dyconitCollection}' already exist.");
//                 }

//                 Console.WriteLine($"--- Bounds: {string.Join(", ", dyconitCollectionData["bounds"])}");

//                 Console.WriteLine("--- Current dyconit collections:");
//                 foreach (var collection in _dyconitCollections)
//                 {
//                     Console.WriteLine($"---- Collection: {collection.Key}");
//                     Console.WriteLine($"---- Ports and heartbeat time: {string.Join(", ", collection.Value["ports"])}");
//                     Console.WriteLine($"---- Bounds: {string.Join(", ", collection.Value["bounds"])}");
//                 }

//                 if (((JArray)dyconitCollectionData["ports"]).Count > 1)
//                 {
//                     Console.WriteLine($"--- Sending newNodeEvent to other admin clients in dyconit collection '{dyconitCollection}'.");

//                     var newMessage = new JObject
//                     {
//                         { "eventType", "newNodeEvent" },
//                         { "port", adminClientPort },
//                         { "collection", dyconitCollection }
//                     };

//                     foreach (var port in ((JArray)dyconitCollectionData["ports"]).ToList())
//                     {
//                         if (port["port"].ToObject<int>() != adminClientPort)
//                         {
//                             SendMessageOverTcp(newMessage.ToString(), port["port"].ToObject<int>());

//                             newMessage["port"] = port["port"];
//                             SendMessageOverTcp(newMessage.ToString(), adminClientPort.Value);
//                         }
//                     }
//                 }

//                 break;

//             case "heartbeatResponse":
//                 var heartbeatTime = DateTime.Now;
//                 foreach (var collection in _dyconitCollections)
//                 {
//                     foreach (var port in ((JArray)collection.Value["ports"]).ToList())
//                     {
//                         if (port["port"].ToObject<int>() == adminClientPort)
//                         {
//                             port["time"] = heartbeatTime;
//                         }
//                     }
//                 }

//                 break;

//             case "throughput":
//                 var throughput = json["throughput"]?.ToObject<double>();
//                 var adminPort = json["adminPort"]?.ToObject<int>();
//                 var topic = json["topic"]?.ToString();

//                 if (throughput <= 0)
//                 {
//                     Console.WriteLine($"-- Received throughput message from admin client listening on port {adminPort}, topic {topic}, Throughput: {throughput}. Ignoring message.");
//                     break;
//                 }

//                 Console.WriteLine($"-- Received throughput message from admin client listening on port {adminPort}, topic {topic}, Throughput: {throughput}.");

//                 if (_dyconitCollections.ContainsKey(topic) && _dyconitCollections[topic].ContainsKey("thresholds") && _dyconitCollections[topic].ContainsKey("rules"))
//                 {
//                     var throughputThreshold = (int)_dyconitCollections[topic]["thresholds"]["throughput"];
//                     var rules = (JArray)_dyconitCollections[topic]["rules"];

//                     foreach (var rule in rules)
//                     {
//                         var condition = rule["condition"].ToString();
//                         var actions = rule["actions"];

//                         var isConditionMet = EvaluateCondition(condition, throughput.Value, throughputThreshold);
//                         if (isConditionMet)
//                         {
//                             foreach (var action in actions)
//                             {
//                                 var type = action["type"].ToString();
//                                 var value = (double)action["value"];

//                                 var bounds = (JObject)_dyconitCollections[topic]["bounds"];
//                                 var newBounds = new JObject();

//                                 foreach (var bound in bounds)
//                                 {
//                                     var newValue = type == "multiply" ? (int)(bound.Value<int>() * value) : (int)(bound.Value<int>() - value);
//                                     newValue = Math.Max(1, newValue); // Ensure value is not less than 1
//                                     newBounds[bound.Key] = newValue;
//                                 }

//                                 _dyconitCollections[topic]["bounds"] = newBounds;

//                                 var newMessage = new JObject
//                                 {
//                                     { "eventType", "updateConitEvent" },
//                                     { "staleness", newBounds["Staleness"] },
//                                     { "orderError", newBounds["OrderError"] },
//                                     { "numericalError", newBounds["NumericalError"] },
//                                     { "collection", topic }
//                                 };

//                                 foreach (var port in ((JArray)_dyconitCollections[topic]["ports"]).ToList())
//                                 {
//                                     SendMessageOverTcp(newMessage.ToString(), port["port"].ToObject<int>());
//                                 }

//                                 Console.WriteLine($"--- Changed bounds of dyconit collection '{topic}' to: {string.Join(", ", newBounds)}.");
//                             }
//                         }
//                     }
//                 }

//                 break;

//             default:
//                 Console.WriteLine($"Unknown message received with eventType '{eventType}': {message}");
//                 break;
//         }
//     });

// }



// bool EvaluateCondition(string condition, double throughput, int threshold)
//         {
//             // Assuming the condition format is "{variable} {operator} {threshold}"
//             var parts = condition.Split(' ');
//             var operatorSymbol = parts[1];

//             switch (operatorSymbol)
//             {
//                 case ">=":
//                     return throughput >= threshold;
//                 case "<":
//                     return throughput < threshold;
//                 // Add more cases for other operators as needed
//                 default:
//                     throw new NotSupportedException($"Operator '{operatorSymbol}' is not supported.");
//             }
//         }



//     }
// }
