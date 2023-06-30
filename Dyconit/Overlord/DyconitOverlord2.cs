// todo there must be a way to let the dyconits subscribe to each other so they can send messages.
// todo create a collection of dyconits where we know what these bounds are and which port the admin clients are listening on.
// todo We must know whether it is a producer or consumer or both.

using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Dyconit.Helper;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace Dyconit.Overlord
{
    public class Bounds
    {
        public int ?Staleness { get; set; }
        public int ?NumericalError { get; set; }
    }

    public class Node
    {
        public int ?Port { get; set; }
        public DateTime ?LastHeartbeatTime { get; set; }
        public Bounds ?Bounds { get; set; }
    }

    public class Thresholds
    {
        public int ?Throughput { get; set; }
        [JsonProperty("overhead_throughput")]
        public int ?OverheadThroughput { get; set; }
    }

    public class Action
    {
        public string ?Type { get; set; }
        public double ?Value { get; set; }
    }

    public class Rule
    {
        public string ?Condition { get; set; }
        public List<Action> ?Actions { get; set; }
    }

    public class Collection
    {
        public string ?Name { get; set; }
        public List<Node> ?Nodes { get; set; }
        public Thresholds ?Thresholds { get; set; }
        public List<Rule> ?Rules { get; set; }
    }

    public class RootObject
    {
        public List<Collection> ?Collections { get; set; }
    }
    public class DyconitOverlord2
    {
        private readonly int _listenPort = 6666;
        private RootObject _dyconitCollections;
        private CancellationTokenSource? _cancellationTokenSource;

        public DyconitOverlord2()
        {
            _dyconitCollections = new RootObject
            {
                Collections = new List<Collection>()
            };
        }

        public void ParsePolicies()
        {
            string policiesFolderPath = "..\\Policies";

            if (Directory.Exists(policiesFolderPath))
            {
                Console.WriteLine("Policies folder found.");

                string[] policyFiles = Directory.GetFiles(policiesFolderPath, "*policy*.json");

                Console.WriteLine($"Found {policyFiles.Length} policy files.");

                foreach (string policyFile in policyFiles)
                {
                    string jsonContent = File.ReadAllText(policyFile);
                    JObject policyJson = JObject.Parse(jsonContent);
                    JArray ?collectionNames = policyJson["collectionNames"] as JArray;
                    JToken ?thresholds = policyJson["thresholds"];
                    JArray ?rules = policyJson["rules"] as JArray;

                    // add some checks to see if collections, thresholds and rules are not null
                    if (collectionNames == null || thresholds == null || rules == null)
                    {
                        Console.WriteLine("Policy file is not valid.");
                        continue;
                    }

                    foreach (string ?collectionName in collectionNames)
                    {
                        Collection ?collection = new Collection
                        {
                            Name = collectionName,
                            Thresholds = thresholds != null ? new Thresholds
                            {
                                Throughput = thresholds.Value<int>("throughput"),
                                OverheadThroughput = thresholds.Value<int>("overhead_throughput")
                            } : null,
                            Rules = new List<Rule>()
                        };

                        foreach (JObject rule in rules ?? new JArray())
                        {
                            Rule? newRule = new Rule
                            {
                                Condition = rule.Value<string>("condition"),
                                Actions = new List<Action>()
                            };

                            foreach (JObject action in rule.Value<JArray>("actions") ?? new JArray())
                            {
                                Action? newAction = new Action
                                {
                                    Type = action.Value<string>("type"),
                                    Value = action.Value<double>("value")
                                };

                                newRule.Actions?.Add(newAction);
                            }

                            collection.Rules?.Add(newRule);
                        }

                        _dyconitCollections.Collections?.Add(collection);

                        Console.WriteLine($"Added policy for collection {collectionName}.");
                    }
                }
            }
        }

        public void StartListening()
        {
            _cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = _cancellationTokenSource.Token;
            Console.WriteLine("- Dyconit overlord started listening.");
            Task.Run(() => ListenForMessagesAsync());
        }

        public void StopListening()
        {
            Console.WriteLine("- Dyconit overlord stopped.");
            _cancellationTokenSource?.Cancel();
            _cancellationTokenSource?.Dispose();
            _cancellationTokenSource = null!;
        }

        private async Task ListenForMessagesAsync()
        {
            var listener = new TcpListener(IPAddress.Any, _listenPort);
            listener.Start();

            while (true)
            {
                var client = await listener.AcceptTcpClientAsync().ConfigureAwait(false);
                _ = ProcessMessageAsync(client);
            }
        }

        private async Task ProcessMessageAsync(TcpClient client)
        {
            try
            {
                using (client)
                using (var reader = new StreamReader(client.GetStream()))
                {
                    var message = await reader.ReadToEndAsync().ConfigureAwait(false);
                    await ParseMessageAsync(message).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error processing message: {ex.Message}");
            }
        }


        private Task ParseMessageAsync(string message)
        {
            var json = JObject.Parse(message);
            var eventType = json.Value<string>("eventType");
            var adminClientPort = json.Value<int?>("adminClientPort");

            switch (eventType)
            {
                case "newAdminEvent":
                    Console.WriteLine($"-- New admin event received from port {adminClientPort}.");
                    var conits = json["conits"] as JObject;
                    if (adminClientPort.HasValue && conits != null)
                    {
                        ProcessNewAdminEvent(adminClientPort.Value, conits);
                        WelcomeNewNode(adminClientPort.Value, conits);
                    };
                    break;
                case "heartbeatResponse":
                    Console.WriteLine($"-- Heartbeat response received from port {adminClientPort}.");
                    ProcessHeartbeatResponse();
                    break;
                case "throughput":
                    Console.WriteLine($"-- Throughput received from port {adminClientPort}.");
                    var throughput = json.Value<int?>("throughput");
                    var collectionName = json.Value<string>("collectionName");
                    if (adminClientPort.HasValue && throughput.HasValue && collectionName != null)
                    {
                        ProcessThroughput(throughput, adminClientPort, collectionName, json);
                    }
                    break;
                case "overheadMessage":
                    Console.WriteLine($"-- Overhead message received from port {adminClientPort}.");
                    var syncThroughput = json["data"] as JObject;
                    if (adminClientPort.HasValue && syncThroughput != null)
                    {
                        ProcessOverheadMessage(adminClientPort.Value, syncThroughput);
                    }
                    break;
                default:
                    break;
            }

            return Task.CompletedTask;

        }

        private void ProcessOverheadMessage(int value, JObject syncThroughput)
        {
            foreach (var collection in syncThroughput)
            {
                var collectionName = collection.Key;
                var throughputToken = collection.Value; // Store the JToken to avoid null reference warnings
                var throughput = throughputToken?.Value<double>(); // Use null conditional operator to handle possible null reference

                var adminClientPort = value;

                // check if the collection has thresholds and rules
                var collectionObj = _dyconitCollections.Collections?.FirstOrDefault(c => c.Name == collectionName);
                if (collectionObj == null || collectionObj.Thresholds == null || collectionObj.Rules == null)
                {
                    Console.WriteLine($"-- Collection {collectionName} has no thresholds or rules. Ignoring...");
                    continue;
                }
                if (throughput != null)
                {
                    ApplyRules(collectionObj, collectionObj.Thresholds.OverheadThroughput, throughput.Value, adminClientPort);
                };
            }
        }

        private void ProcessThroughput(int? throughput, int? adminClientPort, string? collectionName, JObject json)
        {
            if (throughput <= 0 || throughput == null || collectionName == null || adminClientPort == null)
            {
                Console.WriteLine($"-- Received invalid throughput: {throughput}. Ignoring...");
                return;
            }

            Console.WriteLine($"-- Received throughput: {throughput} for adminCLient {adminClientPort} in collection {collectionName}.");

            // check if the collection has thresholds and rules
            var collection = _dyconitCollections.Collections?.FirstOrDefault(c => c.Name == collectionName);
            if (collection == null || collection.Thresholds == null || collection.Rules == null)
            {
                Console.WriteLine($"-- Collection {collectionName} has no thresholds or rules. Ignoring...");
                return;
            }
            ApplyRules(collection, collection.Thresholds.Throughput, throughput.Value, adminClientPort.Value);
        }

        private async void ApplyRules(Collection collection, int? threshold, double throughput, int? adminClientPort)
        {
            if (collection.Rules == null || threshold == null)
            {
                Console.WriteLine($"-- Collection {collection.Name} has no rules or threshold. Ignoring...");
                return;
            }
            foreach (var rule in collection.Rules)
            {
                var condition = rule.Condition;
                var actions = rule.Actions;

                if (condition == null || actions == null)
                {
                    Console.WriteLine($"-- Rule for collection {collection.Name} has no condition or actions. Ignoring...");
                    continue;
                }

                var isConditionMet = EvaluateCondition(condition, throughput, threshold);
                if (isConditionMet)
                {
                    Console.WriteLine($"-- Condition {condition} for collection {collection.Name} is true. Applying actions...");
                    ApplyActions(collection, actions, adminClientPort);
                    await SendUpdatedBoundsToCollection(collection, adminClientPort);
                }
            }
        }

        private async Task SendUpdatedBoundsToCollection(Collection collection, int? adminClientPort)
        {
            // Send the new bounds for the adminClient to all other nodes in the collection
            var nodes = collection.Nodes?.Where(n => n.Port != adminClientPort);

            if (nodes == null)
            {
                Console.WriteLine($"-- Collection {collection.Name} has no nodes. Ignoring...");
                return;
            }

            foreach (var node in nodes)
            {
                if (node.Bounds == null)
                {
                    Console.WriteLine($"-- Node {node.Port} has no bounds. Ignoring...");
                    continue;
                }

                var message = new JObject
                {
                    ["eventType"] = "updateConitEvent",
                    ["adminClientPort"] = adminClientPort,
                    ["bounds"] = new JObject
                    {
                        ["Staleness"] = node.Bounds.Staleness ?? 1,
                        ["NumericalError"] = node.Bounds.NumericalError ?? 1,
                    }
                };

                await SendMessageOverTcp(message.ToString(), node.Port ?? 0).ConfigureAwait(false);
            }
        }


        bool EvaluateCondition(string condition, double throughput, int? threshold)
        {
            // Assuming the condition format is "{variable} {operator} {threshold}"
            var parts = condition.Split(' ');
            var operatorSymbol = parts[1];

            switch (operatorSymbol)
            {
                case ">":
                    return throughput > threshold;
                case "<":
                    return throughput < threshold;
                case ">=":
                    return throughput >= threshold;
                case "<=":
                    return throughput <= threshold;
                case "==":
                    return throughput == threshold;
                case "!=":
                    return throughput != threshold;
                default:
                    return false;
            }
        }

        private void ApplyActions(Collection collection, List<Action> actions, int? adminClientPort)
        {
            foreach (var action in actions)
            {
                var actionType = action.Type;
                int? actionValue = (int?)action.Value;

                // retrieve the bounds of the adminClientPort for the collection
                var bounds = collection.Nodes?.FirstOrDefault(n => n.Port == adminClientPort)?.Bounds;

                if (bounds == null)
                {
                    Console.WriteLine($"-- No bounds found for adminClientPort {adminClientPort} in collection {collection.Name}. Ignoring...");
                    continue;
                }

                // apply the action
                switch (actionType)
                {
                    case "add":
                        bounds.Staleness = bounds.Staleness.HasValue ? (bounds.Staleness + actionValue < 1 ? 1 : bounds.Staleness + actionValue) : null;
                        bounds.NumericalError = bounds.NumericalError.HasValue ? (bounds.NumericalError + actionValue < 1 ? 1 : bounds.NumericalError + actionValue) : null;
                        break;
                    case "subtract":
                        bounds.Staleness = bounds.Staleness.HasValue ? (bounds.Staleness - actionValue < 1 ? 1 : bounds.Staleness - actionValue) : null;
                        bounds.NumericalError = bounds.NumericalError.HasValue ? (bounds.NumericalError - actionValue < 1 ? 1 : bounds.NumericalError - actionValue) : null;
                        break;
                    case "multiply":
                        bounds.Staleness = bounds.Staleness.HasValue ? (bounds.Staleness * actionValue < 1 ? 1 : bounds.Staleness * actionValue) : null;
                        bounds.NumericalError = bounds.NumericalError.HasValue ? (bounds.NumericalError * actionValue < 1 ? 1 : bounds.NumericalError * actionValue) : null;
                        break;
                    case "divide":
                        bounds.Staleness = bounds.Staleness.HasValue ? (bounds.Staleness / actionValue < 1 ? 1 : bounds.Staleness / actionValue) : null;
                        bounds.NumericalError = bounds.NumericalError.HasValue ? (bounds.NumericalError / actionValue < 1 ? 1 : bounds.NumericalError / actionValue) : null;
                        break;
                    default:
                        break;
                }
            }
        }

        private void ProcessNewAdminEvent(int adminClientPort, JObject? conits)
        {
            var collectionName = conits?.Value<string>("collectionName");
            var staleness = conits?.Value<int?>("Staleness");
            var numericalError = conits?.Value<int?>("NumericalError");

            if (collectionName == null || staleness == null || numericalError == null)
            {
                Console.WriteLine("Invalid conit object.");
                return;
            }

            var collection = _dyconitCollections.Collections?.FirstOrDefault(c => c.Name == collectionName);
            if (collection == null)
            {
                collection = new Collection
                {
                    Name = collectionName,
                    Nodes = new List<Node>()
                };
                _dyconitCollections.Collections?.Add(collection);
            }
            else if (collection.Nodes == null)
            {
                collection.Nodes = new List<Node>();
            }

            var node = collection.Nodes?.FirstOrDefault(n => n.Port == adminClientPort);
            if (node == null)
            {
                node = new Node
                {
                    Port = adminClientPort,
                    LastHeartbeatTime = DateTime.Now,
                    Bounds = new Bounds
                    {
                        Staleness = staleness,
                        NumericalError = numericalError
                    }
                };
            }
            else
            {
                node.LastHeartbeatTime = DateTime.Now;
                node.Bounds = new Bounds
                {
                    Staleness = staleness,
                    NumericalError = numericalError
                };
            }

            collection.Nodes?.Add(node);

            // print the entire _dyconitCollections object
            // Console.WriteLine(JsonConvert.SerializeObject(_dyconitCollections, Formatting.Indented));
        }

        private async void WelcomeNewNode(int adminClientPort, JObject? conits)
        {
            var collectionName = conits?.Value<string>("collectionName");
            var staleness = conits?.Value<int?>("Staleness");
            var numericalError = conits?.Value<int?>("NumericalError");

            // Check if there are more than 1 nodes in the collection
            var collection = _dyconitCollections.Collections?.FirstOrDefault(c => c.Name == collectionName);
            if (collection != null && collection.Nodes != null && collection.Nodes.Count > 1)
            {
                // Send a new node event to all the other nodes in the collection
                foreach (var node in collection.Nodes ?? Enumerable.Empty<Node>())
                {
                    if (node.Port != null && node.Port != adminClientPort)
                    {
                        var newAdminMessage = new JObject
                        {
                            ["eventType"] = "newNodeEvent",
                            ["port"] = adminClientPort,
                            ["collectionName"] = collectionName,
                            ["staleness"] = staleness,
                            ["numericalError"] = numericalError
                        };

                        await SendMessageOverTcp(newAdminMessage.ToString(), node.Port.Value);
                        Console.WriteLine($"Sent new node event to port {node.Port.Value}.");

                        var welcomeMessage = new JObject
                        {
                            ["eventType"] = "newNodeEvent",
                            ["port"] = node.Port.Value,
                            ["collectionName"] = collectionName,
                            ["staleness"] = staleness,
                            ["numericalError"] = numericalError
                        };

                        await SendMessageOverTcp(welcomeMessage.ToString(), adminClientPort);
                        Console.WriteLine($"Sent welcome message to port {adminClientPort}.");
                    }
                }
            }
        }

        private void ProcessHeartbeatResponse()
        {
            var heartbeatTime = DateTime.Now;

            // update last heartbeat time for node
            foreach (var collection in _dyconitCollections.Collections ?? new List<Collection>())
            {
                foreach (var node in collection.Nodes ?? new List<Node>())
                {
                    if (node.LastHeartbeatTime.HasValue)
                    {
                        node.LastHeartbeatTime = heartbeatTime;
                    }
                }
            }
        }

        public async void SendHeartbeatAsync()
        {
            var heartbeatMessage = new JObject
            {
                ["eventType"] = "heartbeatEvent",
                ["port"] = _listenPort
            };


            while (true)
            {
                await Task.Delay(10000);

                // send heartbeat to all nodes


                foreach (var collection in _dyconitCollections.Collections ?? new List<Collection>())
                {
                    foreach (var node in collection.Nodes ?? new List<Node>())
                    {
                        if (node.Port != null)
                        {
                            await SendMessageOverTcp(heartbeatMessage.ToString(), node.Port.Value);
                            Console.WriteLine($"Sent heartbeat to port {node.Port.Value}.");
                        }
                    }
                }
             }
        }

        public async void KeepTrackOfNodesAsync()
        {
            while (true)
            {
                await Task.Delay(10000);

                // remove nodes that have not sent a heartbeat in the last 30 seconds
                var heartbeatTime = DateTime.Now;
                foreach (var collection in _dyconitCollections.Collections ?? new List<Collection>())
                {
                    foreach (var node in collection.Nodes ?? new List<Node>())
                    {
                        if (node.LastHeartbeatTime.HasValue && heartbeatTime.Subtract(node.LastHeartbeatTime.Value).TotalSeconds > 30)
                        {
                            collection.Nodes?.Remove(node);
                            Console.WriteLine($"Removed node with port {node.Port} from collection {collection.Name}.");
                        }
                    }
                }
            }
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
                Console.WriteLine($"Failed to send message to port {port}. Exception: {ex.Message}. Removing from collection...");

                // Remove the admin client from the dyconit collections
                var collection = _dyconitCollections.Collections?.FirstOrDefault(c => c.Nodes != null && c.Nodes.Any(n => n.Port == port));
                if (collection != null)
                {
                    var node = collection.Nodes?.FirstOrDefault(n => n.Port == port);
                    if (node != null)
                    {
                        collection.Nodes?.Remove(node);
                    }
                }
            }
        }
    }
}