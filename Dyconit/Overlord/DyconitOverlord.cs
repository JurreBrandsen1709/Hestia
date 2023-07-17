using Dyconit.Helper;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Serilog;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

namespace Dyconit.Overlord
{
    public class DyconitOverlord
    {
        private readonly int _listenPort = 6666;
        private RootObject _dyconitCollections;
        private CancellationTokenSource? _cancellationTokenSource;
        private Dictionary<int, double> _previousThroughput = new Dictionary<int, double>();

        public DyconitOverlord()
        {
            ConfigureLogging();
            _dyconitCollections = new RootObject
            {
                Collections = new List<Collection>()
            };

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

        public void ParsePolicies()
        {
            string policiesFolderPath = "..\\Policies";

            if (Directory.Exists(policiesFolderPath))
            {
                string[] policyFiles = Directory.GetFiles(policiesFolderPath, "*policy*.json");
                foreach (string policyFile in policyFiles)
                {
                    string jsonContent = File.ReadAllText(policyFile);
                    JObject policyJson = JObject.Parse(jsonContent);
                    JArray ?collectionNames = policyJson["collectionNames"] as JArray;
                    JToken ?thresholds = policyJson["thresholds"];
                    JArray ?rules = policyJson["rules"] as JArray;
                    int ?averageSizeThroughput = policyJson.Value<int>("averageSizeThroughput");
                    int ?averageSizeOverheadThroughput = policyJson.Value<int>("averageSizeOverheadThroughput");

                    // add some checks to see if collections, thresholds and rules are not null
                    if (collectionNames == null || thresholds == null || rules == null)
                    {
                        Log.Error("Policy file is not valid.");
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
                            Rules = new List<Rule>(),

                            // add some checks to see if averageSizeThroughput and averageSizeOverheadThroughput are not null. In that case, set new MovingAverage to 1.
                            MovingAverageThroughput = averageSizeThroughput != 0 ? new MovingAverage(averageSizeThroughput.Value) : null,
                            MovingAverageOverheadThroughput = averageSizeOverheadThroughput != 0 ? new MovingAverage(averageSizeOverheadThroughput.Value) : null
                        };

                        foreach (JObject rule in rules ?? new JArray())
                        {
                            Rule? newRule = new Rule
                            {
                                Condition = rule.Value<string>("condition"),
                                PolicyType = rule.Value<string>("policyType"),
                                SmoothingFactor = rule.Value<double?>("smoothingFactor"),
                                PolicyActions = new List<PolicyAction>()
                            };

                            foreach (JObject action in rule.Value<JArray>("actions") ?? new JArray())
                            {
                                PolicyAction? newAction = new PolicyAction
                                {
                                    Type = action.Value<string>("type"),
                                    Value = action.Value<double>("value")
                                };

                                newRule.PolicyActions?.Add(newAction);
                            }
                            collection.Rules?.Add(newRule);
                        }

                        _dyconitCollections.Collections?.Add(collection);
                    }
                }
            }
        }

        public void StartListening()
        {
            _cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = _cancellationTokenSource.Token;
            Task.Run(() => ListenForMessagesAsync());
        }

        public void StopListening()
        {
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
                Log.Error($"Error processing message: {ex.Message}");
            }
        }


        private Task ParseMessageAsync(string message)
        {
            var json = JObject.Parse(message);
            var eventType = json.Value<string>("eventType");
            var adminClientPort = json.Value<int?>("port");

            switch (eventType)
            {
                case "newAdminEvent":
                    var conits = json["conits"] as JObject;
                    if (adminClientPort.HasValue && conits != null)
                    {
                        ProcessNewAdminEvent(adminClientPort.Value, conits);
                        WelcomeNewNode(adminClientPort.Value, conits);
                    };
                    break;
                case "heartbeatResponse":
                    ProcessHeartbeatResponse();
                    break;
                case "throughput":
                    var throughput = json.Value<double?>("throughput");
                    var collectionName = json.Value<string>("collectionName");
                    if (adminClientPort.HasValue && throughput.HasValue && collectionName != null)
                    {
                        ProcessThroughput(throughput, adminClientPort, collectionName, json);
                    }
                    break;
                case "overheadMessage":;
                    var syncThroughput = json["data"] as JObject;
                    if (adminClientPort.HasValue && syncThroughput != null)
                    {
                        ProcessOverheadMessage(adminClientPort.Value, syncThroughput);
                    }
                    break;
                case "finishedEvent":

                    Console.WriteLine("!!!!!!!!!!!!!!!!!!!Finished event received!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                    var data = json["data"] as JObject;
                    var collecitonName = data?.Value<string>("collectionName");

                    if (collecitonName != null && adminClientPort.HasValue && data != null)
                    {
                        ProcessFinishedEvent(adminClientPort.Value, collecitonName, data);
                    }
                    break;
                default:
                    break;
            }

            return Task.CompletedTask;

        }

        private async void ProcessFinishedEvent(int port, string collecitonName, JObject data)
        {
            // send a syncResponse to every node in the collectionName except the one that sent the finishedEvent
            var collection = _dyconitCollections.Collections?.FirstOrDefault(c => c.Name == collecitonName);
            if (collection == null)
            {
                return;
            }

            var nodes = collection.Nodes?.Where(n => n.Port != port);
            if (nodes == null)
            {
                return;
            }

            foreach (var node in nodes)
            {
                var syncResponse = new JObject
                {
                    ["eventType"] = "syncResponse",
                    ["port"] = port,
                    ["data"] = data
                    ["collection"] = collecitonName
                };

                Log.Warning($"-- Sending FINISHED DATA to node {node.Port} for collection {collecitonName}");

                await SendMessageOverTcp(syncResponse.ToString(), node.Port!.Value);
            }
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

                if (collectionObj == null || collectionObj.Thresholds == null || collectionObj.Rules == null || throughput == null)
                {
                    continue;
                }

                if (throughput <= 0)
                {
                    Log.Warning($"-- Received invalid overhead throughput: {throughput}. Ignoring...");
                    continue;
                }

                if (throughput != null)
                {
                    collectionObj.MovingAverageOverheadThroughput?.Add(throughput.Value);
                    ApplyRules(collectionObj, collectionObj.Thresholds.OverheadThroughput, throughput.Value, adminClientPort, collectionObj.MovingAverageOverheadThroughput);
                };
            }
        }

        private void ProcessThroughput(double? throughput, int? adminClientPort, string? collectionName, JObject json)
        {
            if (throughput <= 0 || throughput == null || collectionName == null || adminClientPort == null)
            {
                Log.Error($"-- Received invalid throughput: {throughput}. Ignoring...");
                return;
            }

            // check if the collection has thresholds and rules
            var collection = _dyconitCollections.Collections?.FirstOrDefault(c => c.Name == collectionName);

            if (collection == null || collection.Thresholds == null || collection.Rules == null)
            {
                return;
            }

            collection.MovingAverageThroughput?.Add(throughput.Value);
            ApplyRules(collection, collection.Thresholds.Throughput, throughput.Value, adminClientPort.Value, collection.MovingAverageThroughput);
        }

        private async void ApplyRules(Collection collection, int? threshold, double throughput, int? adminClientPort, MovingAverage? movingAverage)
        {
            if (collection.Rules == null || threshold == null)
            {
                return;
            }

            foreach (var rule in collection.Rules)
            {
                var condition = rule.Condition;
                var policyActions = rule.PolicyActions;
                var policyType = rule.PolicyType;
                var smoothingFactor = rule.SmoothingFactor;


                if (condition == null || policyActions == null)
                {
                    continue;
                }

                if (policyType == "exponentialSmoothing")
                {
                    throughput = ExponentialSmoothing(throughput, smoothingFactor, adminClientPort);
                }

                var isConditionMet = EvaluateCondition(condition, throughput, threshold, movingAverage);

                if (isConditionMet)
                {

                    ApplyActions(collection, policyActions, adminClientPort);
                    await SendUpdatedBoundsToCollection(collection, adminClientPort);
                }
            }
        }

        private double ExponentialSmoothing(double throughput, double? smoothingFactor, int? adminClientPort)
        {
            if (smoothingFactor == null || adminClientPort == null)
            {
                return throughput;
            }

            var alpha = smoothingFactor.Value;

            if (_previousThroughput[adminClientPort.Value] == 0)
            {
                _previousThroughput[adminClientPort.Value] = throughput;
                return throughput;
            }

            throughput = alpha * _previousThroughput[adminClientPort.Value] + (1 - alpha) * throughput;

            _previousThroughput[adminClientPort.Value] = throughput;
            return throughput;
        }

        private async Task SendUpdatedBoundsToCollection(Collection collection, int? adminClientPort)
        {
            // Send the new bounds for the adminClient to all other nodes in the collection
            var nodes = collection.Nodes?.ToList();

            if (nodes == null)
            {
                return;
            }

            foreach (var node in nodes)
            {
                if (node.Bounds == null)
                {
                    continue;
                }

                var message = new JObject
                {
                    ["eventType"] = "updateConitEvent",
                    ["collectionName"] = collection.Name,
                    ["port"] = adminClientPort,
                    ["bounds"] = new JObject
                    {
                        ["Staleness"] = node.Bounds.Staleness ?? 1,
                        ["NumericalError"] = node.Bounds.NumericalError ?? 1,
                    }
                };

                Log.Information($" Upadted bounds to {message} for collection {collection.Name} on node {node.Port}");

                await SendMessageOverTcp(message.ToString(), node.Port ?? 0).ConfigureAwait(false);
            }
        }


        bool EvaluateCondition(string condition, double throughput, int? threshold, MovingAverage? movingAverage)
        {
            // Assuming the condition format is "{variable} {operator} {threshold}"
            var parts = condition.Split(' ');
            var operatorSymbol = parts[1];
            var variableSymbol = parts[0];

            switch (operatorSymbol)
            {
                case ">":
                    if (variableSymbol == "avg" && movingAverage != null)
                    {
                        return throughput > movingAverage.Average;
                    }
                    return throughput > threshold;
                case "<":
                    if (variableSymbol == "avg" && movingAverage != null)
                    {
                        return throughput < movingAverage.Average;
                    }
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

        private void ApplyActions(Collection collection, List<PolicyAction> actions, int? adminClientPort)
        {
            foreach (var action in actions)
            {
                var actionType = action.Type;
                double actionValue = (double)action.Value!;

                // Retrieve the bounds of the adminClientPort for the collection
                var bounds = collection.Nodes?.FirstOrDefault(n => n.Port == adminClientPort)?.Bounds;

                if (bounds == null)
                {
                    continue;
                }

                // Apply the action
                switch (actionType)
                {
                    case "add":
                        bounds.Staleness = ApplyAction(bounds.Staleness, actionValue, (x, y) => x + y);
                        bounds.NumericalError = ApplyAction(bounds.NumericalError, actionValue, (x, y) => x + y);
                        break;
                    case "subtract":
                        bounds.Staleness = ApplyAction(bounds.Staleness, actionValue, (x, y) => x - y);
                        bounds.NumericalError = ApplyAction(bounds.NumericalError, actionValue, (x, y) => x - y);
                        break;
                    case "multiply":
                        bounds.Staleness = ApplyAction(bounds.Staleness, actionValue, (x, y) => x * y);
                        bounds.NumericalError = ApplyAction(bounds.NumericalError, actionValue, (x, y) => x * y);
                        break;
                    case "divide":
                        bounds.Staleness = ApplyAction(bounds.Staleness, actionValue, (x, y) => x / y);
                        bounds.NumericalError = ApplyAction(bounds.NumericalError, actionValue, (x, y) => x / y);
                        break;
                    default:
                        break;
                }

                // Helper function to apply the action
                T ApplyAction<T>(T value, double actionValue, Func<T, double, T> operation)
                {
                    if (value != null)
                    {
                        return operation(value, actionValue);
                    }
                    return value;
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
                Log.Error("Invalid conit object.");
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

            // check if _previousThroughput has the adminClientPort
            if (!_previousThroughput.ContainsKey(adminClientPort))
            {
                _previousThroughput.Add(adminClientPort, 0);
            }
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
                foreach (var node in collection.Nodes.ToList() ?? Enumerable.Empty<Node>())
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

                        var welcomeMessage = new JObject
                        {
                            ["eventType"] = "newNodeEvent",
                            ["port"] = node.Port.Value,
                            ["collectionName"] = collectionName,
                            ["staleness"] = staleness,
                            ["numericalError"] = numericalError
                        };

                        await SendMessageOverTcp(welcomeMessage.ToString(), adminClientPort);
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
                ["port"] = _listenPort,
            };

            while (true)
            {
                await Task.Delay(10000);

                // Create a copy of the collection before iterating
                var collections = _dyconitCollections.Collections?.ToList() ?? new List<Collection>();

                foreach (var collection in collections)
                {

                    heartbeatMessage["collectionName"] = collection.Name;

                    // Create a copy of the nodes list before iterating
                    var nodes = collection.Nodes?.ToList() ?? new List<Node>();

                    foreach (var node in nodes)
                    {
                        if (node.Port != null)
                        {
                            Log.Information($"Sending heartbeat to node {node.Port}");
                            await SendMessageOverTcp(heartbeatMessage.ToString(), node.Port.Value);
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
                    var nodesToRemove = new List<Node>(); // List to store nodes to be removed

                    foreach (var node in collection.Nodes ?? new List<Node>())
                    {
                        if (node.LastHeartbeatTime.HasValue && heartbeatTime.Subtract(node.LastHeartbeatTime.Value).TotalSeconds > 30)
                        {
                            nodesToRemove.Add(node);
                        }
                    }

                    // Remove the marked nodes
                    foreach (var nodeToRemove in nodesToRemove)
                    {
                        collection.Nodes?.Remove(nodeToRemove);

                        // inform other nodes that this node has been removed
                        foreach (var otherNode in collection.Nodes!.ToList() ?? new List<Node>())
                        {
                            if (otherNode.Port != null)
                            {
                                var removeNodeMessage = new JObject
                                {
                                    ["eventType"] = "removeNodeEvent",
                                    ["port"] = nodeToRemove.Port,
                                    ["collectionName"] = collection.Name
                                };

                                await SendMessageOverTcp(removeNodeMessage.ToString(), otherNode.Port.Value);
                            }
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
                Log.Error(ex, "Error sending message over TCP");
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