using Confluent.Kafka;
using Confluent.Kafka.Admin;
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
        private List<string> _receivedData;
        private List<string> _localData;
        private TaskCompletionSource<bool> _stalenessEventReceived;
        private readonly Dictionary<string, object> _localCollection;
        private bool _optimisticMode = true;
        private HashSet<int> _syncResponses = new HashSet<int>();

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
                BootstrapServers = bootstrapServers
            };
            _adminClient = new AdminClientBuilder(_adminClientConfig).Build();

            ListenForMessagesAsync();
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
                var json = JObject.Parse(message);

                Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Received message: {message}");

                var eventType = json["eventType"]?.ToString();
                if (eventType == null)
                {
                    Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Invalid message received: missing eventType. Message: {message}");
                    return;
                }

                switch (eventType)
                {
                    case "syncResponse":
                        var data = json["data"];
                        var timestamp = json["timestamp"];
                        var senderPort = Convert.ToInt32(json["port"]);

                        Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Received syncResponse from port {senderPort}");

                        if (data.Type == JTokenType.String)
                        {
                            _receivedData = new List<string> { data.ToString() };
                        }
                        else if (data.Type == JTokenType.Array)
                        {
                            _receivedData = data.ToObject<List<string>>();
                        }
                        else
                        {
                            Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Invalid 'data' format for completedStalenessEvent: {data}");
                            return;
                        }

                        // Combine each data item in the received data with the timestamp
                        for (var i = 0; i < _receivedData.Count; i++)
                        {
                            _receivedData[i] = $"{_receivedData[i]}|{timestamp}";
                        }

                        // Call the method to process the completed staleness event
                        UpdateLocalData(_localData);

                        Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Updated local data with completedStalenessEvent from port {senderPort}");

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

                        var removeNodePort = Convert.ToInt32(json["port"]);

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

                        // Send a sync response to the requesting node
                        var syncResponse = new JObject
                        {
                            { "eventType", "syncResponse" },
                            { "port", _listenPort },
                            { "data", JArray.FromObject(_localData) }
                        };

                        SendMessageOverTcp(syncResponse.ToString(), syncRequestPort);

                        break;

                    case "heartbeatEvent":
                        var heartbeatPort = Convert.ToInt32(json["port"]);
                        Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Received heartbeat from port {heartbeatPort}");

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
            });
        }

        private void UpdateLocalData(List<string> localData)
        {
            if (_receivedData != null && _receivedData.Any())
            {
                var combinedData = _receivedData.Concat(_localData).Distinct().ToList(); // distinct is nog een probleem.
                _localData = combinedData;
            }
            else
            {
                _localData = localData;
            }
        }

        public async Task<List<string>> BoundStaleness(DateTime consumedTime, List<string> localData)
        {

            // Check if we have received new data from an other node since the last time we checked
            UpdateLocalData(localData);

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
                return _localData;
            }

            if (_optimisticMode)
            {
                // Call the async mode that will send a sync request to the ports that exceeded the staleness bound
                await WaitForResponse(portsStalenessExceeded);

                return _localData;
            }

            WaitForResponse(portsStalenessExceeded);

            // Update the local data
            UpdateLocalData(localData);

            // Return the local data
            return _localData;
        }

        private async Task WaitForResponse(List<int> portsStalenessExceeded)
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

        public void BoundNumericalError(int numericalError)
        {
            // send message to dyconit overlord with numericalError
        }

        public void BoundOrderError(int orderError)
        {
            // send message to dyconit overlord with orderError
        }
    }
}
