// todo there must be a way to let the dyconits subscribe to each other so they can send messages.
// todo create a collection of dyconits where we know what these bounds are and which port the admin clients are listening on.
// todo We must know whether it is a producer or consumer or both.

using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Newtonsoft.Json.Linq;
using System;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace Dyconit.Overlord
{
    public class DyconitOverlord
    {
        private readonly int _listenPort = 6666;

        private Dictionary<string, Dictionary<string, object>> _dyconitCollections;

        public DyconitOverlord()
        {
            _dyconitCollections = new Dictionary<string, Dictionary<string, object>>();
            Console.WriteLine("- Dyconit overlord started.");
            ListenForMessagesAsync();
            SendHeartbeatAsync();
            KeepTrackOfNodesAsync();
        }

        private async void SendHeartbeatAsync()
        {
            // Send a heartbeat response to the requesting node
            var heartbeatEvent = new JObject
            {
                { "eventType", "heartbeatEvent" },
                { "port", _listenPort}
            };


            while (true)
            {
                await Task.Delay(1000).ConfigureAwait(false);

                // Send heartbeat to all admin clients
                foreach (var dyconitCollection in _dyconitCollections.ToList())
                {
                    var dyconitCollectionData = dyconitCollection.Value;
                    var adminClientPorts = (List<Tuple<int, DateTime>>)dyconitCollectionData["ports"];

                    foreach (var adminClientPort in adminClientPorts.ToList())
                    {
                        // send heartbeat to this node
                        SendMessageOverTcp(heartbeatEvent.ToString(), adminClientPort.Item1);
                    }
                }
            }
        }

        // Every 5 seconds, check if we have received a heartbeat from all nodes
        private async void KeepTrackOfNodesAsync()
        {
            while (true)
            {
                await Task.Delay(5000).ConfigureAwait(false);

                // Check if we have received a heartbeat from all nodes
                foreach (var dyconitCollection in _dyconitCollections.ToList())
                {
                    var dyconitCollectionData = dyconitCollection.Value;
                    var adminClientPorts = (List<Tuple<int, DateTime>>)dyconitCollectionData["ports"];

                    foreach (var adminClientPort in adminClientPorts.ToList())
                    {

                        Console.WriteLine($" --- {adminClientPort.Item1} - {adminClientPort.Item2.AddSeconds(10)}");
                        // check if we have received a heartbeat from this node
                        // if not, remove it from the list
                        if (adminClientPort.Item2.AddSeconds(10) < DateTime.Now)
                        {
                            adminClientPorts.Remove(adminClientPort);

                            // notify all other nodes that this node has been removed
                            await RemoveNodeAtAdmins(adminClientPort.Item1);

                            Console.WriteLine($"- Removed node {adminClientPort.Item1} from collection {dyconitCollection.Key}");
                        }
                    }
                }
            }
        }

        private async Task RemoveNodeAtAdmins(int adminClientPort)
        {
            // Send a heartbeat response to the requesting node
            var removeNodeEvent = new JObject
            {
                { "eventType", "removeNodeEvent" },
                { "adminClientPort", adminClientPort }
            };

            // Send heartbeat to all admin clients
            foreach (var dyconitCollectionData in _dyconitCollections.ToList())
            {
                var adminClientPorts = (List<Tuple<int, DateTime>>)dyconitCollectionData.Value["ports"];

                foreach (var otherAdminClientPort in adminClientPorts.ToList())
                {
                    Console.WriteLine($"--- Sending removeNodeEvent to {otherAdminClientPort.Item1}");
                    // send heartbeat to this node
                    await SendMessageOverTcp(removeNodeEvent.ToString(), otherAdminClientPort.Item1);
                }
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
            // Check if message is a newAdminEvent
            var json = JObject.Parse(message);
            var adminClientPort = json["adminClientPort"]?.ToObject<int>();
            var dyconitCollection = json["conits"]?["collection"]?.ToString();

            var eventType = json["eventType"]?.ToString();
            if (eventType == null)
            {
                Console.WriteLine($"Invalid message received: missing eventType. Message: {message}");
                return;
            }
            switch (eventType)
            {
                case "newAdminEvent":

                    if (adminClientPort == null)
                    {
                        Console.WriteLine($"-- Invalid newAdminEvent message received: missing adminClientPort. Message: {message}");
                        break;
                    }
                    Console.WriteLine($"-- Received newAdminEvent message. Admin client listening on port {adminClientPort}.");

                    // Check if we already have a dyconit collection in the dyconitCollections dictionary
                    // If not, create a new one
                    if (!_dyconitCollections.ContainsKey(dyconitCollection))
                    {
                        _dyconitCollections.Add(dyconitCollection, new Dictionary<string, object>
                        {
                            // add a ports list containing the port and the last time we got a heartbeat from it
                            { "ports", new List<Tuple<int, DateTime>>() },
                            { "bounds", new Dictionary<string, int>() }
                        });
                        Console.WriteLine($"--- Added dyconit collection '{dyconitCollection}' to the dyconit collections.");
                    }
                    else
                    {
                        Console.WriteLine($"--- Dyconit collection '{dyconitCollection}' already exists.");
                    }

                    var dyconitCollectionData = (Dictionary<string, object>)_dyconitCollections[dyconitCollection];

                    // Add the admin client port to the dyconit collection
                    ((List<Tuple<int, DateTime>>)dyconitCollectionData["ports"]).Add(new Tuple<int, DateTime>(adminClientPort.Value, DateTime.Now));

                    Console.WriteLine($"--- Added new admin client listening on port '{adminClientPort}' to dyconit collection '{dyconitCollection}'.");

                    // Add the bounds to the dyconit collection
                    var conits = json["conits"];

                    // Check if bounds is still empty
                    if (((Dictionary<string, int>)dyconitCollectionData["bounds"]).Count == 0)
                    {
                        var staleness = conits["Staleness"]?.ToObject<int>();
                        var orderError = conits["OrderError"]?.ToObject<int>();
                        var numericalError = conits["NumericalError"]?.ToObject<int>();

                        ((Dictionary<string, int>)dyconitCollectionData["bounds"]).Add("Staleness", staleness.Value);
                        ((Dictionary<string, int>)dyconitCollectionData["bounds"]).Add("OrderError", orderError.Value);
                        ((Dictionary<string, int>)dyconitCollectionData["bounds"]).Add("NumericalError", numericalError.Value);

                        Console.WriteLine($"--- Added bounds to dyconit collection '{dyconitCollection}'.");
                    }
                    else
                    {
                        Console.WriteLine($"--- Bounds for dyconit collection '{dyconitCollection}' already exist.");
                    }
                    Console.WriteLine($"--- Bounds: {string.Join(", ", ((Dictionary<string, int>)dyconitCollectionData["bounds"]))}");


                    // Print the dyconit collections
                    Console.WriteLine("--- Current dyconit collections:");
                    foreach (var collection in _dyconitCollections.ToList())
                    {
                        Console.WriteLine($"---- Collection: {collection.Key}");
                        Console.WriteLine($"---- Ports and heartbeat time: {string.Join(", ", ((List<Tuple<int, DateTime>>)collection.Value["ports"]))}");
                        Console.WriteLine($"---- Bounds: {string.Join(", ", ((Dictionary<string, int>)collection.Value["bounds"]))}");
                    }

                    // If there are more than one admin clients in the dyconit collection, send the bounds to the other admin clients
                    if (((List<Tuple<int, DateTime>>)dyconitCollectionData["ports"]).Count > 1)
                    {
                        Console.WriteLine($"--- Sending newNodeEvent to other admin clients in dyconit collection '{dyconitCollection}'.");

                        // Create a new message
                        var newMessage = new JObject
                        {
                            { "eventType", "newNodeEvent" },
                            { "port", adminClientPort }
                        };

                        // Send the message to the other admin clients
                        foreach (var port in ((List<Tuple<int, DateTime>>)dyconitCollectionData["ports"]).ToList())
                        {
                            if (port.Item1 != adminClientPort)
                            {
                                SendMessageOverTcp(newMessage.ToString(), port.Item1);
                                // Send the existing ports to the new admin client
                                newMessage = new JObject
                                {
                                    { "eventType", "newNodeEvent" },
                                    { "port", port.Item1 }
                                };
                                SendMessageOverTcp(newMessage.ToString(), adminClientPort.Value);
                            }
                        }
                    }

                    break;
                case "heartbeatResponse":
                    // update the heartbeat time of the admin client
                    var heartbeatTime = DateTime.Now;
                    foreach (var collection in _dyconitCollections.ToList())
                    {
                        foreach (var port in ((List<Tuple<int, DateTime>>)collection.Value["ports"]).ToList())
                        {
                            if (port.Item1 == adminClientPort)
                            {
                                // remove the tuple and add a new one with the new heartbeat time
                                ((List<Tuple<int, DateTime>>)collection.Value["ports"]).Remove(port);
                                ((List<Tuple<int, DateTime>>)collection.Value["ports"]).Add(new Tuple<int, DateTime>(adminClientPort.Value, heartbeatTime));
                            }
                        }
                    }

                    break;

                case "throughput":

                    var throughput = json["throughput"]?.ToObject<double>();
                    var adminPort = json["adminPort"]?.ToObject<int>();

                    Console.WriteLine($"-- Received throughput message from admin client listening on port {adminPort}, Throughput: {throughput}.");


                    break;

                default:
                    Console.WriteLine($"Unknown message received with eventType '{eventType}': {message}");
                    break;
            }
        });
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
                Console.WriteLine($"Failed to send message over TCP: {ex.Message}");

                // Remove the admin client from the dyconit collections
                foreach (var collection in _dyconitCollections.ToList())
                {
                    foreach (var adminClient in ((List<Tuple<int, DateTime>>)collection.Value["ports"]).ToList())
                    {
                        if (adminClient.Item1 == port)
                        {


                            ((List<Tuple<int, DateTime>>)collection.Value["ports"]).Remove(adminClient);
                            // notify all other nodes that this node has been removed
                            await RemoveNodeAtAdmins(port);

                            Console.WriteLine($"--- Removed admin client listening on port '{port}' from dyconit collection '{collection.Key}'.");
                        }
                    }
                }
            }
        }}}
