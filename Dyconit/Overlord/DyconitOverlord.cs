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
                { "eventType", "heartbeatEvent" }
            };


            while (true)
            {
                await Task.Delay(1000).ConfigureAwait(false);

                // Send heartbeat to all admin clients
                foreach (var dyconitCollection in _dyconitCollections)
                {
                    var dyconitCollectionData = dyconitCollection.Value;
                    var adminClientPorts = (List<Tuple<int, DateTime>>)dyconitCollectionData["ports"];


                    foreach (var adminClientPort in adminClientPorts)
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
                foreach (var dyconitCollection in _dyconitCollections)
                {
                    var dyconitCollectionData = dyconitCollection.Value;
                    var adminClientPorts = (List<Tuple<int, DateTime>>)dyconitCollectionData["ports"];

                    foreach (var adminClientPort in adminClientPorts)
                    {
                        // check if we have received a heartbeat from this node
                        // if not, remove it from the list
                        if (adminClientPort.Item2.AddSeconds(10) < DateTime.Now)
                        {
                            adminClientPorts.Remove(adminClientPort);
                            Console.WriteLine($"- Removed node {adminClientPort.Item1} from collection {dyconitCollection.Key}");
                        }
                    }
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
            var dyconitCollection = json["conits"]["collection"].ToString();

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
                    foreach (var collection in _dyconitCollections)
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
                        foreach (var port in ((List<Tuple<int, DateTime>>)dyconitCollectionData["ports"]))
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

                    Console.WriteLine($"-- Received heartbeatResponse message. Message: {message}");

                    // update the heartbeat time of the admin client
                    var heartbeatTime = DateTime.Now;
                    foreach (var collection in _dyconitCollections)
                    {
                        foreach (var port in ((List<Tuple<int, DateTime>>)collection.Value["ports"]))
                        {
                            if (port.Item1 == adminClientPort)
                            {
                                // remove the tuple and add a new one with the new heartbeat time
                                ((List<Tuple<int, DateTime>>)collection.Value["ports"]).Remove(port);
                                ((List<Tuple<int, DateTime>>)collection.Value["ports"]).Add(new Tuple<int, DateTime>(adminClientPort.Value, heartbeatTime));
                                Console.WriteLine($"--- Updated heartbeat time of admin client listening on port '{adminClientPort}' in dyconit collection '{collection.Key}'.");
                            }
                        }
                    }

                    break;

                default:
                    Console.WriteLine($"Unknown message received with eventType '{eventType}': {message}");
                    break;
            }
        });
        }

        private void SendMessageOverTcp(string message, int port)
        {
            try
            {
                // Create a TCP client and connect to the server
                using (var client = new TcpClient())
                {
                    client.Connect("localhost", port);

                    // Get a stream object for reading and writing
                    using (var stream = client.GetStream())
                    using (var writer = new StreamWriter(stream))
                    {
                        // Write a message to the server
                        writer.WriteLine(message);

                        // Flush the stream to ensure that the message is sent immediately
                        writer.Flush();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to send message over TCP: {ex.Message}");
            }
        }
    }
}
