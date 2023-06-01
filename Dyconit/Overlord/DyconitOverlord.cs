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

        private readonly Dictionary<string, Dictionary<string, object>> _dyconitCollections;

        public DyconitOverlord()
        {
            _dyconitCollections = new Dictionary<string, Dictionary<string, object>>();
            Console.WriteLine("- Dyconit overlord started.");
            Task.Run(ListenForMessagesAsync);
        }

        private async Task ListenForMessagesAsync()
        {
            var listener = new TcpListener(IPAddress.Any, _listenPort);
            listener.Start();

            while (true)
            {
                var client = await listener.AcceptTcpClientAsync().ConfigureAwait(false);
                var reader = new StreamReader(client.GetStream());
                var message = await reader.ReadToEndAsync().ConfigureAwait(false);

                // Parse message and act accordingly
                ParseMessage(message);


                reader.Close();
                client.Close();
            }
        }

        private void ParseMessage(string message)
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
                            { "ports", new List<int>() },
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
                    ((List<int>)dyconitCollectionData["ports"]).Add(adminClientPort.Value);

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
                        Console.WriteLine($"---- Ports: {string.Join(", ", ((List<int>)collection.Value["ports"]))}");
                        Console.WriteLine($"---- Bounds: {string.Join(", ", ((Dictionary<string, int>)collection.Value["bounds"]))}");
                    }

                    // If there are more than one admin clients in the dyconit collection, send the bounds to the other admin clients
                    if (((List<int>)dyconitCollectionData["ports"]).Count > 1)
                    {
                        Console.WriteLine($"--- Sending newNodeEvent to other admin clients in dyconit collection '{dyconitCollection}'.");

                        // Create a new message
                        var newMessage = new JObject
                        {
                            { "eventType", "newNodeEvent" },
                            { "port", adminClientPort }
                        };

                        // Send the message to the other admin clients
                        foreach (var port in ((List<int>)dyconitCollectionData["ports"]))
                        {
                            if (port != adminClientPort)
                            {
                                SendMessageOverTcp(newMessage.ToString(), port);
                                // Send the existing ports to the new admin client
                                newMessage = new JObject
                                {
                                    { "eventType", "newNodeEvent" },
                                    { "port", port }
                                };
                                SendMessageOverTcp(newMessage.ToString(), adminClientPort.Value);
                            }
                        }

                    }

                    break;

                default:
                    Console.WriteLine($"Unknown message received with eventType '{eventType}': {message}");
                    break;
            }
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
