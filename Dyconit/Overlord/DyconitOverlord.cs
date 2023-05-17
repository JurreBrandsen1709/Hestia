// todo there must be a way to let the dyconits subscribe to each other so they can send messages.
// todo create a collection of dyconits where we know what these bounds are and which port the admin clients are listening on.

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
            Console.WriteLine("Dyconit overlord started.");
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
            }
        }

        private void ParseMessage(string message)
        {
            // Check if message is a newAdminEvent
            var json = JObject.Parse(message);
            var eventType = json["eventType"]?.ToString();
            if (eventType == null)
            {
                Console.WriteLine($"Invalid message received: missing eventType. Message: {message}");
                return;
            }

            switch (eventType)
            {
                case "newAdminEvent":
                    var adminClientPort = json["adminClientPort"]?.ToObject<int>();
                    if (adminClientPort == null)
                    {
                        Console.WriteLine($"Invalid newAdminEvent message received: missing adminClientPort. Message: {message}");
                        break;
                    }
                    Console.WriteLine($"Received newAdminEvent message. Admin client listening on port {adminClientPort}.");

                    // Check if we already have a dyconit collection in the dyconitCollections dictionary
                    var dyconitCollection = json["conits"]["collection"].ToString();

                    // If not, create a new one
                    if (!_dyconitCollections.ContainsKey(dyconitCollection))
                    {
                        _dyconitCollections.Add(dyconitCollection, new Dictionary<string, object> {
                            { "ports", new List<int>() },
                            { "bounds", new Dictionary<string, int>() }
                        });
                    }

                    // Add the admin client port to the dyconit collection
                    ((List<int>)_dyconitCollections[dyconitCollection]["ports"]).Add(adminClientPort.Value);

                    // Add the bounds to the dyconit collection
                    var conits = json["conits"];
                    if (conits != null)
                    {
                        var staleness = conits["Staleness"]?.ToObject<int>();
                        var orderError = conits["OrderError"]?.ToObject<int>();
                        var numericalError = conits["NumericalError"]?.ToObject<int>();

                        ((Dictionary<string, int>)_dyconitCollections[dyconitCollection]["bounds"]).Add("Staleness", staleness.Value);
                        ((Dictionary<string, int>)_dyconitCollections[dyconitCollection]["bounds"]).Add("OrderError", orderError.Value);
                        ((Dictionary<string, int>)_dyconitCollections[dyconitCollection]["bounds"]).Add("NumericalError", numericalError.Value);

                    }

                    // print the debug message
                    Console.WriteLine($"Added admin client listening on port {adminClientPort} to dyconit collection {dyconitCollection}.");

                    // Print the dyconit collections
                    Console.WriteLine("Current dyconit collections:");
                    foreach (var collection in _dyconitCollections)
                    {
                        Console.WriteLine($"Collection: {collection.Key}");
                        Console.WriteLine($"Ports: {string.Join(", ", ((List<int>)collection.Value["ports"]))}");
                        Console.WriteLine($"Bounds: {string.Join(", ", ((Dictionary<string, int>)collection.Value["bounds"]))}");
                    }

                    break;

                case "dataEvent":
                    var data = json["data"];
                    if (data == null)
                    {
                        Console.WriteLine($"Invalid dataEvent message received: missing data field. Message: {message}");
                        break;
                    }
                    Console.WriteLine($"Received dataEvent message with data: {data}.");
                    // Todo: handle data
                    break;

                default:
                    Console.WriteLine($"Unknown message received with eventType '{eventType}': {message}");
                    break;
            }
        }

        // todo add ports that must receive this message.
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

        // public async Task<int> GetPartitionCount(string topicName)
        // {
        //     using var adminClient = new AdminClientBuilder(_adminClientConfig).Build();

        //     var metadata = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(10));

        //     return metadata.Topics[0].Partitions.Count;
        // }

        // public async Task<int> AddPartitions(string topicName, int partitionCount)
        // {
        //     using var adminClient = new AdminClientBuilder(_adminClientConfig).Build();

        //     var partitionSpecification = new PartitionsSpecification
        //     {
        //         Topic = topicName,
        //         IncreaseTo = partitionCount
        //     };

        //     await adminClient.CreatePartitionsAsync(new[] { partitionSpecification });

        //     var metadata = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(10));

        //     return metadata.Topics[0].Partitions.Count;
        // }
    }
}
