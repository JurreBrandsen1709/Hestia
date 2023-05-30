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
                { "bounds", new Dictionary<string, int>() },
                { "last_time_since_pull", new Dictionary<int, List<Tuple<int, DateTime>>>() }
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

        // Initialize the last_time_since_pull for the admin client port
        var lastTimeSincePull = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Local);
        ((Dictionary<int, List<Tuple<int, DateTime>>>)dyconitCollectionData["last_time_since_pull"]).Add(adminClientPort.Value, new List<Tuple<int, DateTime>>());

        // Add your own port to the last_time_since_pull for the admin client port
        ((Dictionary<int, List<Tuple<int, DateTime>>>)dyconitCollectionData["last_time_since_pull"])[adminClientPort.Value].Add(new Tuple<int, DateTime>(adminClientPort.Value, lastTimeSincePull));

        // Add the last time since pull of the existing ports to the new admin client port
        foreach (int port in (List<int>)dyconitCollectionData["ports"])
        {
            if (port != adminClientPort.Value)
            {
                // get only the own last time since pull of the port
                var ownLastTimeSincePull = ((Dictionary<int, List<Tuple<int, DateTime>>>)dyconitCollectionData["last_time_since_pull"])[port].Find(tuple => tuple.Item1 == port).Item2;
                ((Dictionary<int, List<Tuple<int, DateTime>>>)dyconitCollectionData["last_time_since_pull"])[adminClientPort.Value].Add(new Tuple<int, DateTime>(port, ownLastTimeSincePull));
            }
        }

        Console.WriteLine($"--- last_time_since_pull for {adminClientPort}: {lastTimeSincePull.ToString("dd-MM-yyyy HH:mm:ss")}");

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

        // Update the lists of tuples for existing ports
        var ports = (List<int>)dyconitCollectionData["ports"];
        var lastTimeSincePullDict = (Dictionary<int, List<Tuple<int, DateTime>>>)dyconitCollectionData["last_time_since_pull"];
        foreach (int port in ports)
        {
            if (port != adminClientPort.Value)
            {
                lastTimeSincePullDict[port].Add(new Tuple<int, DateTime>(adminClientPort.Value, lastTimeSincePull));
            }
        }

        // Print the dyconit collections
        Console.WriteLine("--- Current dyconit collections:");
        foreach (var collection in _dyconitCollections)
        {
            Console.WriteLine($"---- Collection: {collection.Key}");
            Console.WriteLine($"---- Ports: {string.Join(", ", ((List<int>)collection.Value["ports"]))}");
            Console.WriteLine($"---- Bounds: {string.Join(", ", ((Dictionary<string, int>)collection.Value["bounds"]))}");

            Console.WriteLine($"---- last_time_since_pull: ");
            foreach (var port in ((Dictionary<int, List<Tuple<int, DateTime>>>)collection.Value["last_time_since_pull"]))
            {
                Console.WriteLine($"------ Port: {port.Key}");
                foreach (var tuple in port.Value)
                {
                    Console.WriteLine($"-------- Tuple: {tuple.Item1}, {tuple.Item2.ToString("dd-MM-yyyy HH:mm:ss.fff")}");
                }
            }

        }

        break;


            // switch (eventType)
            // {
            //     case "newAdminEvent":

            //         if (adminClientPort == null)
            //         {
            //             Console.WriteLine($"-- Invalid newAdminEvent message received: missing adminClientPort. Message: {message}");
            //             break;
            //         }
            //         Console.WriteLine($"-- Received newAdminEvent message. Admin client listening on port {adminClientPort}.");

            //         // Check if we already have a dyconit collection in the dyconitCollections dictionary


            //         // If not, create a new one
                    // if (!_dyconitCollections.ContainsKey(dyconitCollection))
                    // {
                    //     _dyconitCollections.Add(dyconitCollection, new Dictionary<string, object> {
                    //         { "ports", new List<int>() },
                    //         { "bounds", new Dictionary<string, int>() },
                    //         { "last_time_since_pull", new Dictionary<int, DateTime>() }
                    //     });
                    //     Console.WriteLine($"--- Added dyconit collection '{dyconitCollection}' to the dyconit collections.");
                    // }

            //         else
            //         {
            //             Console.WriteLine($"--- Dyconit collection '{dyconitCollection}' already exists.");
            //         }

            //         // Add the admin client port to the dyconit collection
            //         ((List<int>)_dyconitCollections[dyconitCollection]["ports"]).Add(adminClientPort.Value);

            //         Console.WriteLine($"--- Added new admin client listening on port '{adminClientPort}' to dyconit collection '{dyconitCollection}'.");

            //         // Initialize the last_time_since_pull for the admin client port
            //         var lastTimeSincePull = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Local);
            //         ((Dictionary<int, DateTime>)_dyconitCollections[dyconitCollection]["last_time_since_pull"]).Add(adminClientPort.Value, lastTimeSincePull);

            //         Console.WriteLine($"--- last_time_since_pull for {adminClientPort}: {lastTimeSincePull.ToString("dd-MM-yyyy HH:mm:ss")}");

            //         // Add the bounds to the dyconit collection
            //         var conits = json["conits"];

            //         // Check if bounds is still empty
            //         if (((Dictionary<string, int>)_dyconitCollections[dyconitCollection]["bounds"]).Count == 0)
            //         {
            //             var staleness = conits["Staleness"]?.ToObject<int>();
            //             var orderError = conits["OrderError"]?.ToObject<int>();
            //             var numericalError = conits["NumericalError"]?.ToObject<int>();

            //             ((Dictionary<string, int>)_dyconitCollections[dyconitCollection]["bounds"]).Add("Staleness", staleness.Value);
            //             ((Dictionary<string, int>)_dyconitCollections[dyconitCollection]["bounds"]).Add("OrderError", orderError.Value);
            //             ((Dictionary<string, int>)_dyconitCollections[dyconitCollection]["bounds"]).Add("NumericalError", numericalError.Value);

            //             Console.WriteLine($"--- Added bounds to dyconit collection '{dyconitCollection}'.");
            //         }
            //         else
            //         {
            //             Console.WriteLine($"--- Bounds for dyconit collection '{dyconitCollection}' already exist.");
            //         }
            //         Console.WriteLine($"--- Bounds: {string.Join(", ", ((Dictionary<string, int>)_dyconitCollections[dyconitCollection]["bounds"]))}");

            //         // Print the dyconit collections
            //         Console.WriteLine("--- Current dyconit collections:");
            //         foreach (var collection in _dyconitCollections)
            //         {
            //             Console.WriteLine($"---- Collection: {collection.Key}");
            //             Console.WriteLine($"---- Ports: {string.Join(", ", ((List<int>)collection.Value["ports"]))}");
            //             Console.WriteLine($"---- Bounds: {string.Join(", ", ((Dictionary<string, int>)collection.Value["bounds"]))}");
            //             Console.WriteLine($"---- last_time_since_pull: {string.Join(", ", ((Dictionary<int, DateTime>)collection.Value["last_time_since_pull"]))}");
            //         }

            //         break;

                case "checkStalenessEvent":

    var shouldPull = false;

    Console.WriteLine($"-- Received checkStalenessEvent message from {adminClientPort}.");

    // Update the last_time_since_pull for the admin client port
    var consumedTime = json["consumedTime"].ToObject<DateTime>();

    // Get the staleness bound for the collection
    var stalenessBound = ((Dictionary<string, int>)_dyconitCollections[dyconitCollection]["bounds"])["Staleness"];

    foreach (var portEntry in ((Dictionary<int, List<Tuple<int, DateTime>>>)_dyconitCollections[dyconitCollection]["last_time_since_pull"]))
    {
        var port = portEntry.Key;
        var tuplesList = portEntry.Value;

        if (port != adminClientPort)
        {
            Console.WriteLine($"--- Port: {port}, last_time_since_pull: {tuplesList.Last().Item2.ToString("dd-MM-yyyy HH:mm:ss.fff")}");

            var ltsp = tuplesList.Last().Item2;
            var timeDifference = consumedTime - ltsp;

            if (timeDifference.TotalMilliseconds > stalenessBound)
            {
                // Staleness violation, perform necessary actions
                Console.WriteLine($"---- Staleness violation for admin client port {port}. Time difference: {timeDifference.TotalMilliseconds} milliseconds.");

                shouldPull = true;

                // Simulate actions
                Console.WriteLine($"----- Starting synchronization with port {port}...");
                Thread.Sleep(1000);
                Console.WriteLine($"----- Synchronization completed with port {port}.");

                // Send a message to the admin client
                var stalenessCompletedMessage = new JObject
                {
                    { "eventType", "completedStalenessEvent" },
                    { "data", "xxx" },
                }.ToString();

                SendMessageOverTcp(stalenessCompletedMessage, adminClientPort.Value);
            }
        }
    }

if (shouldPull)
{
    if (adminClientPort.HasValue)
    {
        var ltspd = (Dictionary<int, List<Tuple<int, DateTime>>>)_dyconitCollections[dyconitCollection]["last_time_since_pull"];

        if (ltspd.ContainsKey(adminClientPort.Value))
        {
            var tuplesList = ltspd[adminClientPort.Value];
            var lastTimePulled = DateTime.Now;

            foreach (var portEntry in ltspd)
            {
                var port = portEntry.Key;
                if (port != adminClientPort.Value)
                {
                    var portTuplesList = portEntry.Value;
                    var tupleIndex = portTuplesList.FindIndex(t => t.Item1 == adminClientPort.Value);

                    portTuplesList[tupleIndex] = new Tuple<int, DateTime>(adminClientPort.Value, lastTimePulled);
                    // tuplesList[tupleIndex] = new Tuple<int, DateTime>(port, lastTimePulled);
                }
            }
        }

        var adminPortTuplesList = ltspd[adminClientPort.Value];
        var adminTupleIndex = adminPortTuplesList.FindIndex(t => t.Item1 == adminClientPort.Value);
        adminPortTuplesList[adminTupleIndex] = new Tuple<int, DateTime>(adminClientPort.Value, DateTime.Now);

    }

    // Console.WriteLine($"--- Updated last_time_since_pull for {adminClientPort}: {((Dictionary<int, List<Tuple<int, DateTime>>>)_dyconitCollections[dyconitCollection]["last_time_since_pull"])[adminClientPort.Value].Last().Item2.ToString("dd-MM-yyyy HH:mm:ss")}");
    // Print the dyconit collections
        Console.WriteLine("--- Current dyconit collections:");
        foreach (var collection in _dyconitCollections)
        {
            // Console.WriteLine($"---- Collection: {collection.Key}");
            // Console.WriteLine($"---- Ports: {string.Join(", ", ((List<int>)collection.Value["ports"]))}");
            // Console.WriteLine($"---- Bounds: {string.Join(", ", ((Dictionary<string, int>)collection.Value["bounds"]))}");

            Console.WriteLine($"---- last_time_since_pull: ");
            foreach (var port in ((Dictionary<int, List<Tuple<int, DateTime>>>)collection.Value["last_time_since_pull"]))
            {
                Console.WriteLine($"------ Port: {port.Key}");
                foreach (var tuple in port.Value)
                {
                    Console.WriteLine($"-------- Tuple: {tuple.Item1}, {tuple.Item2.ToString("dd-MM-yyyy HH:mm:ss.fff")}");
                }
            }

        }
}
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
    }
}
