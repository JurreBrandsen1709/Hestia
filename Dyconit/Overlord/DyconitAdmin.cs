// todo check the health of the client and update this to the overlord if needed.

using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Newtonsoft.Json.Linq;
using System;
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

        // 1: consumer, 2: producer, 3: both
        private readonly int _type;

        private readonly int _listenPort;
        private DateTime _lastCheckTime;
        private TimeSpan _CheckInterval;

        private DateTime? lastConsumedTime;
        private readonly Dictionary<string, object> _conit;
        private readonly string _collection;
        private readonly int _staleness;
        private readonly int _orderError;
        private readonly int _numericalError;

        public DyconitAdmin(string bootstrapServers, int type, int listenPort, Dictionary<string, object> conit)
        {
            _type = type;
            _listenPort = listenPort;
            _conit = conit;
            _collection = conit.ContainsKey("collection") ? conit["collection"].ToString() : null;
            _staleness = conit.ContainsKey("Staleness") ? Convert.ToInt32(conit["Staleness"]) : 0;
            _orderError = conit.ContainsKey("OrderError") ? Convert.ToInt32(conit["OrderError"]) : 0;
            _numericalError = conit.ContainsKey("NumericalError") ? Convert.ToInt32(conit["NumericalError"]) : 0;


            _adminClientConfig = new AdminClientConfig
            {
                BootstrapServers = bootstrapServers
            };
            _adminClient = new AdminClientBuilder(_adminClientConfig).Build();
            Task.Run(ListenForMessagesAsync);
        }


        private int getThroughputThreshold()
        {
            return 0;
        }

        private int setThroughputThreshold()
        {
            return 0;
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
                await ParseMessageAsync(message);

                reader.Close();
                client.Close();
            }
        }

        private async Task ParseMessageAsync(string message)
        {
            var json = JObject.Parse(message);
            var eventType = json["eventType"]?.ToString();
            if (eventType == null)
            {
                Console.WriteLine($"Invalid message received: missing eventType. Message: {message}");
                return;
            }

            switch (eventType)
            {
                case "completedStalenessEvent":

                // Simulate processing data received from dyconit overlord.
                Random rnd = new Random();
                int waitTime = rnd.Next(0, 3000);
                await Task.Delay(waitTime);

                Console.WriteLine($"[{_listenPort}] - Completed processing new data from other actors. Processing took {waitTime} ms.");

                break;

                default:
                    Console.WriteLine($"Unknown message received with eventType '{eventType}': {message}");
                break;
            }
        }

        // We receive the statistics. We calculate the throughput.
        // We apply what the policy says.
        // We update the bounds of every Dyconit that is affected.
        // We possibly have to modify the configuration of the consumer.
        // public void ProcessStatistics(string json, ClientConfig config)
        // {
        //     var stats = JObject.Parse(json);
        //     var s1 = stats["brokers"][$"{config.BootstrapServers}/1"]["rtt"]["avg"];

        //     Console.WriteLine($"Consumer RTT: {s1}");

        //     // TODO: Calculate throughput

        //     // TODO: Apply policy

        //     // TODO: Update bounds

        //     // TODO: Adjust configuration
        // }

        public async Task BoundStaleness(DateTime consumedTime)
        {
            // Create a JSON message containing the EventType as checkStalenessEvent and the consumedTime as the consumedTime (from parameter)

            var message = new Dictionary<string, object>
            {
                { "eventType", "checkStalenessEvent" },
                { "consumedTime", consumedTime },
                { "adminClientPort", _listenPort },
                { "conits", _conit }
            };

            // Serialize message to JSON
            var json = JObject.FromObject(message).ToString();

            await Task.Run(() => Console.WriteLine($"[{_listenPort}] - Sending checkStalenessEvent to Dyconit Overlord."));

            // Console.WriteLine($"[{_listenPort}] - Sending checkStalenessEvent to Dyconit Overlord.");

            // send message to dyconit overlord.
            SendMessageOverTcp(json, 6666);
        }

        private async Task SendMessageOverTcp(string message, int port)
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
                        // writer.WriteLine(message);
                        await Task.Run(() => writer.WriteLine(message));

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