using Confluent.Kafka;
using Dyconit.Overlord;
using Newtonsoft.Json.Linq;
using System;
using System.IO;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;

namespace Dyconit.Consumer
{
    public class DyconitConsumerBuilder<TKey, TValue> : ConsumerBuilder<TKey, TValue>
    {
        private Action<string, double>? _statisticsHandler;
        private readonly int _type;
        private readonly DyconitAdmin _adminClient;
        private readonly Dictionary<string, object> _conits;

        private readonly int _adminPort;

        public DyconitConsumerBuilder(ClientConfig config, Dictionary<string, object> Conits, int type) : base(config)
        {
            _type = type;
            _adminPort = FindPort();
            _adminClient = new DyconitAdmin(config.BootstrapServers, type, _adminPort);
            _conits = Conits;

            SetStatisticsHandler((_, json) =>
            {
                _adminClient.ProcessStatistics(json, config);
            });

            // Send message to overlord, notifying the following:
            // - I am a producer
            // - my admin client is listening on port 1337
            // - my Conit bounds are [0,1,2]
            SendMessageToOverlord();

        }

        private void SendMessageToOverlord()
        {
            try
            {
                // Determine message type based on type parameter
                var messageType = "consumer";
                if (_type == 2)
                {
                    messageType = "producer";
                }

                // Create message dictionary with updated values
                var message = new Dictionary<string, object>
                {
                    {"eventType", "newAdminEvent"},
                    { "type", messageType },
                    { "adminClientPort", _adminPort },
                    { "conits", _conits }
                };

                // Serialize message to JSON
                var json = JObject.FromObject(message).ToString();

                // Create a TCP client and connect to the server
                using (var client = new TcpClient())
                {
                    client.Connect("localhost", 6666);

                    // Get a stream object for reading and writing
                    using (var stream = client.GetStream())
                    using (var writer = new StreamWriter(stream))
                    using (var reader = new StreamReader(stream))
                    {
                        // Write a message to the server
                        writer.WriteLine(json);
                        writer.Flush();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to send message over TCP: {ex.Message}");
            }
        }

        private int FindPort()
        {
            var random = new Random();
            int adminClientPort;
            while (true)
            {
                adminClientPort = random.Next(5000, 10000);
                var isPortInUse = IPGlobalProperties.GetIPGlobalProperties()
                    .GetActiveTcpListeners()
                    .Any(x => x.Port == adminClientPort);
                if (!isPortInUse)
                {
                    break;
                }
            }
            return adminClientPort;
        }

        public DyconitConsumerBuilder<TKey, TValue> SetStatisticsHandler(Action<string, double> handler)
        {
            _statisticsHandler = handler;
            return this;
        }
    }
}
