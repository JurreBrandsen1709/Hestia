using Confluent.Kafka;
using Dyconit.Overlord;
using Newtonsoft.Json.Linq;
using System;
using System.IO;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;

namespace Dyconit.Producer
{
    public class DyconitProducerBuilder<TKey, TValue> : ProducerBuilder<TKey, TValue>
    {
        private Action<string, double>? _statisticsHandler;
        private readonly int _type;
        private readonly DyconitAdmin _adminClient;
        private readonly Dictionary<string, object> _conits;
        private readonly int _adminPort;

        public DyconitProducerBuilder(ClientConfig config, Dictionary<string, object> Conits, int type, int adminPort) : base(config)
        {
            _type = type;
            _adminPort = adminPort;
            _conits = Conits;
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
    }
}