using Confluent.Kafka;
using Dyconit.Overlord;
using Newtonsoft.Json;
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
        private readonly int _type;
        private readonly JToken _conits;
        private readonly int _adminPort;

        public DyconitConsumerBuilder(ClientConfig config, JToken Conits, int type, int adminPort) : base(config)
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
                var message = new JObject
                {
                    new JProperty("eventType", "newAdminEvent"),
                    new JProperty( "type", messageType),
                    new JProperty("adminPort", _adminPort),
                    new JProperty("conits", _conits)
                };

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
                        writer.WriteLine(message.ToString());
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
