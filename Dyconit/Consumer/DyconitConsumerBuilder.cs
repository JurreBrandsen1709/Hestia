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
        private readonly string _host;

        public DyconitConsumerBuilder(ClientConfig config, JToken Conits, int type, int adminPort, string host) : base(config)
        {
            _type = type;
            _adminPort = adminPort;
            _host = host;
            _conits = Conits;
            SendMessageToOverlord();

        }
        private void SendMessageToOverlord()
        {
            try
            {
                // Create message dictionary with updated values
                var message = new JObject
                {
                    new JProperty("eventType", "newAdminEvent"),
                    new JProperty( "type", "consumer"),
                    new JProperty("port", _adminPort),
                    new JProperty("host", _host),
                    new JProperty("conits", _conits)
                };

                // Create a TCP client and connect to the server
                using (var client = new TcpClient())
                {
                    client.Connect("app1", 6666);

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
