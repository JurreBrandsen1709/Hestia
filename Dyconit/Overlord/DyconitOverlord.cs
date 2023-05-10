using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Newtonsoft.Json.Linq;
using System;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace Dyconit.Overlord
{
    public class DyconitOverlord
    {
        private readonly AdminClientConfig _adminClientConfig;
        private readonly IProducer<Null, string> _producer;
        private readonly int _rttThreshold;

        public DyconitOverlord(string bootstrapServers, int rttThreshold)
        {
            _adminClientConfig = new AdminClientConfig
            {
                BootstrapServers = bootstrapServers,
            };
            _rttThreshold = rttThreshold;
        }

        public void ProcessConsumerStatistics(string json, ConsumerConfig config)
        {
            var stats = JObject.Parse(json);
            var s1 = stats["brokers"][$"{config.BootstrapServers}/1"]["rtt"]["avg"];

            Console.WriteLine($"Consumer RTT: {s1}");

            if (s1.Value<int>() >= _rttThreshold)
            {
                Console.WriteLine($"RTT threshold exceeded: {s1.Value<int>()} >= {_rttThreshold}");

                SendMessageOverTcp("RTT threshold exceeded for consumer!");
            }
        }

        public void ProcessProducerStatistics(string json, ProducerConfig config)
        {
            var stats = JObject.Parse(json);
            var s1 = stats["brokers"][$"{config.BootstrapServers}/1"]["rtt"]["avg"];

            Console.WriteLine($"Producer RTT: {s1}");

            if (s1.Value<int>() >= _rttThreshold)
            {
                Console.WriteLine($"RTT threshold exceeded: {s1.Value<int>()} >= {_rttThreshold}");

                SendMessageOverTcp("RTT threshold exceeded for producer!");
            }
        }

        private void SendMessageOverTcp(string message)
        {
            try
            {
                // Create a TCP client and connect to the server
                using (var client = new TcpClient())
                {
                    client.Connect("localhost", 1337);

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

        public async Task<int> GetPartitionCount(string topicName)
        {
            using var adminClient = new AdminClientBuilder(_adminClientConfig).Build();

            var metadata = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(10));

            return metadata.Topics[0].Partitions.Count;
        }

        public async Task<int> AddPartitions(string topicName, int partitionCount)
        {
            using var adminClient = new AdminClientBuilder(_adminClientConfig).Build();

            var partitionSpecification = new PartitionsSpecification
            {
                Topic = topicName,
                IncreaseTo = partitionCount
            };

            await adminClient.CreatePartitionsAsync(new[] { partitionSpecification });

            var metadata = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(10));

            return metadata.Topics[0].Partitions.Count;
        }
    }
}
