using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Newtonsoft.Json.Linq;
using System;
using System.Threading.Tasks;

namespace Dyconit.Overlord
{
    public class DyconitOverlord
    {
        private readonly AdminClientConfig _adminClientConfig;

        public DyconitOverlord(string bootstrapServers)
        {
            _adminClientConfig = new AdminClientConfig
            {
                BootstrapServers = bootstrapServers,
            };
        }
        public void ProcessConsumerStatistics(string json, ConsumerConfig config)
        {
            var stats = JObject.Parse(json);
            var s1 = stats["brokers"][$"{config.BootstrapServers}/1"]["rtt"]["avg"];

            Console.WriteLine($"Consumer RTT: {s1}");
        }

        public void ProcessProducerStatistics(string json, ProducerConfig config)
        {
            var stats = JObject.Parse(json);
            var s1 = stats["brokers"][$"{config.BootstrapServers}/1"]["rtt"]["avg"];

            Console.WriteLine($"Producer RTT: {s1}");

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
