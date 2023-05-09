using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System;
using System.Threading.Tasks;

class AdminClientExample
{
    static async Task Main(string[] args)
    {
        var bootstrapServers = "localhost:9092";
        var topicName = "TestTopicccc";

        var adminClientConfig = new AdminClientConfig
        {
            BootstrapServers = bootstrapServers
        };

        using var adminClient = new AdminClientBuilder(adminClientConfig).Build();

        var metadata = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(10));
        Console.WriteLine($"The topic {topicName} currently has {metadata.Topics[0].Partitions.Count} partitions.");

        // wait for 1 minute
        await Task.Delay(TimeSpan.FromMinutes(1));

        // create additional partitions
        var partitionSpecification = new PartitionsSpecification
        {
            Topic = topicName,
            IncreaseTo = metadata.Topics[0].Partitions.Count + 1
        };

        await adminClient.CreatePartitionsAsync(new[] { partitionSpecification });
        metadata = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(10));

        Console.WriteLine($"The topic {topicName} now has {metadata.Topics[0].Partitions.Count} partitions.");
    }
}
