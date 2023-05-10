using Confluent.Kafka;
using System;
using Microsoft.Extensions.Configuration;
using System.Text;

class Producer {
    static void Main(string[] args)
    {
        // configure bootstrap.servers in text
        var configuration = new ProducerConfig
        {
            BootstrapServers = "localhost:9092"
        };

        const string topic = "TestTopicccc";

        string[] users = { "eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther" };
        string[] items = { "a"};

        using var adminClient = new AdminClientBuilder(configuration).Build();

        using (var producer = new ProducerBuilder<string, int>(configuration)
                                    .Build())
        {
            var numProduced = 0;
            Random rnd = new Random();
            const int numMessages = 100000;
            for (int i = 0; i < numMessages; ++i)
            {

                // Create a byte array to hold the larger payload data
                // byte[] payload = new byte[4096];
                // rnd.NextBytes(payload);

                int index = rnd.Next(5000);


                var message = new Message<string, int>
                {
                    Key = "a",
                    Value = index
                };


                producer.ProduceAsync(topic, message).ContinueWith(task =>
{
                    var result = task.Result;
                    var x = result.Timestamp.UtcDateTime;
                    var y = result.Offset;
                    var z = result.TopicPartitionOffset;
                    Console.WriteLine($"{x} - {y} Produced message --- key: {message.Value}");
                });

            }

            producer.Flush(TimeSpan.FromSeconds(10));
            Console.WriteLine($"{numProduced} messages were produced to topic {topic}");
        }
    }


}
