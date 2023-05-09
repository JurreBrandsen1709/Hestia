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

        using (var producer = new ProducerBuilder<string, byte[]>(configuration)
                                    .SetKeySerializer(Serializers.Utf8)
                                    .SetValueSerializer(Serializers.ByteArray)
                                    .Build())
        {

            var numProduced = 0;
            Random rnd = new Random();
            const int numMessages = 5000;
            for (int i = 0; i < numMessages; ++i)
            {

                // Create a byte array to hold the larger payload data
                byte[] payload = new byte[409600];
                rnd.NextBytes(payload);

                var message = new Message<string, byte[]>
                {
                    Key = "c"+i,
                    Value = payload
                };

                var deliveryReport = producer.ProduceAsync(topic, message).GetAwaiter().GetResult();
                Console.WriteLine($"T: {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")} - {i}");
                numProduced += 1;
            }

            producer.Flush(TimeSpan.FromSeconds(10));
            Console.WriteLine($"{numProduced} messages were produced to topic {topic}");
        }
    }
}
