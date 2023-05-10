using Confluent.Kafka;
using System;
using Microsoft.Extensions.Configuration;
using System.Text;
using System.Timers;
using System.IO;
using Dyconit.Producer;
using Dyconit.Overlord;

class Producer {
    static void Main(string[] args)
    {
        // configure bootstrap.servers in text
        var configuration = new ProducerConfig
        {
            BootstrapServers = "localhost:9092",
            StatisticsIntervalMs = 2000,
        };

        var adminClient = new DyconitOverlord("localhost:9092", 2000);



        const string topic = "TestTopicccc";

        string[] users = { "eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther" };
        string[] items = { "a"};

        using (var producer = new DyconitProducerBuilder<string, byte[]>(configuration, adminClient, 1337)
                                    .SetKeySerializer(Serializers.Utf8)
                                    .SetValueSerializer(Serializers.ByteArray)
                                    .Build())
        {
            Console.WriteLine("Press Ctrl+C to quit.");

            var numProduced = 0;
            Random rnd = new Random();
            const int numMessages = 5000;

            // Set up a timer to send 5 messages every second
            var timer = new Timer(1000); // 1000 milliseconds = 1 second
            timer.Elapsed += (sender, e) => {
                for (int i = 0; i < 5; i++) {
                    // Create a byte array to hold the larger payload data
                    byte[] payload = new byte[4096];
                    rnd.NextBytes(payload);

                    var message = new Message<string, byte[]>
                    {
                        Key = "d" + numProduced,
                        Value = payload
                    };

                    var deliveryReport = producer.ProduceAsync(topic, message).GetAwaiter().GetResult();
                    Console.WriteLine($"T: {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")} - {numProduced}");
                    numProduced += 1;
                }
            };
            timer.Start();

            // Wait until all messages have been sent
            while (numProduced < numMessages) {
                // You can do something else while waiting here
            }

            timer.Stop();

            producer.Flush(TimeSpan.FromSeconds(10));
            Console.WriteLine($"{numProduced} messages were produced to topic {topic}");
        }
    }
}
