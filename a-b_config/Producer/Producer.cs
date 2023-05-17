// TODO add difference between read and write in messages that are send.
// Read is a command that is send to the broker to get the data from the topic.
// Write is a event that is send to the broker to write the data to the topic.

using Confluent.Kafka;
using System;
using Microsoft.Extensions.Configuration;
using System.Text;
using System.Timers;
using System.IO;
using Dyconit.Producer;
using Dyconit.Message;
using Dyconit.Overlord;
using System.Collections.Generic;

class Producer {
    static void Main(string[] args)
    {
        // configure bootstrap.servers in text
        var configuration = new ProducerConfig
        {
            BootstrapServers = "localhost:9092",
            StatisticsIntervalMs = 2000,
        };


        const string topic = "input_topic";

        // Add what collection the conits are in.
        string collection = "bankTransaction";

        // Make a dictionary with the collection and the conits in it.
        Dictionary<string, object> conitConfiguration = new Dictionary<string, object>
        {
            { "collection", collection },
            { "Staleness", 1000 },
            { "OrderError", 0.1 },
            { "NumericalError", 0.1 }
        };

        // Create debug saying that we created the conitConfiguration and it's content.
        Console.WriteLine("Created conitConfiguration with the following content:");
        foreach (KeyValuePair<string, object> kvp in conitConfiguration)
        {
            Console.WriteLine("Key = {0}, Value = {1}", kvp.Key, kvp.Value);
        }

        using (var producer = new DyconitProducerBuilder<Null, String>(configuration, conitConfiguration, 2).Build())
        {
            Console.WriteLine("Press Ctrl+C to quit.");

            var numProduced = 0;
            Random rnd = new Random();
            const int numMessages = 5000;

            // Set up a timer to send 5 messages every second
            var timer = new Timer(1000); // 1000 milliseconds = 1 second
            timer.Elapsed += (sender, e) => {
                for (int i = 0; i < 5; i++) {

                    // create a random length payload string
                    var payload = new string('x', rnd.Next(1, 10));

                    var message = new DyconitMessage<Null, string>
                    {
                        Value = payload,
                        Weight = 3.0
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
