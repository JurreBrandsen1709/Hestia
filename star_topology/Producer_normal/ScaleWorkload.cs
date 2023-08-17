using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using System.IO;
using Newtonsoft.Json;
using System.Collections.Generic;

class ScaleWorkload
{
    static async Task Main(string[] args)
    {
        var config = new ProducerConfig { BootstrapServers = "broker:9092" };

        // Read the JSON file and convert it to a dictionary
        var myDict = JsonConvert.DeserializeObject<Dictionary<string, string>>(File.ReadAllText("w3_priority.json"));

        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            int step = 100;        // Step size for increasing/decreasing delay
            int delay = 2100;         // Current delay in milliseconds
            int msgCount = 0;
            int i = 0;

            while (delay >= 0)
            {
                delay -= step;
                msgCount = 0;
                Console.WriteLine("Delay: " + delay);
                while (msgCount != 100)
                {
                    // Get the message from the dictionary
                    string message = "a";

                    var deliveryResult = await producer.ProduceAsync("topic_normal", new Message<Null, string> { Value = message});

                    await Task.Delay(delay); // Delay to control the message rate

                    msgCount++;
                    i++;
                }
            }

        }
    }
}
