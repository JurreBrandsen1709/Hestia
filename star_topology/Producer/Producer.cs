using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using System.IO;
using Newtonsoft.Json;
using System.Collections.Generic;

class Producer
{
    static async Task Main(string[] args)
    {
        var config = new ProducerConfig { BootstrapServers = "broker:9092" };

        // Read the JSON file and convert it to a dictionary
        var myDict = JsonConvert.DeserializeObject<Dictionary<string, string>>(File.ReadAllText("priority.json"));

        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            int amplitude = 2000; // Maximum delay in milliseconds
            int step = 50;        // Step size for increasing/decreasing delay
            int delay = 0;         // Current delay in milliseconds
            int i = 0;
            int msgCount = 0;

            while (msgCount != 200)
            {
                // Get the message from the dictionary
                string message = myDict[i.ToString()];

                var deliveryResult = await producer.ProduceAsync("topic_priority", new Message<Null, string> { Value = message});

                await Task.Delay(delay); // Delay to control the message rate

                delay += step;
                if (delay >= amplitude || delay <= 0)
                {
                    step = -step;
                }

                i++;
                msgCount++;
            }
        }
    }
}
