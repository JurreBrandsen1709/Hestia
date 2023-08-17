using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using System.IO;
using Newtonsoft.Json;
using System.Collections.Generic;

class Workload1
{
    static async Task Main(string[] args)
    {
        var config = new ProducerConfig { BootstrapServers = "broker:9092" };

        // Read the JSON file and convert it to a dictionary
        var myDict = JsonConvert.DeserializeObject<Dictionary<string, string>>(File.ReadAllText("normal.json"));
        var delayDict = JsonConvert.DeserializeObject<Dictionary<string, int>>(File.ReadAllText("delay_w1.json"));

        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            int i = 0;

            while (i != 800)
            {
                // Get the message from the dictionary
                string message = myDict[i.ToString()];

                var deliveryResult = await producer.ProduceAsync("topic_normal", new Message<Null, string> { Value = message});

                await Task.Delay(delayDict[i.ToString()]); // Delay to control the message rate

                i++;
            }
        }
    }
}
