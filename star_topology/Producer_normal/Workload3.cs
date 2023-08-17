using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using System.IO;
using Newtonsoft.Json;
using System.Collections.Generic;

class Workload3
{
    static async Task Main(string[] args)
    {
        var config = new ProducerConfig { BootstrapServers = "broker:9092" };

        // Read the JSON file and convert it to a dictionary
        var myDict = JsonConvert.DeserializeObject<Dictionary<string, string>>(File.ReadAllText("w3_priority.json"));

        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            int i = 0;

            while (i != 4000)
            {
                // Get the message from the dictionary
                string message = myDict[i.ToString()];

                var deliveryResult = await producer.ProduceAsync("topic_normal", new Message<Null, string> { Value = message});

                i++;
            }
        }
    }
}
