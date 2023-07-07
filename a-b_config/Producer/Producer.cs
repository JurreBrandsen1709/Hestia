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
using System.Net.NetworkInformation;
using System.Linq;
using Dyconit.Helper;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

class Producer {
    static void Main(string[] args)
    {
        // configure bootstrap.servers in text
        var configuration = new ProducerConfig
        {
            BootstrapServers = "localhost:9092",
            StatisticsIntervalMs = 2000,
        };

        const string topic = "topic_priority";
        using (var producer = new ProducerBuilder<Null, String>(configuration).Build())
        {
            Console.WriteLine("Press Ctrl+C to quit.");

            var numProduced = 0;
            Random rnd = new Random();

            for (int i = 0; i<200; i++)
            {
                    // create a random length payload string
                    var payload = new string(i.ToString() + " " + topic);

                    var message = new DyconitMessage<Null, string>
                    {
                        Value = payload,
                        Weight = 3.0
                    };

                    var deliveryReport = producer.ProduceAsync(topic, message).GetAwaiter().GetResult();
                    Console.WriteLine($"T: {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")} - {numProduced}");
                    }

            producer.Flush(TimeSpan.FromSeconds(10));
        }
    }
}
