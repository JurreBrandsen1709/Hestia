using Confluent.Kafka;
using System;
using Dyconit.Message;


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
                        Weight = 1.0
                    };

                    var deliveryReport = producer.ProduceAsync(topic, message).GetAwaiter().GetResult();
                    Console.WriteLine($"T: {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")} - {numProduced}");
                    }

            producer.Flush(TimeSpan.FromSeconds(10));
        }
    }
}
