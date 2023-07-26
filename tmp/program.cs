using System;
using System.Text;
using System.Collections.Generic;
using Confluent.Kafka;

public class Producer
    {
        public static void Main(string[] args)
        {
            var config = new ProducerConfig { BootstrapServers = "broker:9092" };

            using (var producer = new ProducerBuilder<string, string>(config).Build())
            using (var producer2 = new DependentProducerBuilder<Null, int>(producer.Handle).Build())
            {
                // write (null, int) data to topic "second-data" using the same underlying broker connections.
                for (int i = 0; i < 50000; i++)
                {
                    producer2.Produce("topic6", new Message<Null, int> { Value = 42 });

                    // add a little delay between each message
                    // System.Threading.Thread.Sleep(1);
                }

                for (int i = 0; i < 50000; i++)
                {
                    producer2.ProduceAsync("topic2", new Message<Null, int> { Value = 42 });
                }

                for (int i = 0; i < 50000; i++)
                {
                    producer2.ProduceAsync("topic3", new Message<Null, int> { Value = 42 });
                }

                for (int i = 0; i < 50000; i++)
                {
                    producer2.ProduceAsync("topic4", new Message<Null, int> { Value = 42 });
                }

                for (int i = 0; i < 50000; i++)
                {
                    producer2.ProduceAsync("topic5", new Message<Null, int> { Value = 42 });
                }

                // As the Tasks returned by ProduceAsync are not waited on there will still be messages in flight.
                producer.Flush(TimeSpan.FromSeconds(10));
            }
        }
    }
