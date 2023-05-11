using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using System.IO;
using Newtonsoft.Json.Linq;
using Dyconit.Consumer;
using Dyconit.Producer;
using Dyconit.Overlord;
using System.Collections.Generic;

class Consumer {
    public static async Task Main()
    {
        var configuration = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "kafka-dotnet-getting-started",
            // EnableAutoCommit = true,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            // StatisticsIntervalMs = 2000,
        };

        var adminClient = new DyconitOverlord("localhost:9092", 100000);
        const string topic = "input_topic";

        CancellationTokenSource cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };

        using (var consumer = new DyconitConsumerBuilder<Null, string>(configuration, adminClient, 1337).Build())
            {
                consumer.Subscribe(topic);

                var producerConfig = new ProducerConfig
                {
                    BootstrapServers = "localhost:9092",
                    // StatisticsIntervalMs = 2000,
                };

                using (var producer = new DyconitProducerBuilder<Null, string>(producerConfig, adminClient, 1337).Build())
                {

                    try
                    {
                        while (true)
                        {
                            var consumeResult = consumer.Consume(cts.Token);
                            var inputMessage = consumeResult.Message.Value;

                            Console.WriteLine($"Consumed message '{inputMessage}' at: '{consumeResult.TopicPartitionOffset}'.");

                            // Process the input message
                            var outputMessage = inputMessage.ToUpper();

                            // wait a random amount of time to simulate processing time
                            Random rnd = new Random();
                            int delay = rnd.Next(0, 1000);
                            Thread.Sleep(delay);

                            var produceResult = await producer.ProduceAsync("output_topic", new Message<Null, string> { Value = outputMessage });

                            Console.WriteLine($"Produced message '{outputMessage}' to topic {produceResult.TopicPartitionOffset}");
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // Ctrl-C was pressed.
                    }
                    finally
                    {
                        consumer.Close();
                    }
            }
        }
    }
}