using Confluent.Kafka;
using System;
using System.Threading;
using Microsoft.Extensions.Configuration;
using System.IO;
using Newtonsoft.Json.Linq;
using Dyconit.Consumer;
using Dyconit.Overlord;
using System.Collections.Generic;

class Consumer {
    static void Main(string[] args)
    {
        var configuration = new ConsumerConfig
        {
            BootstrapServers = "kafka-a:9092",
            GroupId = "kafka-dotnet-getting-started",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            // StatisticsIntervalMs = 2000,
        };

        var adminClient = new DyconitOverlord("kafka-a:9092", 100000);
        const string topic = "input_topicc";

        CancellationTokenSource cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };

        using (var consumer = new DyconitConsumerBuilder<Null, string>(configuration, adminClient, 1337).Build())
        {
            consumer.Subscribe(topic);

            try
            {
                while (true)
                {
                    var consumeResult = consumer.Consume(cts.Token);
                    var inputMessage = consumeResult.Message.Value;
                    Console.WriteLine($"Consumed message '{inputMessage}' at: '{consumeResult.TopicPartitionOffset}'.");
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