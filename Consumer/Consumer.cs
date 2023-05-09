// docker exec -t broker kafka-console-consumer --bootstrap-server localhost:9092 --topic banaan --from-beginning --max-messages 10

using Confluent.Kafka;
using System;
using System.Threading;
using Microsoft.Extensions.Configuration;
using System.IO;
using Newtonsoft.Json.Linq;
using Dyconit.Consumer;
using Dyconit.Overlord;

class Consumer {
    static void Main(string[] args)
    {
        var configuration = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "kafka-dotnet-getting-started",
            EnableAutoCommit = true,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            StatisticsIntervalMs = 2000,
        };

        var adminClient = new DyconitOverlord("localhost:9092");


        const string topic = "TestTopicccc";

        CancellationTokenSource cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };

        using (var consumer = new DyconitConsumerBuilder<string, byte[]>(configuration, adminClient)
    .SetKeyDeserializer(Deserializers.Utf8)
    .SetValueDeserializer(Deserializers.ByteArray)
    .Build())
        {
            consumer.Subscribe(topic);

            try
            {
                while (true)
                {
                    var consumeResult = consumer.Consume(cts.Token);
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