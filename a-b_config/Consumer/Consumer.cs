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
using System.Linq;

class Consumer {

    static double GetMessageWeight(ConsumeResult<Null, string> result)
    {
        double weight = -1.0;

        var weightHeader = result.Message.Headers.FirstOrDefault(h => h.Key == "Weight");
        if (weightHeader != null)
        {
            var weightBytes = weightHeader.GetValueBytes();
            weight = BitConverter.ToDouble(weightBytes);
        }

        return weight;
    }
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

                    Console.WriteLine($"Consumed message with value '{consumeResult.Value}', weight '{GetMessageWeight(consumeResult)}'");


                    // Console.WriteLine($"Consumed message '{weight}' at: '{consumeResult.TopicPartitionOffset}'.");

                    // var parts = inputMessage.Split(',');
                    // var weight = double.Parse(parts[0]);
                    // var payload = parts[1];

                    // Console.WriteLine($"Consumed message '{payload}' with weight '{weight}' at: '{consumeResult.TopicPartitionOffset}'.");

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
