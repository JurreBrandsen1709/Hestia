// todo: Do something with the weights.

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

        var DyconitLogger = new DyconitPerformanceLogger(1337); // TODO put in the complete configuration and parse it at the performance logger.

        const string topic = "input_topic";

        // TODO maybe add áway to let programmers create collections seperately and then add them to the place where they are needed.
        // Add what collection the conits are in.
        string collection = "Transactions_consumer";

        // Make a dictionary with the collection and the conits in it.
        Dictionary<string, object> conitConfiguration = new Dictionary<string, object>
        {
            { "collection", collection },
            { "Staleness", 1337 }, // wanneer heb ik voor het laatst data gezien? Ben ik er overheen, dan moet ik aan alle andere conits vragen of ze nog data hebben.
            { "OrderError", 42 },
            { "NumericalError", 8 }
        };

        // Create debug saying that we created the conitConfiguration and it's content.
        Console.WriteLine("Created conitConfiguration with the following content:");
        foreach (KeyValuePair<string, object> kvp in conitConfiguration)
        {
            Console.WriteLine("Key = {0}, Value = {1}", kvp.Key, kvp.Value);
        }

        CancellationTokenSource cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };

        using (var consumer = new DyconitConsumerBuilder<Null, string>(configuration, conitConfiguration, 1).Build())
        {
            consumer.Subscribe(topic);
            try
            {
                while (true)
                {
                    // Here we need to know what is the last time we saw data.
                    // if this time exceeds the staleness, we need to ask all other conits if they have data in a blocking way.
                    // if we do not exceed this bound, we can still ask other conits if they have data, but do it async.
                    var consumeResult = consumer.Consume(cts.Token);
                    var consumedTime = DateTime.Now; // Get the current time as the consumed time

                    // add random wait time to simulate processing time.
                    Random rnd = new Random();
                    int waitTime = rnd.Next(0, 3000);
                    await Task.Delay(waitTime);

                    DyconitLogger.LogConsumedMessage(consumedTime); // Log the consumed message time

                    var inputMessage = consumeResult.Message.Value;

                    // Console.WriteLine($"Consumed message with value '{consumeResult.Value}', weight '{GetMessageWeight(consumeResult)}'");
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
