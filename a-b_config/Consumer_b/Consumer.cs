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
using System.Net.NetworkInformation;
using System.Net.Sockets;

class Consumer {

    // TODO move these helper functions to a seperate file.
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

    private static int FindPort()
        {
            var random = new Random();
            int adminClientPort;
            while (true)
            {
                adminClientPort = random.Next(5000, 10000);
                var isPortInUse = IPGlobalProperties.GetIPGlobalProperties()
                    .GetActiveTcpListeners()
                    .Any(x => x.Port == adminClientPort);
                if (!isPortInUse)
                {
                    break;
                }
            }
            return adminClientPort;
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

        var adminPort = FindPort();


        const string topic = "input_topic";

        // TODO maybe add áway to let programmers create collections seperately and then add them to the place where they are needed.
        // Add what collection the conits are in.
        string collection = "Transactions_consumer";

        // Make a dictionary with the collection and the conits in it.
        Dictionary<string, object> conitConfiguration = new Dictionary<string, object>
        {
            { "collection", collection },
            { "Staleness", 1337 },
            { "OrderError", 42 },
            { "NumericalError", 8 }
        };

        // var DyconitLogger = new DyconitPerformanceLogger(conitConfiguration);
        var DyconitLogger = new DyconitAdmin(configuration.BootstrapServers, 1, adminPort, conitConfiguration);

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

        using (var consumer = new DyconitConsumerBuilder<Null, string>(configuration, conitConfiguration, 1, adminPort).Build())
        {
            consumer.Subscribe(topic);
            try
            {
                while (true)
                {

                    // The Staleness actor checks for each remote node that shares one of the affected Dynamic Conits if it has synchronized its
                    // updates within the set staleness bound. If this is not the case, the Staleness actor synchronizes with
                    // the remote node by requesting its updates.

                    var consumeResult = consumer.Consume(cts.Token);
                    var consumedTime = DateTime.Now; // Get the current time as the consumed time

                    // add random wait time to simulate processing time.
                    Random rnd = new Random();
                    int waitTime = rnd.Next(0, 3000);
                    await Task.Delay(waitTime);

                    DyconitLogger.BoundStaleness(consumedTime); // Log the consumed message time

                    var inputMessage = consumeResult.Message.Value;

                    Console.WriteLine($"Consumed message with value '{inputMessage}', weight '{GetMessageWeight(consumeResult)}'");
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
