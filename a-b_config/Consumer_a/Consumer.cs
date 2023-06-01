using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using Dyconit.Overlord;
using Dyconit.Consumer;

class Consumer
{
    private static List<string> _storedMessages;
    static async Task Main()
    {
        _storedMessages = new List<string>();
        var configuration = GetConsumerConfiguration();

        var adminPort = FindPort();

        const string topic = "input_topic";
        string collection = "Transactions_consumer";

        Dictionary<string, object> conitConfiguration = GetConitConfiguration(collection);

        var DyconitLogger = new DyconitAdmin(configuration.BootstrapServers, 1, adminPort, conitConfiguration);

        PrintConitConfiguration(conitConfiguration);

        int i = 1;
        CancellationTokenSource cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };

        using (var consumer = CreateDyconitConsumer(configuration, conitConfiguration, adminPort))
        {
            consumer.Subscribe(topic);
            try
            {


                while (true)
                {
                    var consumeResult = consumer.Consume(cts.Token);
                    var consumedTime = DateTime.Now;

                    Random rnd = new Random();
                    int waitTime = rnd.Next(0, 3000);
                    await Task.Delay(waitTime);

                    // PrintStoredMessages();
                    _storedMessages = await DyconitLogger.BoundStaleness(consumedTime, _storedMessages);
                    // Console.WriteLine("------------------------------------------");
                    // PrintStoredMessages();

                    var inputMessage = consumeResult.Message.Value;

                    Console.WriteLine($"Consumed message with value '{inputMessage}', weight '{GetMessageWeight(consumeResult)}'");

                    if (ShouldStoreMessage(i))
                    {
                        _storedMessages.Add(consumeResult.Message.Value);
                    }
                    else
                    {
                        CommitStoredMessages(consumer, _storedMessages);
                        Console.WriteLine("Committed messages");
                    }
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

    //create a funciton that prints to stored messages
    static void PrintStoredMessages()
    {
        Console.WriteLine("Stored messages:");
        foreach (var message in _storedMessages)
        {
            Console.WriteLine($"Message with value '{message}");
        }
    }

    static ConsumerConfig GetConsumerConfiguration()
    {
        return new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "kafka-dotnet-getting-started",
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };
    }

    // TODO move to dyconit helper functions.
    static int FindPort()
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

    static Dictionary<string, object> GetConitConfiguration(string collection)
    {
        return new Dictionary<string, object>
        {
            { "collection", collection },
            { "Staleness", 5000 },
            { "OrderError", 42 },
            { "NumericalError", 8 }
        };
    }

    static void PrintConitConfiguration(Dictionary<string, object> conitConfiguration)
    {
        Console.WriteLine("Created conitConfiguration with the following content:");
        foreach (KeyValuePair<string, object> kvp in conitConfiguration)
        {
            Console.WriteLine("Key = {0}, Value = {1}", kvp.Key, kvp.Value);
        }
    }

    static IConsumer<Null, string> CreateDyconitConsumer(ConsumerConfig configuration, Dictionary<string, object> conitConfiguration, int adminPort)
    {
        return new DyconitConsumerBuilder<Null, string>(configuration, conitConfiguration, 1, adminPort).Build();
    }

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

    static bool ShouldStoreMessage(int i)
    {
        // Replace with your actual condition
        return i % 10 != 0;
    }

    static void CommitStoredMessages(IConsumer<Null, string> consumer, List<string> storedMessages)
    {
        foreach (var storedMessage in storedMessages)
        {
            consumer.Commit();
        }
    }
}
