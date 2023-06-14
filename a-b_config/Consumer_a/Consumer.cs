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
    private static List<ConsumeResult<Null, string>> _uncommittedConsumedMessages;
    private static List<ConsumeResult<Null, string>> _uncommittedConsumedMessagesTest;
    private static long _lastCommittedOffset = -1;
    static async Task Main()
    {
        _storedMessages = new List<string>();
        var configuration = GetConsumerConfiguration();
        // SetStatisticsHandler((_, json) =>
        //     {
        //         _adminClient.ProcessConsumerStatistics(json, config);
        //     });
        _uncommittedConsumedMessages = new List<ConsumeResult<Null, string>>();
        _uncommittedConsumedMessagesTest = new List<ConsumeResult<Null, string>>();

        var adminPort = FindPort();

        const string topic = "d";
        string collection = "Transactions_consumer";

        Dictionary<string, object> conitConfiguration = GetConitConfiguration(collection);

        var DyconitLogger = new DyconitAdmin(configuration.BootstrapServers, 1, adminPort, conitConfiguration);

        PrintConitConfiguration(conitConfiguration);

        CancellationTokenSource cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };

        using (var consumer = CreateDyconitConsumer(configuration, conitConfiguration, adminPort, DyconitLogger))
        {
            consumer.Subscribe(topic);
            try
            {
                while (true)
                {
                    if (_lastCommittedOffset > 0)
                    {
                        Console.WriteLine($"Assigning topic {topic} with offset {_lastCommittedOffset}");
                        consumer.Assign(new List<TopicPartitionOffset>() { new TopicPartitionOffset(topic, 0, _lastCommittedOffset) });
                    }
                    var consumeResult = consumer.Consume(cts.Token);

                    // if we consume a message that is older than the last committed offset, we ignore it.
                    if (consumeResult.Offset < _lastCommittedOffset)
                    {
                        Console.WriteLine($"Ignoring message with offset {consumeResult.Offset}");
                        continue;
                    }

                    _uncommittedConsumedMessages.Add(consumeResult);
                    _uncommittedConsumedMessagesTest.Add(consumeResult);
                    var consumedTime = DateTime.Now;

                    Random rnd = new Random();
                    int waitTime = rnd.Next(0, 2000);
                    await Task.Delay(waitTime);

                    // PrintStoredMessages();
                    SyncResult result = await DyconitLogger.BoundStaleness(consumedTime, _uncommittedConsumedMessages);
                    _uncommittedConsumedMessages = result.Data;
                    Console.WriteLine($"result: {result.Data.Count} {result.changed}");
                    if (result.changed == true)
                    {
                        Console.WriteLine($"Received messages: {_uncommittedConsumedMessages.Count}, local messages: {_uncommittedConsumedMessagesTest.Count}");

                        CommitStoredMessages(consumer);
                        Console.WriteLine("Committed messages");
                    }
                    else
                    {
                        Console.WriteLine("No messages to commit");
                    }

                    result = await DyconitLogger.BoundNumericalError(_uncommittedConsumedMessages);
                    _uncommittedConsumedMessages = result.Data;
                    Console.WriteLine($"result: {result.Data.Count} {result.changed}");

                    if (result.changed == true)
                    {
                        Console.WriteLine($"Received messages: {_uncommittedConsumedMessages.Count}, local messages: {_uncommittedConsumedMessagesTest.Count}");

                        CommitStoredMessages(consumer);
                        Console.WriteLine("Committed messages");
                    }
                    else
                    {
                        Console.WriteLine("No messages to commit");
                    }







                    // _uncommittedConsumedMessages = await DyconitLogger.BoundStaleness(consumedTime, _uncommittedConsumedMessages);
                    // _uncommittedConsumedMessages = await DyconitLogger.BoundNumericalError(_uncommittedConsumedMessages);
                    // Console.WriteLine("------------------------------------------");
                    // PrintStoredMessages();

                    var inputMessage = consumeResult.Message.Value;


                    Console.WriteLine($"Consumed message with value '{inputMessage}', weight '{GetMessageWeight(consumeResult)}'");

                    // if (ShouldStoreMessage(i))
                    // {
                    //     _storedMessages.Add(inputMessage);
                    // }
                    // else
                    // {
                    //     CommitStoredMessages(consumer);
                    //     Console.WriteLine("Committed messages");
                    // }
                    // i++;
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
            GroupId = "tryout-1",
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            StatisticsIntervalMs = 5000,
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
            { "NumericalError", 30 }
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

    static IConsumer<Null, string> CreateDyconitConsumer(ConsumerConfig configuration, Dictionary<string, object> conitConfiguration, int adminPort, DyconitAdmin DyconitLogger)
    {
        return new DyconitConsumerBuilder<Null, string>(configuration, conitConfiguration, 1, adminPort, DyconitLogger).Build();
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

    // static bool ShouldStoreMessage(int i)
    // {
    //     // Replace with your actual condition
    //     return i % 10 != 0;
    // }

    static void CommitStoredMessages(IConsumer<Null, string> consumer)
    {

       foreach (ConsumeResult<Null, string> storedMessage in _uncommittedConsumedMessages)
       {
            consumer.Commit(storedMessage);
       }

       // Retrieve the committed offsets for the assigned partitions
        var committedOffsets = consumer.Committed(TimeSpan.FromSeconds(10));

        // Process the committed offsets
        foreach (var committedOffset in committedOffsets)
        {
            // Console.WriteLine($"Partition: {committedOffset.Partition}, Committed Offset: {committedOffset.Offset}");
            _lastCommittedOffset = committedOffset.Offset;
        }


       // Clear the list of stored messages
         _uncommittedConsumedMessages.Clear();
         _uncommittedConsumedMessagesTest.Clear();
    }

}
