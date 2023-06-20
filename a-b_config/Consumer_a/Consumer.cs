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
using Dyconit.Helper;

class Consumer
{
    private static List<string> _storedMessages;
    private static List<ConsumeResult<Null, string>> _uncommittedConsumedMessages;
    private static List<ConsumeResult<Null, string>> _uncommittedConsumedMessagesTest;
    private static long _lastCommittedOffset = -1;
    private static Dictionary<string, Dictionary<string, object>> _localCollection = new Dictionary<string, Dictionary<string, object>>();
    static async Task Main()
    {
        _storedMessages = new List<string>();
        var configuration = GetConsumerConfiguration();
        _uncommittedConsumedMessages = new List<ConsumeResult<Null, string>>();
        _uncommittedConsumedMessagesTest = new List<ConsumeResult<Null, string>>();

        var adminPort = DyconitHelper.FindPort();

        var topics = new List<string>
        {
            "topic_priority",
            "topic_normal",
        };
        string collection = "topic_priority";
        string collection2 = "topic_normal";

        Dictionary<string, object> conitConfiguration = DyconitHelper.GetConitConfiguration(2000, 20, 5);
        Dictionary<string, object> conitConfiguration2 = DyconitHelper.GetConitConfiguration(5000, 20, 10);

        DyconitHelper.PrintConitConfiguration(conitConfiguration);
        DyconitHelper.PrintConitConfiguration(conitConfiguration2);

        // put the collections in a dictionary
        _localCollection.Add(collection, conitConfiguration);
        _localCollection.Add(collection2, conitConfiguration2);

        var DyconitLogger = new DyconitAdmin(configuration.BootstrapServers, 1, adminPort, _localCollection);

        CancellationTokenSource cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };

        bool commit = false;

        using (var consumer = DyconitHelper.CreateDyconitConsumer(configuration, _localCollection, adminPort, DyconitLogger))
        {
            consumer.Subscribe(topics);
            try
            {
                while (true)
                {
                    var consumeResult = consumer.Consume(cts.Token);
                    var topic = consumeResult.Topic;

                    // find the collection corresponding to the topic
                    var collectionConfiguration = _localCollection[topic];

                    var inputMessage = consumeResult.Message.Value;
                    Console.WriteLine($"[{adminPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Consumed message with value '{inputMessage}', weight '{DyconitHelper.GetMessageWeight(consumeResult)}'");
                    _lastCommittedOffset = consumeResult.Offset;

                    // if we consume a message that is older than the last committed offset, we ignore it.
                    if (consumeResult.Offset < _lastCommittedOffset)
                    {
                        Console.WriteLine($"[{adminPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Ignoring message with offset {consumeResult.Offset}");
                        continue;
                    }

                    Random rnd = new Random();
                    int waitTime = rnd.Next(0, 2000);
                    await Task.Delay(waitTime);

                    _uncommittedConsumedMessages.Add(consumeResult);
                    _uncommittedConsumedMessagesTest.Add(consumeResult);
                    var consumedTime = DateTime.Now;

                    Console.WriteLine($"[{adminPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} bounding staleness");
                    SyncResult result = await DyconitLogger.BoundStaleness(consumedTime, _uncommittedConsumedMessages, collectionConfiguration, topic);
                    _uncommittedConsumedMessages = result.Data;
                    commit = result.changed;

                    if (_uncommittedConsumedMessages.Last().Offset >= _lastCommittedOffset)
                    {
                        _lastCommittedOffset = _uncommittedConsumedMessages.Last().Offset+1;
                    }


                    Console.WriteLine($"[{adminPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} bounding numerical error");
                    result = await DyconitLogger.BoundNumericalError(_uncommittedConsumedMessages, collectionConfiguration, topic);
                    _uncommittedConsumedMessages = result.Data;
                    commit = commit || result.changed;

                    if (_uncommittedConsumedMessages.Last().Offset >= _lastCommittedOffset)
                    {
                        _lastCommittedOffset = _uncommittedConsumedMessages.Last().Offset+1;
                    }

                    Console.WriteLine($"[{adminPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} result: {_uncommittedConsumedMessages.Count} {commit}");
                    if (commit == true)
                    {
                        _lastCommittedOffset = DyconitHelper.CommitStoredMessages(consumer, _uncommittedConsumedMessages, _lastCommittedOffset);
                        Console.WriteLine($"[{adminPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Committed messages");
                        Console.WriteLine($"[{adminPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} After committing lastcommittedoffset is: {_lastCommittedOffset}");
                    }
                    else
                    {
                        Console.WriteLine($"[{adminPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} No messages to commit");
                    }

                    Console.WriteLine($"[{adminPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} lastcommittedoffset: {_lastCommittedOffset}");
                    if (_lastCommittedOffset > 0)
                    {
                        Console.WriteLine($"[{adminPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Assigning topic {topic} with offset {_lastCommittedOffset}");
                        consumer.Assign(new List<TopicPartitionOffset>() { new TopicPartitionOffset(topic, 0, _lastCommittedOffset) });
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
}
