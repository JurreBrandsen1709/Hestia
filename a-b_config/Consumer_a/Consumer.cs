using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Dyconit.Helper;
using Dyconit.Overlord;

class Consumer
{
    private static Dictionary<string, List<ConsumeResult<Null, string>>> _uncommittedConsumedMessages = new Dictionary<string, List<ConsumeResult<Null, string>>>();
    private static Dictionary<string, Dictionary<string, object>> _localCollection = new Dictionary<string, Dictionary<string, object>>();

    static void Main()
    {
        var configuration = GetConsumerConfiguration();

        var adminPort = DyconitHelper.FindPort();

        var topics = new List<string>
        {
            "topic_priority",
            "topic_normal",
        };

        foreach (var topic in topics)
        {
            var conitConfiguration = DyconitHelper.GetConitConfiguration(topic, topic == "topic_priority" ? 2000 : 5000, 20, topic == "topic_priority" ? 5 : 10);
            DyconitHelper.PrintConitConfiguration(conitConfiguration);

            _localCollection.Add(topic, conitConfiguration);

            var consumedMessages = new List<ConsumeResult<Null, string>>();
            _uncommittedConsumedMessages.Add(topic, consumedMessages);
        }

        var DyconitLogger = new DyconitAdmin(configuration.BootstrapServers, 1, adminPort, _localCollection);

        var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };

        // Create a consumer task for each topic
        var consumerTasks = new List<Task>();
        foreach (var collection in _localCollection)
        {
            var topic = collection.Key;
            var collectionConfiguration = collection.Value;

            consumerTasks.Add(Task.Run(() => ConsumeMessages(topic, cts.Token, configuration, adminPort, DyconitLogger, collectionConfiguration)));
        }

        // Wait for all consumer tasks to complete
        Task.WaitAll(consumerTasks.ToArray());
    }

    static async Task ConsumeMessages(string topic, CancellationToken token, ConsumerConfig configuration, int adminPort, DyconitAdmin DyconitLogger, Dictionary<string, object> conitConfiguration)
    {
        long _lastCommittedOffset = -1;
        bool commit = false;
        var collectionConfiguration = _localCollection[topic];

        using (var consumer = DyconitHelper.CreateDyconitConsumer(configuration, conitConfiguration, adminPort, DyconitLogger))
        {
            consumer.Subscribe(topic);
            try
            {
                while (!token.IsCancellationRequested)
                {
                    var consumeResult = consumer.Consume(token);

                    var inputMessage = consumeResult.Message.Value;
                    Console.WriteLine($"[{topic}] - {DateTime.Now:HH:mm:ss.fff} Consumed message with value '{inputMessage}', weight '{DyconitHelper.GetMessageWeight(consumeResult)}'");
                    _lastCommittedOffset = consumeResult.Offset;

                    // if we consume a message that is older than the last committed offset, we ignore it.
                    if (consumeResult.Offset < _lastCommittedOffset)
                    {
                        Console.WriteLine($"[{topic}] - {DateTime.Now:HH:mm:ss.fff} Ignoring message with offset {consumeResult.Offset}");
                        continue;
                    }

                    _uncommittedConsumedMessages[topic].Add(consumeResult);

                    Random rnd = new Random();
                    int waitTime = rnd.Next(0, 2000);
                    await Task.Delay(waitTime);

                    var consumedTime = DateTime.Now;

                    Console.WriteLine($"[{topic}] - {DateTime.Now:HH:mm:ss.fff} bounding staleness");
                    SyncResult result = await DyconitLogger.BoundStaleness(consumedTime, _uncommittedConsumedMessages[topic], collectionConfiguration, topic);

                    _uncommittedConsumedMessages[topic] = result.Data;
                    commit = result.changed;

                    if (_uncommittedConsumedMessages[topic].Count > 0)
                    {
                        var lastConsumedOffset = _uncommittedConsumedMessages[topic].Last().Offset;
                        _lastCommittedOffset = Math.Max(_lastCommittedOffset, lastConsumedOffset + 1);
                    }


                    Console.WriteLine($"[{adminPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} bounding numerical error");
                    result = await DyconitLogger.BoundNumericalError(_uncommittedConsumedMessages[topic], collectionConfiguration, topic);
                    _uncommittedConsumedMessages[topic] = result.Data;
                    commit = commit || result.changed;

                    if (_uncommittedConsumedMessages[topic].Count > 0)
                    {
                        var lastConsumedOffset = _uncommittedConsumedMessages[topic].Last().Offset;
                        _lastCommittedOffset = Math.Max(_lastCommittedOffset, lastConsumedOffset + 1);
                    }



                    Console.WriteLine($"[{topic}] - {DateTime.Now:HH:mm:ss.fff} result: {_uncommittedConsumedMessages.Count} {commit}");
                    if (commit)
                    {
                        _lastCommittedOffset = DyconitHelper.CommitStoredMessages(consumer, _uncommittedConsumedMessages[topic], _lastCommittedOffset);
                        Console.WriteLine($"[{topic}] - {DateTime.Now:HH:mm:ss.fff} Committed messages");
                        Console.WriteLine($"[{topic}] - {DateTime.Now:HH:mm:ss.fff} After committing lastcommittedoffset is: {_lastCommittedOffset}");
                    }
                    else
                    {
                        Console.WriteLine($"[{topic}] - {DateTime.Now:HH:mm:ss.fff} No messages to commit");
                    }

                    Console.WriteLine($"[{topic}] - {DateTime.Now:HH:mm:ss.fff} lastcommittedoffset: {_lastCommittedOffset}");
                    if (_lastCommittedOffset > 0)
                    {
                        Console.WriteLine($"[{topic}] - {DateTime.Now:HH:mm:ss.fff} Assigning topic {topic} with offset {_lastCommittedOffset}");
                        consumer.Assign(new List<TopicPartitionOffset>() { new TopicPartitionOffset(topic, 0, _lastCommittedOffset) });
                    }


                }
            }
            catch (OperationCanceledException)
            {
                // Ensure the consumer leaves the group cleanly and final offsets are committed.
                consumer.Close();
            }
        }
    }

    static ConsumerConfig GetConsumerConfiguration()
    {
        return new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "test-consumer-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnablePartitionEof = true,
            StatisticsIntervalMs = 5000,
        };
    }
}
