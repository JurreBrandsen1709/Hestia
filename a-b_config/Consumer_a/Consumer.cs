using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Dyconit.Helper;
using Dyconit.Overlord;
using Newtonsoft.Json.Linq;

class Consumer
{
    private static Dictionary<string, List<ConsumeResult<Null, string>>> _uncommittedConsumedMessages = new Dictionary<string, List<ConsumeResult<Null, string>>>();
    // private static Dictionary<string, Dictionary<string, object>> _localCollection = new Dictionary<string, Dictionary<string, object>>();
    private static JObject _localCollection = new JObject();

    public static int adminPort = DyconitHelper.FindPort();
    public static DyconitAdmin _DyconitLogger;
    private static Random _random = new Random();
    private static double _totalLocalWeight = 0.0;

    static async Task Main()
    {
        var configuration = GetConsumerConfiguration();

        var topics = new List<string>
        {
            "topic_priority",
            "topic_normal",
        };

        foreach (var topic in topics)
        {
            var conitConfiguration = DyconitHelper.GetConitConfiguration(topic, topic == "topic_priority" ? 200 : 500, topic == "topic_priority" ? 5 : 10);

            _localCollection[topic] = conitConfiguration;

            var consumedMessages = new List<ConsumeResult<Null, string>>();
            _uncommittedConsumedMessages.Add(topic, consumedMessages);
        }

        _DyconitLogger = new DyconitAdmin(configuration.BootstrapServers, 1, adminPort, _localCollection);

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

            consumerTasks.Add(ConsumeMessages(topic, cts.Token, configuration, adminPort, _DyconitLogger, collectionConfiguration));
        }

        // Wait for all consumer tasks to complete
        await Task.WhenAll(consumerTasks);
    }

    static async Task ConsumeMessages(string topic, CancellationToken token, ConsumerConfig configuration, int adminPort, DyconitAdmin DyconitLogger, JToken conitConfiguration)
    {
        // long _lastCommittedOffset = -1;
        var collectionConfiguration = _localCollection[topic];

        using (var consumer = DyconitHelper.CreateDyconitConsumer(configuration, conitConfiguration, adminPort))
        {

            consumer.Subscribe(topic);
            // Introduce a delay to allow the consumer to retrieve the committed offset for the topic/partition.
            // await Task.Delay(TimeSpan.FromSeconds(10));

            // _ = CalculateThroughput(consumer, adminPort, token, topic);

            try
            {
                while (!token.IsCancellationRequested)
                {
                    var consumeResult = consumer.Consume(token);
                    var inputMessage = consumeResult.Message.Value;
                    // _totalLocalWeight += DyconitHelper.GetMessageWeight(consumeResult);


                    Console.WriteLine($"[{topic}] - {DateTime.Now:HH:mm:ss.fff} Consumed message with value '{inputMessage}', weight '{DyconitHelper.GetMessageWeight(consumeResult)}'");
                    // _lastCommittedOffset = consumeResult.Offset;

                    // // if we consume a message that is older than the last committed offset, we ignore it.
                    // if (consumeResult.Offset < _lastCommittedOffset)
                    // {
                    //     Console.WriteLine($"[{topic}] - {DateTime.Now:HH:mm:ss.fff} Ignoring message with offset {consumeResult.Offset}");
                    //     continue;
                    // }

                    // _uncommittedConsumedMessages[topic].Add(consumeResult);

                    int waitTime = _random.Next(400, 600);
                    await Task.Delay(waitTime);

                    // var consumedTime = DateTime.Now;

                    // Console.WriteLine($"[{topic}] - {DateTime.Now:HH:mm:ss.fff} bounding staleness");
                    // SyncResult result = await DyconitLogger.BoundStaleness(consumedTime, _uncommittedConsumedMessages[topic], collectionConfiguration, topic);

                    // _uncommittedConsumedMessages[topic] = result.Data;
                    // var commit = result.changed;

                    // if (_uncommittedConsumedMessages[topic].Count > 0)
                    // {
                    //     var lastConsumedOffset = _uncommittedConsumedMessages[topic].Last().Offset;
                    //     _lastCommittedOffset = Math.Max(_lastCommittedOffset, lastConsumedOffset + 1);
                    // }

                    // Console.WriteLine($"[{adminPort}] - {DateTime.Now:HH:mm:ss.fff} bounding numerical error");
                    // SyncResult result = await DyconitLogger.BoundNumericalError(_uncommittedConsumedMessages[topic], collectionConfiguration, topic, _totalLocalWeight);
                    // _uncommittedConsumedMessages[topic] = result.Data;
                    // var commit = result.changed;

                    // if (_uncommittedConsumedMessages[topic].Count > 0)
                    // {
                    //     var lastConsumedOffset = _uncommittedConsumedMessages[topic].Last().Offset;
                    //     _lastCommittedOffset = Math.Max(_lastCommittedOffset, lastConsumedOffset + 1);
                    // }

                    // Console.WriteLine($"[{topic}] - {DateTime.Now:HH:mm:ss.fff} result: {_uncommittedConsumedMessages.Count} {commit}");
                    // if (commit)
                    // {
                    //     _lastCommittedOffset = DyconitHelper.CommitStoredMessages(consumer, _uncommittedConsumedMessages[topic], _lastCommittedOffset);
                    //     Console.WriteLine($"[{topic}] - {DateTime.Now:HH:mm:ss.fff} Committed messages");
                    //     Console.WriteLine($"[{topic}] - {DateTime.Now:HH:mm:ss.fff} After committing lastcommittedoffset is: {_lastCommittedOffset}");
                    // }
                    // else
                    // {
                    //     Console.WriteLine($"[{topic}] - {DateTime.Now:HH:mm:ss.fff} No messages to commit");
                    // }

                    // Console.WriteLine($"[{topic}] - {DateTime.Now:HH:mm:ss.fff} lastcommittedoffset: {_lastCommittedOffset}");
                    // if (_lastCommittedOffset > 0)
                    // {
                    //     Console.WriteLine($"[{topic}] - {DateTime.Now:HH:mm:ss.fff} Assigning topic {topic} with offset {_lastCommittedOffset}");
                    //     consumer.Assign(new List<TopicPartitionOffset>() { new TopicPartitionOffset(topic, 0, _lastCommittedOffset) });
                    // }
                }
            }
            catch (OperationCanceledException)
            {
                // Ensure the consumer leaves the group cleanly and final offsets are committed.
                consumer.Close();
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
            GroupId = "test-consumer-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnablePartitionEof = true,
            StatisticsIntervalMs = 5000,
        };
    }

    private static async Task CalculateThroughput(IConsumer<Null, string> consumer, int adminPort, CancellationToken token, string topic)
    {
        while (!token.IsCancellationRequested)
        {
            var startTime = DateTime.UtcNow;

            await CalculateThroughputAsync(consumer, topic);

            var endTime = DateTime.UtcNow;
            var elapsed = endTime - startTime;
            var delay = TimeSpan.FromSeconds(10) - elapsed;

            if (delay > TimeSpan.Zero)
            {
                await Task.Delay(delay);
            }
        }
    }

    private static async Task CalculateThroughputAsync(IConsumer<Null, string> consumer, string topic)
    {
        if (consumer == null)
        {
            return;
        }

        var offsets = new Dictionary<TopicPartition, Tuple<Offset, DateTime>>();

        // get all partitions for the topic
        var partitions = _DyconitLogger._adminClient.GetMetadata(TimeSpan.FromSeconds(20)).Topics.First(t => t.Topic == topic).Partitions;
        foreach (var partition in partitions)
        {
            var topicPartition = new TopicPartition(topic, partition.PartitionId);
            Offset previousOffset = consumer.Position(topicPartition);

            if (previousOffset == Offset.Unset)
            {
                Console.WriteLine($"====================[{adminPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} No previous offset for topic {topic} partition {partition.PartitionId}");
                previousOffset = 0;
            }

            offsets.Add(topicPartition, Tuple.Create(previousOffset, DateTime.UtcNow));
            Console.WriteLine($"~~~~~[{adminPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Previous offset for topic {topic} partition {partition.PartitionId}: {previousOffset}");
        }

        await Task.Delay(TimeSpan.FromSeconds(5));

        double topicThroughput = 0.0;

        foreach (var partition in partitions)
        {
            var topicPartition = new TopicPartition(topic, partition.PartitionId);

            var previousValues = offsets[topicPartition];
            long previousOffset = previousValues.Item1;
            DateTime previousTimestamp = previousValues.Item2;

            long currentOffset = consumer.Position(topicPartition);
            if (currentOffset == Offset.Unset)
            {
                currentOffset = 0;
            }
            DateTime currentTimestamp = DateTime.UtcNow;
            Console.WriteLine($"~~~~~[{adminPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Current Offset for topic {topic} partition {partition.PartitionId}: {currentOffset}");

            double consumptionRate = (currentOffset - previousOffset) / (currentTimestamp - previousTimestamp).TotalSeconds;
            topicThroughput += consumptionRate;
            Console.WriteLine($"~~~~~[{adminPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Consumption rate for topic {topic} partition {partition.PartitionId}: {consumptionRate}");
        }

        var throughputMessage = new JObject
        {
            { "eventType", "throughput" },
            { "throughput", topicThroughput },
            { "adminPort", adminPort },
            { "topic", topic }
        };

        await DyconitHelper.SendMessageOverTcp(throughputMessage.ToString(), 6666, adminPort);
    }
}
