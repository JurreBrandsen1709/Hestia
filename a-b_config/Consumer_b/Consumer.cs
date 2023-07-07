using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Dyconit.Helper;
using Dyconit.Overlord;
using Newtonsoft.Json.Linq;
using Serilog;

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
            var conitConfiguration = DyconitHelper.GetConitConfiguration(topic, topic == "topic_priority" ? 2000 : 5000, topic == "topic_priority" ? 5 : 10);

            _localCollection[topic] = conitConfiguration;

            var consumedMessages = new List<ConsumeResult<Null, string>>();
            _uncommittedConsumedMessages.Add(topic, consumedMessages);
        }

        _DyconitLogger = new DyconitAdmin(configuration.BootstrapServers, adminPort, _localCollection);

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
        long _lastCommittedOffset = -1;
        var collectionConfiguration = _localCollection[topic];

        using (var consumer = DyconitHelper.CreateDyconitConsumer(configuration, conitConfiguration, adminPort))
        {

            consumer.Subscribe(topic);
            // Introduce a delay to allow the consumer to retrieve the committed offset for the topic/partition.
            await Task.Delay(TimeSpan.FromSeconds(10));

            _ = CalculateThroughput(consumer, adminPort, token, topic);

            try
            {
                while (!token.IsCancellationRequested)
                {
                    var consumeResult = consumer.Consume(token);
                    var inputMessage = consumeResult.Message.Value;
                    _totalLocalWeight += DyconitHelper.GetMessageWeight(consumeResult);

                    Log.Information($"[{topic}] - Consumed message '{inputMessage}' at: '{consumeResult.TopicPartitionOffset}'.");
                     _lastCommittedOffset = consumeResult.Offset;

                    // if we consume a message that is older than the last committed offset, we ignore it.
                    if (consumeResult.Offset < _lastCommittedOffset)
                    {
                        continue;
                    }

                    _uncommittedConsumedMessages[topic].Add(consumeResult);

                    int waitTime = _random.Next(400, 600);
                    await Task.Delay(waitTime);

                    SyncResult result = await DyconitLogger.BoundStaleness(_uncommittedConsumedMessages[topic], topic);

                    Log.Information($"**-*[{topic}] - result: {result.Data.Count} {result.changed}");

                    _uncommittedConsumedMessages[topic] = result.Data;
                    var commit = result.changed;

                    if (_uncommittedConsumedMessages[topic].Count > 0)
                    {
                        // check if the uncommitted messages topic is the same as the topic of the consumed message
                        if (_uncommittedConsumedMessages[topic].First().TopicPartition.Topic == topic)
                        {
                            var lastConsumedOffset = _uncommittedConsumedMessages[topic].Last().Offset;
                            _lastCommittedOffset = Math.Max(_lastCommittedOffset, lastConsumedOffset + 1);
                        }
                        else
                        {
                            Log.Information($"[{topic}] - Uncommitted messages topic is not the same as the topic of the consumed message");
                        }
                    }

                    Log.Debug($"[{topic}] - lastcommittedoffset: {_lastCommittedOffset}");

                    // Console.WriteLine($"[{adminPort}] - {DateTime.Now:HH:mm:ss.fff} bounding numerical error");
                    // SyncResult result = await DyconitLogger.BoundNumericalError(_uncommittedConsumedMessages[topic], collectionConfiguration, topic, _totalLocalWeight);
                    // _uncommittedConsumedMessages[topic] = result.Data;
                    // var commit = result.changed;

                    // if (_uncommittedConsumedMessages[topic].Count > 0)
                    // {
                    //     var lastConsumedOffset = _uncommittedConsumedMessages[topic].Last().Offset;
                    //     _lastCommittedOffset = Math.Max(_lastCommittedOffset, lastConsumedOffset + 1);
                    // }

                    Log.Information($"[{topic}] - result: {_uncommittedConsumedMessages.Count} {commit}");
                    if (commit)
                    {
                        _lastCommittedOffset = DyconitHelper.CommitStoredMessages(consumer, _uncommittedConsumedMessages[topic], _lastCommittedOffset);
                        Log.Information($"[{topic}] - Committed messages");
                        Log.Information($"[{topic}] - After committing lastcommittedoffset is: {_lastCommittedOffset}");
                    }
                    else
                    {
                        Log.Information($"[{topic}] - No messages to commit");
                    }

                    Log.Information($"[{topic}] - lastcommittedoffset: {_lastCommittedOffset}");
                    if (_lastCommittedOffset > 0)
                    {
                        Log.Information($"[{topic}] - Assigning topic {topic} with offset {_lastCommittedOffset}");
                        consumer.Seek(new TopicPartitionOffset(topic, 0, _lastCommittedOffset));
                    }

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
            GroupId = "test-consumer-group-1",
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
                Log.Debug($"[{adminPort}] - No previous offset for topic {topic} partition {partition.PartitionId}");
                previousOffset = 0;
            }

            offsets.Add(topicPartition, Tuple.Create(previousOffset, DateTime.UtcNow));
            Log.Debug($"[{adminPort}] - Previous offset for topic {topic} partition {partition.PartitionId}: {previousOffset}");
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
            Log.Debug($"[{adminPort}] - Current Offset for topic {topic} partition {partition.PartitionId}: {currentOffset}");

            double consumptionRate = (currentOffset - previousOffset) / (currentTimestamp - previousTimestamp).TotalSeconds;
            topicThroughput += consumptionRate;
            Log.Debug($"[{adminPort}] - Consumption rate for topic {topic} partition {partition.PartitionId}: {consumptionRate}");
        }

        var throughputMessage = new JObject
        {
            { "eventType", "throughput" },
            { "throughput", topicThroughput },
            { "port", adminPort },
            { "topic", topic }
        };

        await DyconitHelper.SendMessageOverTcp(throughputMessage.ToString(), 6666, adminPort);
    }
}
