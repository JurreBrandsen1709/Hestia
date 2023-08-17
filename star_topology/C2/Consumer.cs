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
using System.Diagnostics;
using System.Collections.Concurrent;

class Consumer
{
    private static Dictionary<string, List<ConsumeResult<Null, string>>> _uncommittedConsumedMessages = new Dictionary<string, List<ConsumeResult<Null, string>>>();
    private static JObject _localCollection = new JObject();
    public static int adminPort = DyconitHelper.FindPort();
    public static DyconitAdmin _DyconitLogger;
    private static Random _random = new Random();
    private static Dictionary<string, double> _totalWeight = new Dictionary<string, double>();
    private static Dictionary<string, double> _currentOffset = new Dictionary<string, double>();
    private static Dictionary<string, int> _consumerCount = new Dictionary<string, int>();
    private static Process _currentProcess;
    private static Dictionary<int, int> _delay = new Dictionary<int, int>();
    private static ConcurrentDictionary<string, ConsumeInfo> _consumeInfos = new ConcurrentDictionary<string, ConsumeInfo>();

    static async Task Main()
    {
        var configuration = GetConsumerConfiguration();

        // Create a PerformanceCounter to monitor CPU usage
        _currentProcess = Process.GetCurrentProcess();
        _delay = DyconitHelper.LoadDictionary("faulty_consumer.json");

        var topics = new List<string>
        {
            "topic_priority",
            "topic_normal",
        };

        foreach (var topic in topics)
        {
            var conitConfiguration = DyconitHelper.GetConitConfiguration(topic, topic == "topic_priority" ? 10000 : 15000, topic == "topic_priority" ? 10 : 25);

            _localCollection[topic] = conitConfiguration;

            var consumedMessages = new List<ConsumeResult<Null, string>>();
            _uncommittedConsumedMessages.Add(topic, consumedMessages);

            _currentOffset.Add(topic, 0);
            _consumerCount.Add(topic, 0);
            _totalWeight.Add(topic, 0);
            _consumeInfos[topic] = new ConsumeInfo { Time = DateTime.UtcNow, Count = 0 };
        }

        _DyconitLogger = new DyconitAdmin(configuration, adminPort, _localCollection, "app3");

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

        using (var consumer = DyconitHelper.CreateDyconitConsumer(configuration, conitConfiguration, adminPort, "app3"))
        {
            consumer.Subscribe(topic);

            // Introduce a delay to allow the consumer to retrieve the committed offset for the topic/partition.
            await Task.Delay(TimeSpan.FromSeconds(10));

            _ = CalculateThroughput(consumer, adminPort, token, topic);

            try
            {
                while (!token.IsCancellationRequested)
                {
                    double cpuUsage = _currentProcess.TotalProcessorTime.Ticks / (float)Stopwatch.Frequency * 100;
                    Log.Debug($"port: {adminPort} - CPU Utilization: {cpuUsage}%");

                    var consumeResult = consumer.Consume(token);

                    // check if we have consumed a message. Retry up to 10 time if we have not consumed a message then we exit.
                    if (consumeResult != null && consumeResult.Message != null && consumeResult.Message.Value != null)
                    {
                        var inputMessage = consumeResult.Message.Value;
                        _consumerCount[topic] += 1;

                        // increase the local consume count by taking the current consume count and adding 1.
                        _consumeInfos[topic] = new ConsumeInfo { Time = DateTime.UtcNow, Count = _consumeInfos[topic].Count + 1 };

                        var delay = _delay[_consumeInfos[topic].Count];

                        // add random delay to simulate processing time
                        if (topic == "topic_priority")
                        {
                            await Task.Delay(delay);
                        }
                        else
                        {
                            await Task.Delay(delay);
                        }
                    }
                    else
                    {
                        Log.Warning($"===================== Currently no message in topic {topic} =====================");

                        // // send message to the overlord to indicate that we are finished consuming messages
                        // DyconitHelper.SendFinishedMessage(adminPort, topic, _uncommittedConsumedMessages[topic]);

                        // // commit the last consumed message
                        // DyconitHelper.CommitStoredMessages(consumer, _uncommittedConsumedMessages[topic], _lastCommittedOffset);

                        continue;
                    }


                    Log.Information($"========================= Topic: {topic} - consumer count {_consumerCount[topic]} =========================");

                    _totalWeight[topic] += 1.0;
                    _lastCommittedOffset = consumeResult.Offset;
                    _currentOffset[topic] += 1;

                    // if we consume a message that is older than the last committed offset, we ignore it.
                    if (consumeResult.Offset < _lastCommittedOffset)
                    {
                        continue;
                    }

                    _uncommittedConsumedMessages[topic].Add(consumeResult);

                    SyncResult result = await DyconitLogger.BoundStaleness(_uncommittedConsumedMessages[topic], topic);
                    _uncommittedConsumedMessages[topic] = result.Data;
                    var commit = result.changed;

                    if (_uncommittedConsumedMessages[topic].Count > 0)
                    {
                        var lastConsumedOffset = _uncommittedConsumedMessages[topic].Last().Offset;
                        _currentOffset[topic] += 1;
                        _consumerCount[topic] = _uncommittedConsumedMessages[topic].Count;
                        _lastCommittedOffset = Math.Max(_lastCommittedOffset, lastConsumedOffset + 1);
                    }

                    bool boundResult = await DyconitLogger.BoundNumericalError(_uncommittedConsumedMessages[topic], topic, _totalWeight[topic]);
                    commit = boundResult || commit;

                    _uncommittedConsumedMessages[topic] = result.Data;

                    if (_uncommittedConsumedMessages[topic].Count > 0)
                    {
                        var lastConsumedOffset = _uncommittedConsumedMessages[topic].Last().Offset;
                        _currentOffset[topic] += 1;
                        _consumerCount[topic] = _uncommittedConsumedMessages[topic].Count;
                        _lastCommittedOffset = Math.Max(_lastCommittedOffset, lastConsumedOffset + 1);
                    }

                    if (commit)
                    {
                        _lastCommittedOffset = DyconitHelper.CommitStoredMessages(consumer, _uncommittedConsumedMessages[topic], _lastCommittedOffset);
                        _totalWeight[topic] = 0.0;
                    }

                    if (_lastCommittedOffset > 0)
                    {
                        consumer.Assign(new List<TopicPartitionOffset>() { new TopicPartitionOffset(topic, 0, _lastCommittedOffset) });
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
            BootstrapServers = "broker:9092",
            GroupId = "c2",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true,
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
        if (consumer == null || !_consumeInfos.ContainsKey(topic))
        {
            return;
        }

        var previousConsumeInfo = new ConsumeInfo { Time = DateTime.Now, Count = _consumeInfos[topic].Count };
        await Task.Delay(TimeSpan.FromSeconds(5));

        if (_consumeInfos.TryGetValue(topic, out var currentConsumeInfo))
        {
            double topicThroughput = (currentConsumeInfo.Count - previousConsumeInfo.Count) / (DateTime.UtcNow - previousConsumeInfo.Time).TotalSeconds;

            Log.Information($"Topic {topic} message throughput: {topicThroughput} messages/s");

            var throughputMessage = new JObject
            {
                { "eventType", "throughput" },
                { "throughput", topicThroughput },
                { "port", adminPort },
                { "collectionName", topic }
            };

            await DyconitHelper.SendMessageOverTcp(throughputMessage.ToString(), 6666, adminPort);
        }
    }
}
