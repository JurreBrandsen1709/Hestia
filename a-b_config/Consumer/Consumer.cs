using Confluent.Kafka;
using System;
using System.Threading;
using Microsoft.Extensions.Configuration;
using System.IO;
using Newtonsoft.Json.Linq;
using Dyconit.Consumer;
using Dyconit.Overlord;
using System.Collections.Generic;

class Consumer {
    static void Main(string[] args)
    {
        var configuration = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "kafka-dotnet-getting-started",
            EnableAutoCommit = true,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            StatisticsIntervalMs = 2000,
        };

        var adminClient = new DyconitOverlord("localhost:9092", 100000);
        const string topic = "TestTopiccc";

        CancellationTokenSource cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };

        var lastProcessedOffsets = new Dictionary<TopicPartition, long>();

        using (var consumer = new DyconitConsumerBuilder<string, byte[]>(configuration, adminClient, 1337)
            .SetKeyDeserializer(Deserializers.Utf8)
            .SetValueDeserializer(Deserializers.ByteArray)
            .Build())
            {
                consumer.Subscribe(topic);

                try
                {
                    while (true)
                    {
                        var consumeResult = consumer.Consume(cts.Token);
                        var offset = consumeResult.TopicPartitionOffset;

                        if (lastProcessedOffsets.TryGetValue(offset.TopicPartition, out var lastProcessedOffset))
                        {
                            if (offset.Offset != lastProcessedOffset + 1)
                            {
                                Console.WriteLine($"Out-of-order message! Expected offset: {lastProcessedOffset + 1}, actual offset: {offset.Offset}");
                            }
                        }

                        var message = consumeResult.Message;
                        Console.WriteLine($"Received message at {message.Timestamp.UtcDateTime}:\n\t{message.Value}");


                        lastProcessedOffsets[offset.TopicPartition] = offset.Offset;
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