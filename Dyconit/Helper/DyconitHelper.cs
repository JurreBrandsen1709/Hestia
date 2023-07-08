using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using Confluent.Kafka;
using Dyconit.Consumer;
using Dyconit.Overlord;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Serilog;

namespace Dyconit.Helper
{
    public class Bounds
    {
        public double ?Staleness { get; set; }
        public double ?NumericalError { get; set; }

        internal void UpdateBounds(double? staleness, double? numericalError)
        {
            if (staleness != null)
            {
                Staleness = staleness;
            }
            if (numericalError != null)
            {
                NumericalError = numericalError;
            }
        }
    }

    public class Node
    {
        public int ?Port { get; set; }
        public int ?SyncCount { get; set; }
        public DateTime ?LastHeartbeatTime { get; set; }
        public DateTime ?LastTimeSincePull { get; set; }
        public Bounds ?Bounds { get; set; }
    }

    public class Thresholds
    {
        public int ?Throughput { get; set; }
        [JsonProperty("overhead_throughput")]
        public int ?OverheadThroughput { get; set; }
    }

    public class PolicyAction
    {
        public string ?Type { get; set; }
        public double ?Value { get; set; }
    }

    public class Rule
    {
        public string ?Condition { get; set; }
        public List<PolicyAction> ?PolicyActions { get; set; }
    }

    public class Collection
    {
        public string ?Name { get; set; }
        public List<Node> ?Nodes { get; set; }
        public Thresholds ?Thresholds { get; set; }
        public List<Rule> ?Rules { get; set; }
    }

    public class RootObject
    {
        public List<Collection> ?Collections { get; set; }
    }

        public class SyncResult
    {
        public bool changed { get; set; }
        public List<ConsumeResult<Null, string>> ?Data { get; set; }
        public double ?Weight { get; set; } = 0;

    }

    // Custom comparer to compare ConsumeResult based on timestamps
    class ConsumeResultComparer : IEqualityComparer<ConsumeResult<Null, string>?>
    {
        public bool Equals(ConsumeResult<Null, string>? x, ConsumeResult<Null, string>? y)
        {
            if (x == null && y == null)
                return true;
            if (x == null || y == null)
                return false;

            return x.Offset == y.Offset && x.Topic == y.Topic;
        }

        public int GetHashCode(ConsumeResult<Null, string>? obj)
        {
            return obj?.Offset.GetHashCode() ?? 0;
        }
    }

    public class ConsumeResultWrapper<TKey, TValue>
    {
        public ConsumeResultWrapper()
        {
            Topic = null!;
            Message = null!;
        }

        public ConsumeResultWrapper(ConsumeResult<TKey, TValue> result)
        {
            Topic = result.Topic!;
            Partition = result.Partition!.Value;
            Offset = result.Offset!.Value;
            Message = new MessageWrapper<TKey, TValue>(result.Message!);
            IsPartitionEOF = result.IsPartitionEOF;
        }

        public string? Topic { get; set; }
        public int Partition { get; set; }
        public long Offset { get; set; }
        public MessageWrapper<TKey, TValue>? Message { get; set; }
        public bool IsPartitionEOF { get; set; }
    }


    public class MessageWrapper<TKey, TValue>
    {
        public MessageWrapper()
        {
            Key = default!;
            Value = default!;
            Headers = new List<HeaderWrapper>();
        }

        public MessageWrapper(Message<TKey, TValue> message)
        {
            Key = message.Key!;
            Value = message.Value!;
            Timestamp = message.Timestamp;
            Headers = message.Headers?.Select(header => new HeaderWrapper(header.Key, header.GetValueBytes())).ToList() ?? new List<HeaderWrapper>();
        }

        public TKey? Key { get; set; }
        public TValue? Value { get; set; }
        public Timestamp Timestamp { get; set; }
        public List<HeaderWrapper> Headers { get; set; }
    }


    public class HeaderWrapper
    {
        public HeaderWrapper()
        {
            Key = null!;
            Value = null!;
        }

        public HeaderWrapper(string key, byte[] value)
        {
            Key = key;
            Value = value;
        }

        public string Key { get; set; }
        public byte[] Value { get; set; }
    }

    public class DyconitHelper
    {
        public static int FindPort()
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

        public static JObject GetConitConfiguration(string collectionName, int staleness, int numericalError)
        {
            var jsonObject = new JObject(
                new JProperty("collectionName", collectionName),
                new JProperty("Staleness", staleness),
                new JProperty("NumericalError", numericalError)
            );

            return jsonObject;
        }

        public static void PrintConitConfiguration(Dictionary<string, object> conitConfiguration)
        {
            Console.WriteLine("Created conitConfiguration with the following content:");
            foreach (KeyValuePair<string, object> kvp in conitConfiguration)
            {
                Console.WriteLine("Key = {0}, Value = {1}", kvp.Key, kvp.Value);
            }
        }

        public static IConsumer<Null, string> CreateDyconitConsumer(ConsumerConfig configuration, JToken conitConfiguration, int adminPort)
        {
            return new DyconitConsumerBuilder<Null, string>(configuration, conitConfiguration, 1, adminPort).Build();
        }

        public static double GetMessageWeight(ConsumeResult<Null, string> result)
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

        public static long CommitStoredMessages(IConsumer<Null, string> consumer, List<ConsumeResult<Null, string>> uncommittedConsumedMessages, long lastCommittedOffset)
        {
            foreach (ConsumeResult<Null, string> storedMessage in uncommittedConsumedMessages)
            {
                consumer.Commit(storedMessage);
            }

            // Retrieve the committed offsets for the assigned partitions
            var committedOffsets = consumer.Committed(TimeSpan.FromSeconds(10));

            // Process the committed offsets
            foreach (var committedOffset in committedOffsets)
            {
                lastCommittedOffset = committedOffset.Offset.Value;
            }

            return lastCommittedOffset;
        }

        public static async Task SendMessageOverTcp(string message, int port, int _listenPort)
        {
            try
            {
                using (var client = new TcpClient())
                {
                    client.Connect("localhost", port);

                    using (var stream = client.GetStream())
                    using (var writer = new StreamWriter(stream))
                    {
                        await writer.WriteLineAsync(message);
                        await writer.FlushAsync();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{_listenPort}] - {DateTime.Now.ToString("HH:mm:ss.fff")} Failed to send message over TCP: {ex.Message}");
            }
        }
    }
}
