using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using Confluent.Kafka;
using Dyconit.Consumer;
using Dyconit.Overlord;

namespace Dyconit.Helper
{
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

        public static Dictionary<string, object> GetConitConfiguration(string collection, int staleness, int orderError, int numericalError)
        {
            return new Dictionary<string, object>
            {
                { "collection", collection },
                { "Staleness", staleness },
                { "OrderError", orderError },
                { "NumericalError", numericalError }
            };
        }

        public static void PrintConitConfiguration(Dictionary<string, object> conitConfiguration)
        {
            Console.WriteLine("Created conitConfiguration with the following content:");
            foreach (KeyValuePair<string, object> kvp in conitConfiguration)
            {
                Console.WriteLine("Key = {0}, Value = {1}", kvp.Key, kvp.Value);
            }
        }

        public static IConsumer<Null, string> CreateDyconitConsumer(ConsumerConfig configuration, Dictionary<string, object> conitConfiguration, int adminPort, DyconitAdmin dyconitLogger)
        {
            return new DyconitConsumerBuilder<Null, string>(configuration, conitConfiguration, 1, adminPort, dyconitLogger).Build();
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
