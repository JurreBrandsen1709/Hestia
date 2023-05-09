using Confluent.Kafka;
using Newtonsoft.Json.Linq;
using System;

namespace Dyconit.Kafka.Consumer
{
    public class DyconitConsumerBuilder<TKey, TValue> : ConsumerBuilder<TKey, TValue>
    {
        private Action<string, double>? _statisticsHandler;

        public DyconitConsumerBuilder(ConsumerConfig config)
            : base(config)
        {
            SetStatisticsHandler((_, json) =>
            {
                var stats = JObject.Parse(json);
                var requestLatency = stats["brokers"][$"{config.BootstrapServers}/1"]["rtt"]["avg"];

                // turn requestlatency into an integer
                double requestLatencyy = Convert.ToDouble(requestLatency);

                // turn it into milliseconds
                requestLatencyy = requestLatencyy / 1000;
                var min = 500 - (500 * 0.1);
                var max = 500 + (500 * 0.1);
                if (requestLatencyy < min || requestLatencyy > max)
                {
                    // _statisticsHandler?.Invoke((string)stats["name"], requestLatencyy);
                    Console.WriteLine($"T: {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")} - {requestLatencyy}");
                }
            });
        }

        public new DyconitConsumerBuilder<TKey, TValue> SetStatisticsHandler(Action<string, double> handler)
        {
            _statisticsHandler = handler;
            return this;
        }
    }
}
