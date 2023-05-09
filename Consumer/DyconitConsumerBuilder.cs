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
                var s1 = stats["brokers"][$"{config.BootstrapServers}/1"]["rtt"]["avg"];
                var s2 = stats["brokers"][$"{config.BootstrapServers}/1"]["outbuf_msg_cnt"];
                int s3 = Convert.ToInt32(stats["msg_max"]);
                var s4 = stats["msg_size_max"];

                // turn requestlatency into an integer
                double rtt = Convert.ToDouble(s1);

                // turn it into milliseconds
                rtt = rtt / 1000;
                var min = 500 - (500 * 0.1);
                var max = 500 + (500 * 0.1);
                if (rtt < min || rtt > max)
                {
                    // _statisticsHandler?.Invoke((string)stats["name"], requestLatencyy);
                    Console.WriteLine($"rtt: {rtt}, reqAwait: {s2} - {s3} - {s4}");
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
