using Confluent.Kafka;
using Dyconit.Overlord;
using Newtonsoft.Json.Linq;
using System;
using System.IO;

namespace Dyconit.Consumer
{
    public class DyconitConsumerBuilder<TKey, TValue> : ConsumerBuilder<TKey, TValue>
    {
        private Action<string, double>? _statisticsHandler;
        private readonly DyconitOverlord _adminClient;

        public DyconitConsumerBuilder(ConsumerConfig config, DyconitOverlord adminClient) : base(config)
        {

            _adminClient = adminClient;

            SetStatisticsHandler((_, json) =>
            {
                _adminClient.ProcessConsumerStatistics(json, config);
            });
        }

        public DyconitConsumerBuilder<TKey, TValue> SetStatisticsHandler(Action<string, double> handler)
        {
            _statisticsHandler = handler;
            return this;
        }
    }
}
