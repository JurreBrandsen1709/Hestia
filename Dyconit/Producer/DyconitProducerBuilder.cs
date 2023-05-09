using Confluent.Kafka;
using Dyconit.Overlord;
using Newtonsoft.Json.Linq;
using System;
using System.IO;

namespace Dyconit.Producer
{
    public class DyconitProducerBuilder<TKey, TValue> : ProducerBuilder<TKey, TValue>
    {
        private Action<string, double>? _statisticsHandler;
        private readonly DyconitOverlord _adminClient;

        public DyconitProducerBuilder(ProducerConfig config, DyconitOverlord adminClient) : base(config)
        {

            _adminClient = adminClient;

            SetStatisticsHandler((_, json) =>
            {
                _adminClient.ProcessProducerStatistics(json, config);
            });
        }

        public DyconitProducerBuilder<TKey, TValue> SetStatisticsHandler(Action<string, double> handler)
        {
            _statisticsHandler = handler;
            return this;
        }
    }
}
