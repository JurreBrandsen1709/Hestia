using Confluent.Kafka;
using Dyconit.Overlord;
using Newtonsoft.Json.Linq;
using System;
using System.IO;
using System.Net;
using System.Net.Sockets;

namespace Dyconit.Consumer
{
    public class DyconitConsumerBuilder<TKey, TValue> : ConsumerBuilder<TKey, TValue>
    {
        private Action<string, double>? _statisticsHandler;
        private readonly DyconitOverlord _adminClient;

        private readonly int _listenPort;

        public DyconitConsumerBuilder(ConsumerConfig config, DyconitOverlord adminClient, int listenPort) : base(config)
        {

            _adminClient = adminClient;
            _listenPort = listenPort;

            SetStatisticsHandler((_, json) =>
            {
                _adminClient.ProcessConsumerStatistics(json, config);
            });

            Task.Run(ListenForMessagesAsync);
        }

        private async Task ListenForMessagesAsync() {
            var listener = new TcpListener(IPAddress.Any, _listenPort);
            listener.Start();

            while (true) {
                var client = await listener.AcceptTcpClientAsync().ConfigureAwait(false);
                var reader = new StreamReader(client.GetStream());
                var message = await reader.ReadToEndAsync().ConfigureAwait(false);

                // Check if message meets the threshold for important information
                if (message.Length > 0) {
                    Console.WriteLine($"Received message: {message}");
                }
            }
        }

        public DyconitConsumerBuilder<TKey, TValue> SetStatisticsHandler(Action<string, double> handler)
        {
            _statisticsHandler = handler;
            return this;
        }
    }
}
