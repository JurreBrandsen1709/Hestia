// todo check the health of the client and update this to the overlord if needed.

using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Newtonsoft.Json.Linq;
using System;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace Dyconit.Overlord
{
    public class DyconitAdmin
    {
        private readonly AdminClientConfig _adminClientConfig;
        private readonly IAdminClient _adminClient;

        private readonly int _throughputThreshold;

        // 1: consumer, 2: producer, 3: both
        private readonly int _type;

        private readonly int _listenPort;
        private DateTime _lastCheckTime;
        private TimeSpan _CheckInterval;

        private DateTime? lastConsumedTime;
        private readonly Dictionary<string, object> _conit;
        private readonly string _collection;
        private readonly int _staleness;
        private readonly int _orderError;
        private readonly int _numericalError;

        public DyconitAdmin(string bootstrapServers, int type, int listenPort, Dictionary<string, object> conit)
        {
            _type = type;
            _listenPort = listenPort;
            _conit = conit;
            _collection = conit.ContainsKey("collection") ? conit["collection"].ToString() : null;
            _staleness = conit.ContainsKey("Staleness") ? Convert.ToInt32(conit["Staleness"]) : 0;
            _orderError = conit.ContainsKey("OrderError") ? Convert.ToInt32(conit["OrderError"]) : 0;
            _numericalError = conit.ContainsKey("NumericalError") ? Convert.ToInt32(conit["NumericalError"]) : 0;


            _adminClientConfig = new AdminClientConfig
            {
                BootstrapServers = bootstrapServers
            };
            _adminClient = new AdminClientBuilder(_adminClientConfig).Build();
            // _throughputThreshold = getThroughputThreshold(); // the overlord should send this.
            // Task.Run(ListenForMessagesAsync);
        }


        private int getThroughputThreshold()
        {
            return 0;
        }

        private int setThroughputThreshold()
        {
            return 0;
        }

        // We receive the statistics. We calculate the throughput.
        // We apply what the policy says.
        // We update the bounds of every Dyconit that is affected.
        // We possibly have to modify the configuration of the consumer.
        // public void ProcessStatistics(string json, ClientConfig config)
        // {
        //     var stats = JObject.Parse(json);
        //     var s1 = stats["brokers"][$"{config.BootstrapServers}/1"]["rtt"]["avg"];

        //     Console.WriteLine($"Consumer RTT: {s1}");

        //     // TODO: Calculate throughput

        //     // TODO: Apply policy

        //     // TODO: Update bounds

        //     // TODO: Adjust configuration
        // }

        public void BoundStaleness(DateTime consumedTime)
        {
            // send message to dyconit overlord with consumedTime
            // The overlord will check if other affected nodes belonging to the same Collection of dyconit has synchronized its updates
            // with the rest of the nodes within the set stalenessBound. if not, it will direct that node to synchronize its updates with
            // this node.
        }

    }

}