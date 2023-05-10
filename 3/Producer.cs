using Confluent.Kafka;
using System;
using System.Diagnostics;
using System.Linq;

class Producer
{
    static void Main(string[] args)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = "localhost:9092",
            BatchNumMessages = 32768,
            CompressionType = CompressionType.None,
            QueueBufferingBackpressureThreshold = 1,
            RetryBackoffMs = 50,
            MessageSendMaxRetries = 3,
            LingerMs = 0,
            QueueBufferingMaxKbytes = 1048576,
            QueueBufferingMaxMessages = 500000,
            Acks = Acks.None,
        };


        const int numMessages = 10000;

        var elapsedTimes = new double[10];
        var throughputs = new double[10];

        for (var i = 0; i < 10; i++)
        {
            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                var stopwatch = new Stopwatch();
                stopwatch.Start();

                var message = new Message<Null, string>
                {
                    Value = new string('X', 100000)
                };

                for (var j = 0; j < numMessages; j++)
                {
                    producer.ProduceAsync("test-topic", message).Wait();
                }

                producer.Flush(TimeSpan.FromSeconds(10));

                stopwatch.Stop();

                var elapsedTime = stopwatch.Elapsed.TotalMilliseconds;
                var throughput = numMessages / stopwatch.Elapsed.TotalSeconds;

                elapsedTimes[i] = elapsedTime;
                throughputs[i] = throughput;

                Console.WriteLine($"Test {i + 1}: Elapsed time: {elapsedTime} ms, Throughput: {throughput} messages/sec");
            }
        }

        var averageElapsedTime = elapsedTimes.Average();
        var averageThroughput = throughputs.Average();

        Console.WriteLine($"Average elapsed time: {averageElapsedTime} ms");
        Console.WriteLine($"Average throughput: {averageThroughput} messages/sec");
    }
}
