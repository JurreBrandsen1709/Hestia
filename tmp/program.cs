using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Threading;

class Program
{
    static void Main()
    {
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = "localhost:9092"
        };

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "my-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        var topics = new List<string> { "topic1", "topic2" };

        // Start a producer thread to continuously produce messages to the topics
        var producerThread = new Thread(() => ProduceMessages(producerConfig, topics));
        producerThread.Start();

        // Start a consumer thread for each topic to consume messages
        var consumerThreads = new List<Thread>();
        foreach (var topic in topics)
        {
            var consumerThread = new Thread(() => ConsumeMessages(consumerConfig, topic));
            consumerThread.Start();
            consumerThreads.Add(consumerThread);
        }

        // Wait for the producer and consumer threads to complete
        producerThread.Join();
        foreach (var consumerThread in consumerThreads)
        {
            consumerThread.Join();
        }
    }

    static void ProduceMessages(ProducerConfig config, List<string> topics)
    {
        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            var rnd = new Random();
            var counter = 1;

            while (true)
            {
                var topic = topics[rnd.Next(0, topics.Count)];
                var message = $"Message {counter} - Topic: {topic}";

                var deliveryReport = producer.ProduceAsync(topic, new Message<Null, string> { Value = message }).Result;
                Console.WriteLine($"Produced message: {message} | Partition: {deliveryReport.Partition} | Offset: {deliveryReport.Offset}");

                counter++;

                Thread.Sleep(1000);
            }
        }
    }

    static void ConsumeMessages(ConsumerConfig config, string topic)
    {
        using (var consumer = new ConsumerBuilder<Null, string>(config).Build())
        {
            consumer.Subscribe(topic);

            while (true)
            {
                var consumeResult = consumer.Consume();

                Console.WriteLine($"Consumed message: {consumeResult.Message.Value} | Partition: {consumeResult.Partition} | Offset: {consumeResult.Offset}");
            }
        }
    }
}
