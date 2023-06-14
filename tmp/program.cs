
using Confluent.Kafka;
using System;
using System.Text;
using System.Text.Json.Serialization;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

public class ConsumeResultWrapper<TKey, TValue>
{
    public ConsumeResultWrapper()
    {
    }

    public ConsumeResultWrapper(ConsumeResult<TKey, TValue> result)
    {
        Topic = result.Topic;
        Partition = result.Partition.Value;
        Offset = result.Offset.Value;
        Message = new MessageWrapper<TKey, TValue>(result.Message);
        IsPartitionEOF = result.IsPartitionEOF;
    }

    public string Topic { get; set; }
    public int Partition { get; set; }
    public long Offset { get; set; }
    public MessageWrapper<TKey, TValue> Message { get; set; }
    public bool IsPartitionEOF { get; set; }
}

public class MessageWrapper<TKey, TValue>
{
    public MessageWrapper()
    {
    }

    public MessageWrapper(Message<TKey, TValue> message)
    {
        Key = message.Key;
        Value = message.Value;
        Timestamp = message.Timestamp;
        Headers = message.Headers?.Select(header => new HeaderWrapper(header.Key, header.GetValueBytes())).ToList();
    }

    public TKey Key { get; set; }
    public TValue Value { get; set; }
    public Timestamp Timestamp { get; set; }
    public List<HeaderWrapper> Headers { get; set; }
}

public class HeaderWrapper
{
    public HeaderWrapper()
    {
    }

    public HeaderWrapper(string key, byte[] value)
    {
        Key = key;
        Value = value;
    }

    public string Key { get; set; }
    public byte[] Value { get; set; }
}

class Program
{
    static void Main()
    {
        // Sample data
        List<ConsumeResult<Null, string>> _localData = new List<ConsumeResult<Null, string>>()
        {
            new ConsumeResult<Null, string>
            {
                Message = new Message<Null, string>
                {
                    Headers = new Headers
                    {
                        new Header("Weight", new byte[0])
                    },
                    Value = "x"
                }
            }
        };

        // Serialization
        JArray jArray = new JArray(_localData.Select(cr =>
        {
            var wrapper = new ConsumeResultWrapper<Null, string>(cr);
            JObject jObject = JObject.FromObject(wrapper);
            return jObject;
        }));
        string json = jArray.ToString();

        // Deserialization
        JArray deserializedArray = JArray.Parse(json);
        List<ConsumeResultWrapper<Null, string>> deserializedList = deserializedArray.Select(jObject =>
        {
            var wrapper = jObject.ToObject<ConsumeResultWrapper<Null, string>>();
            wrapper.Message = new MessageWrapper<Null, string>
            {
                Key = wrapper.Message.Key,
                Value = wrapper.Message.Value,
                Timestamp = wrapper.Message.Timestamp,
                Headers = wrapper.Message.Headers?.Select(header => new HeaderWrapper(header.Key, header.Value)).ToList()
            };
            return wrapper;
        }).ToList();

        // Accessing deserialized data
        foreach (var resultWrapper in deserializedList)
        {
            Console.WriteLine($"Topic: {resultWrapper.Topic}");
            Console.WriteLine($"Partition: {resultWrapper.Partition}");
            Console.WriteLine($"Offset: {resultWrapper.Offset}");
            Console.WriteLine($"Key: {resultWrapper.Message.Key}");
            Console.WriteLine($"Value: {resultWrapper.Message.Value}");
            Console.WriteLine($"Timestamp: {resultWrapper.Message.Timestamp}");
            Console.WriteLine($"IsPartitionEOF: {resultWrapper.IsPartitionEOF}");
            Console.WriteLine("Headers:");
            foreach (var header in resultWrapper.Message.Headers)
            {
                Console.WriteLine($"  Key: {header.Key}");
                Console.WriteLine($"  Value: {BitConverter.ToString(header.Value)}");
            }
            Console.WriteLine();
        }
    }
}
