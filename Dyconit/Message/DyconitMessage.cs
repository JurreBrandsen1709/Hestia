using System.Collections.Generic;
using Confluent.Kafka;

namespace Dyconit.Message;

public class DyconitMessage<TKey, TValue>
{
    public TValue Value;
    public double Weight;

    public static implicit operator Message<TKey, TValue>(DyconitMessage<TKey, TValue> message)
    {
        var headers = new Headers();
        headers.Add("Weight", BitConverter.GetBytes(message.Weight));

        return new Message<TKey, TValue>
        {
            Value = message.Value,
            Headers = headers
        };
    }
}
