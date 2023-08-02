// TODO: Toevoegen latency van berichten (events en dyconit events)
// TODO: Toevoegen wat de bounds zijn elk moment een bericht wordt consumed.
// Ik heb het namelijk over latency in de NFRs. Dus daar moet ik eigenlijk ook op testen.
// Wat ik voor me zie is een coole grafiek waarbij je de bounds hebt en dan daaronder de werkelijke latency.

using Newtonsoft.Json.Linq;
using Dyconit.Helper;
using Newtonsoft.Json;
using Confluent.Kafka;
using System.Net.Sockets;
using System.Net;
using Serilog;
using Serilog.Events;
using Confluent.Kafka.Admin;
using System.Collections.Concurrent;

namespace Dyconit.Overlord
{
    public class DyconitAdmin
    {
        public readonly IAdminClient _adminClient;
        private readonly int _listenPort;
        private readonly string _host;
        private readonly AdminClientConfig _adminClientConfig;
        private readonly ProducerConfig _producerConfig;
        private readonly ConsumerConfig _consumerConfig;
        private IProducer<Null, string> _producer;
        private RootObject _dyconitCollections;
        private Dictionary <string, List<ConsumeResult<Null, string>>> _localData = new Dictionary<string, List<ConsumeResult<Null, string>>>();
        // private List<ConsumeResult<Null, string>> ?_localData = new List<ConsumeResult<Null, string>>();
        private List<ConsumeResult<Null, string>> ?_receivedData;
        private bool _synced;
        private HashSet<int> _syncResponses = new HashSet<int>();
        private Dictionary<string, List<ConsumeResult<Null, string>>> _buffer = new Dictionary<string, List<ConsumeResult<Null, string>>>();
        private Dictionary<string, List<string>> _receivedChunks = new Dictionary<string, List<string>>();


        public DyconitAdmin(ConsumerConfig consumerConfig, int adminPort, JObject conitCollection, string host)
        {
            ConfigureLogging();

            _listenPort = adminPort;
            _host = host;
            _consumerConfig = consumerConfig;
            _adminClientConfig = new AdminClientConfig
            {
                BootstrapServers = _consumerConfig.BootstrapServers,
            };
            _adminClient = new AdminClientBuilder(_adminClientConfig).Build();
            _adminClient.CreateTopicsAsync(new List<TopicSpecification> { new TopicSpecification { Name = "trans_topic_priority", NumPartitions = 1, ReplicationFactor = 1 } });
            _adminClient.CreateTopicsAsync(new List<TopicSpecification> { new TopicSpecification { Name = "trans_topic_normal", NumPartitions = 1, ReplicationFactor = 1 } });
            _adminClient.CreateTopicsAsync(new List<TopicSpecification> { new TopicSpecification { Name = "topic_normal", NumPartitions = 1, ReplicationFactor = 1 } });
            _adminClient.CreateTopicsAsync(new List<TopicSpecification> { new TopicSpecification { Name = "topic_priority", NumPartitions = 1, ReplicationFactor = 1 } });

            // create the special topics by looping over the hosts and creating syncRequests and syncResponses
            _adminClient.CreateTopicsAsync(new List<TopicSpecification> { new TopicSpecification { Name = $"syncRequest_{_host}", NumPartitions = 1, ReplicationFactor = 1 } });
            _adminClient.CreateTopicsAsync(new List<TopicSpecification> { new TopicSpecification { Name = $"syncResponse_{_host}", NumPartitions = 1, ReplicationFactor = 1 } });
            _dyconitCollections = CreateDyconitCollections(conitCollection, adminPort, host);

            // create two keys for the _localData dictionary
            _localData.Add("topic_normal", new List<ConsumeResult<Null, string>>());
            _localData.Add("topic_priority", new List<ConsumeResult<Null, string>>());

            // create a procuder for the admin
            _producerConfig = new ProducerConfig { BootstrapServers = _consumerConfig.BootstrapServers };
            _producer = new ProducerBuilder<Null, string>(_producerConfig).Build();

            ListenForMessagesAsync();
        }

        async Task ConsumeSyncRequest(string topic, ConsumerConfig configuration)
        {
            configuration.GroupId = $"syncRequest_{_host}";
            // increase message max bytes to max it can do.
            using (var consumer = new ConsumerBuilder<Ignore, string>(configuration).Build())
            {
                consumer.Subscribe(topic);
                try
                {
                    while (true)
                    {
                        Log.Information($"Listening for sync requests on {topic}");

                        var consumeResult = consumer.Consume();
                        if (consumeResult != null && consumeResult.Message != null && consumeResult.Message.Value != null)
                        {
                            // the message is 'topic;host' extract that.
                            var messageParts = consumeResult.Message.Value.Split(';');
                            if (messageParts.Length == 2)
                            {
                                var messageTopic = messageParts[0];
                                var messageHost = messageParts[1];

                                Log.Information($"Received sync request from {messageHost} for topic {messageTopic}");

                                // Sync Request. I should send a sync response.
                                List<ConsumeResultWrapper<Null, string>> wrapperList = _localData[messageTopic]!.Select(cr => new ConsumeResultWrapper<Null, string>(cr)).ToList();
                                string consumeResultWrapped = JsonConvert.SerializeObject(wrapperList, Formatting.Indented);

                                // send the consumeResultwrapped and the host to the syncResponse topic. Also send your own host as a message.
                                await _producer.ProduceAsync($"syncResponse_{messageHost}", new Message<Null, string> { Value = $"{_host};{consumeResultWrapped}" });

                                Log.Information($"Sent sync response to {messageHost} for topic {messageTopic}");

                                _synced = true;

                                // increment the sync for the port and colleciton combination
                                var collection = _dyconitCollections.Collections?.FirstOrDefault(c => c.Name == messageTopic);
                                var node = collection?.Nodes?.FirstOrDefault(n => n.Port == _listenPort);
                                if (node != null)
                                {
                                    node.SyncCount++;
                                }
                            }
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Ctrl+C was pressed.
                }
                finally
                {
                    consumer.Close();
                }
            }
        }

        async Task ConsumeSyncResponse(string topic, ConsumerConfig configuration)
        {
            await Task.Run(() =>
            {
                configuration.GroupId = $"syncRequest_{_host}";
                using (var consumer = new ConsumerBuilder<Ignore, string>(configuration).Build())
                {
                    consumer.Subscribe(topic);
                    try
                    {
                        while (true)
                        {
                            Log.Information($"Listening for sync requests on {topic}");

                            var consumeResult = consumer.Consume();
                            if (consumeResult != null && consumeResult.Message != null && consumeResult.Message.Value != null)
                            {
                                // the message is 'data;host' extract that.
                                var messageParts = consumeResult.Message.Value.Split(';');
                                var messageHost = messageParts[0];

                                // The remaining part is the data
                                var messageData = string.Join(';', messageParts.Skip(1));

                                Log.Information($"Received sync response from {messageHost}");

                                List<ConsumeResultWrapper<Null, string>> deserializedList = JsonConvert.DeserializeObject<List<ConsumeResultWrapper<Null, string>>>(messageData);
                                if (deserializedList != null)
                                {
                                    // Convert back to original format
                                    _receivedData = deserializedList.Select(dw => new ConsumeResult<Null, string>
                                    {
                                        Topic = dw.Topic ?? string.Empty, // Handle possible null value
                                        Partition = dw.Partition,
                                        Offset = dw.Offset,
                                        Message = new Message<Null, string>
                                        {
                                            Key = dw.Message?.Key, // Handle possible null value
                                            Value = dw.Message?.Value ?? string.Empty, // Handle possible null value
                                            Timestamp = dw.Message?.Timestamp ?? default, // Handle possible null value
                                            Headers = dw.Message?.Headers != null ? ConvertToKafkaHeaders(dw.Message.Headers) : null
                                        },
                                        IsPartitionEOF = dw.IsPartitionEOF
                                    }).ToList();

                                    var receivedTopic = _receivedData.FirstOrDefault()?.Topic;

                                    Log.Information($"Received data count: {_receivedData.Count()} - collection name: {receivedTopic} - sender host: {messageHost}");

                                    // update the last time since pull for the sender port and collection combination
                                    var collection = _dyconitCollections.Collections
                                        ?.FirstOrDefault(c => c.Name == receivedTopic);
                                    if (collection != null)
                                    {
                                        var sender = collection.Nodes
                                            ?.FirstOrDefault(s => s.Host == messageHost);
                                        if (sender != null)
                                        {
                                            sender.LastTimeSincePull = DateTime.Now;
                                        }
                                    }

                                    // update the syn counter for the _listenPort and collection combination
                                    var node = _dyconitCollections.Collections
                                        ?.FirstOrDefault(c => c.Name == receivedTopic)
                                        ?.Nodes
                                        ?.FirstOrDefault(n => n.Port == _listenPort);

                                    if (node != null)
                                    {
                                        node.SyncCount++;
                                    }
                                }
                                else
                                {
                                    Log.Warning($"Invalid message data: {messageData}");
                                }
                            }
                            else
                            {
                                Log.Warning("Received null or invalid message.");
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // Ctrl+C was pressed.
                    }
                    finally
                    {
                        consumer.Close();
                    }
                }
            });
        }


        // async Task ConsumeSyncResponse(string topic, ConsumerConfig configuration)
        // {
        //     await Task.Run(() =>
        //     {
        //         configuration.GroupId = $"syncRequest_{_host}";
        //         configuration.MessageMaxBytes = 1000000000;
        //         using (var consumer = new ConsumerBuilder<Ignore, string>(configuration).Build())
        //         {
        //             consumer.Subscribe(topic);
        //             try
        //             {
        //                 while (true)
        //                 {
        //                     Log.Information($"Listening for sync requests on {topic}");

        //                     var consumeResult = consumer.Consume();
        //                     if (consumeResult != null && consumeResult.Message != null && consumeResult.Message.Value != null)
        //                     {
        //                         // the message is 'data;host' extract that.
        //                         var messageParts = consumeResult.Message.Value.Split(';');
        //                         var messageHost = messageParts[0];

        //                         // The remaining part is the data
        //                         var messageData = string.Join(';', messageParts.Skip(1));

        //                         Log.Information($"Received sync response from {messageHost}");

        //                         List<ConsumeResultWrapper<Null, string>> deserializedList = JsonConvert.DeserializeObject<List<ConsumeResultWrapper<Null, string>>>(messageData);
        //                         if (deserializedList != null)
        //                         {
        //                             // Convert back to original format
        //                             _receivedData = deserializedList.Select(dw => new ConsumeResult<Null, string>
        //                             {
        //                                 Topic = dw.Topic ?? string.Empty, // Handle possible null value
        //                                 Partition = dw.Partition,
        //                                 Offset = dw.Offset,
        //                                 Message = new Message<Null, string>
        //                                 {
        //                                     Key = dw.Message?.Key, // Handle possible null value
        //                                     Value = dw.Message?.Value ?? string.Empty, // Handle possible null value
        //                                     Timestamp = dw.Message?.Timestamp ?? default, // Handle possible null value
        //                                     Headers = dw.Message?.Headers != null ? ConvertToKafkaHeaders(dw.Message.Headers) : null
        //                                 },
        //                                 IsPartitionEOF = dw.IsPartitionEOF
        //                             }).ToList();

        //                             var receivedTopic = _receivedData.FirstOrDefault()?.Topic;

        //                             Log.Information($"Received data count: {_receivedData.Count()} - collection name: {receivedTopic} - sender host: {messageHost}");

        //                             // update the last time since pull for the sender port and collection combination
        //                             var collection = _dyconitCollections.Collections
        //                                 ?.FirstOrDefault(c => c.Name == receivedTopic);
        //                             if (collection != null)
        //                             {
        //                                 var sender = collection.Nodes
        //                                     ?.FirstOrDefault(s => s.Host == messageHost);
        //                                 if (sender != null)
        //                                 {
        //                                     sender.LastTimeSincePull = DateTime.Now;
        //                                 }
        //                             }

        //                             // update the syn counter for the _listenPort and collection combination
        //                             var node = _dyconitCollections.Collections
        //                                 ?.FirstOrDefault(c => c.Name == receivedTopic)
        //                                 ?.Nodes
        //                                 ?.FirstOrDefault(n => n.Port == _listenPort);

        //                             if (node != null)
        //                             {
        //                                 node.SyncCount++;
        //                             }
        //                         }
        //                         else
        //                         {
        //                             Log.Warning($"Invalid message data: {messageData}");
        //                         }
        //                     }
        //                     else
        //                     {
        //                         Log.Warning("Received null or invalid message.");
        //                     }
        //                 }
        //             }
        //             catch (OperationCanceledException)
        //             {
        //                 // Ctrl+C was pressed.
        //             }
        //             finally
        //             {
        //                 consumer.Close();
        //             }
        //         }
        //     });
        // }


        static public void ConfigureLogging()
        {
            string logFileName = $"log_{DateTime.Now:yyyyMMdd_HHmmss}.txt";

            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .WriteTo.Console()
                .WriteTo.File(logFileName, rollingInterval: RollingInterval.Infinite)
                .CreateLogger();
        }

        private async void ListenForMessagesAsync()
        {

            _ = SendSyncCount();
            // _ = ConsumeSyncRequest($"syncRequest_{_host}", _consumerConfig);
            // _ = ConsumeSyncResponse($"syncResponse_{_host}", _consumerConfig);

            // create a new thread for consuming sync requests
            var consumeSyncRequest = new Thread(async () => await ConsumeSyncRequest($"syncRequest_{_host}", _consumerConfig));
            consumeSyncRequest.Start();

            // create a new thread for consuming sync responses
            var consumeSyncResponse = new Thread(async () => await ConsumeSyncResponse($"syncResponse_{_host}", _consumerConfig));
            consumeSyncResponse.Start();

            var listener = new TcpListener(IPAddress.Any, _listenPort);
            listener.Start();

            while (true)
            {
                var client = await listener.AcceptTcpClientAsync().ConfigureAwait(false);
                var reader = new StreamReader(client.GetStream());
                var message = await reader.ReadToEndAsync().ConfigureAwait(false);

                // Parse message and act accordingly
                await ParseMessageAsync(message);

                reader.Close();
                client.Close();
            }
        }

        private Task ParseMessageAsync(string message)
        {

            var messageObject = JObject.Parse(message);
            var eventType = messageObject["eventType"]?.ToString();

            switch (eventType)
            {
                case "newNodeEvent":
                    return HandleNewNodeEventAsync(messageObject);
                case "removeNodeEvent":
                    return HandleRemoveNodeEventAsync(messageObject);
                case "updateConitEvent":
                    return HandleUpdateConitEventAsync(messageObject);
                case "heartbeatEvent":
                    return HandleHeartbeatEventAsync(messageObject);
                // case "syncRequest":
                    // return HandleSyncRequestAsync(messageObject);
                // case "syncResponse":
                    // return HandleSyncResponseAsync(messageObject);
                case "finishEvent":
                    return HandleFinishEventAsync(messageObject);
                default:
                    Log.Warning("Unknown eventType: {EventType}", eventType);
                    return Task.CompletedTask;
            }
        }

        private Task HandleFinishEventAsync(JObject messageObject)
        {
            var data = messageObject["data"]?.ToString();
            var collectionName = messageObject["collection"]?.ToString();
            var senderPort = messageObject["port"]?.ToObject<int>();

            // check for null
            if (data == null || collectionName == null || senderPort == null)
            {
                Log.Error("SyncResponse data, collectionName or senderPort is null");
                return Task.CompletedTask;
            }

            // deserialize the data
            List<ConsumeResultWrapper<Null, string>> deserializedList = JsonConvert.DeserializeObject<List<ConsumeResultWrapper<Null, string>>>(data)!;

            // Convert back to original format
            _receivedData = deserializedList.Select(dw => new ConsumeResult<Null, string>
            {
                Topic = dw.Topic ?? string.Empty, // Handle possible null value
                Partition = dw.Partition,
                Offset = dw.Offset,
                Message = new Message<Null, string>
                {
                    Key = dw.Message?.Key!, // Handle possible null value
                    Value = dw.Message?.Value ?? string.Empty, // Handle possible null value
                    Timestamp = dw.Message?.Timestamp ?? default, // Handle possible null value
                    Headers = dw.Message?.Headers != null ? ConvertToKafkaHeaders(dw.Message.Headers) : null
                },
                IsPartitionEOF = dw.IsPartitionEOF
            }).ToList();

            Log.Information($"Received data count: {_receivedData.Count} - collection name: {collectionName} - sender port: {senderPort}");

            // add this data to the buffer
            if (_buffer.ContainsKey(collectionName))
            {
                _buffer[collectionName] = _receivedData;
            }
            else
            {
                _buffer.TryAdd(collectionName, _receivedData);
            }

            // we can now remove this collection from the dyconit collections
            _dyconitCollections.Collections?.RemoveAll(c => c.Name == collectionName);

            return Task.CompletedTask;
        }

        // private Task HandleSyncResponseAsync(JObject messageObject)
        // {
        //     var data = messageObject["data"]?.ToString();
        //     var collectionName = messageObject["collection"]?.ToString();
        //     var senderPort = messageObject["port"]?.ToObject<int>();
        //     var senderHost = messageObject["host"]?.ToString();

        //     // check for null
        //     if (data == null || collectionName == null || senderPort == null)
        //     {
        //         Log.Error("SyncResponse data, collectionName or senderPort is null");
        //         return Task.CompletedTask;
        //     }

        //     Log.Information($"Received sync response from {senderHost} for collection {collectionName}");

        //     // deserialize the data
        //     List<ConsumeResultWrapper<Null, string>> deserializedList = JsonConvert.DeserializeObject<List<ConsumeResultWrapper<Null, string>>>(data)!;

        //     // Convert back to original format
        //     _receivedData = deserializedList.Select(dw => new ConsumeResult<Null, string>
        //     {
        //         Topic = dw.Topic ?? string.Empty, // Handle possible null value
        //         Partition = dw.Partition,
        //         Offset = dw.Offset,
        //         Message = new Message<Null, string>
        //         {
        //             Key = dw.Message?.Key!, // Handle possible null value
        //             Value = dw.Message?.Value ?? string.Empty, // Handle possible null value
        //             Timestamp = dw.Message?.Timestamp ?? default, // Handle possible null value
        //             Headers = dw.Message?.Headers != null ? ConvertToKafkaHeaders(dw.Message.Headers) : null
        //         },
        //         IsPartitionEOF = dw.IsPartitionEOF
        //     }).ToList();

        //     Log.Information($"Received data count: {_receivedData.Count()} - collection name: {collectionName} - sender port: {senderPort}");

        //     // update the last time since pull for the sender port and collection combination
        //     var collection = _dyconitCollections.Collections
        //         ?.FirstOrDefault(c => c.Name == collectionName);
        //     if (collection != null)
        //     {
        //         var sender = collection.Nodes
        //             ?.FirstOrDefault(s => s.Port == senderPort);
        //         if (sender != null)
        //         {
        //             sender.LastTimeSincePull = DateTime.Now;
        //         }
        //     }

        //     // update the syn counter for the _listenPort and collection combination
        //     var node = _dyconitCollections.Collections
        //         ?.FirstOrDefault(c => c.Name == collectionName)
        //         ?.Nodes
        //         ?.FirstOrDefault(n => n.Port == _listenPort);

        //     if (node != null)
        //     {
        //         node.SyncCount++;
        //     }

        //     _syncResponses.Add(senderPort.Value);

        //     Log.Information("Handled sync request from {SenderPort} for data {collectionName}", senderPort, collectionName);

        //     return Task.CompletedTask;
        // }


        private Headers ConvertToKafkaHeaders(List<HeaderWrapper> headerWrappers)
        {
            var headers = new Headers();
            foreach (var headerWrapper in headerWrappers)
            {
                headers.Add(headerWrapper.Key, headerWrapper.Value);
            }
            return headers;
        }

        // private SyncResult UpdateLocalData(List<ConsumeResult<Null, string>> localData, string collectionName)
        // {
        //     if (localData == null)
        //     {
        //         Log.Error("Local data is null");
        //         return new SyncResult
        //         {
        //             changed = false,
        //             Data = _localData[collectionName],
        //         };
        //     }

        //     _localData[collectionName] = localData;

        //     // Make a local copy of localData
        //     var localDataCopy = localData.ToList();
        //     var _localDataCopy = _localData[collectionName].ToList();

        //     var topic = localDataCopy.First().Topic;

        //     if (_receivedData == null)
        //     {
        //         Log.Error("Received data is null");
        //         return new SyncResult
        //         {
        //             changed = false,
        //             Data = _localDataCopy,
        //         };
        //     }

        //     // Make a local copy of _receivedData
        //     var receivedDataCopy = _receivedData.ToList();

        //     if (receivedDataCopy.Count == 0)
        //     {

        //         if (_buffer.ContainsKey(topic))
        //         {
        //             receivedDataCopy = _buffer[topic];

        //             _buffer.Remove(topic);
        //         }
        //         else
        //         {
        //             Log.Error("Received data is empty");
        //             return new SyncResult
        //             {
        //                 changed = false,
        //                 Data = _localDataCopy,
        //             };
        //         }
        //     }

        //     if (receivedDataCopy.First().Topic != localDataCopy.First().Topic)
        //     {
        //         if (_buffer.ContainsKey(topic))
        //         {
        //             receivedDataCopy = _buffer[topic];
        //             _buffer.Remove(topic);
        //         }
        //         else
        //         {
        //             var key = receivedDataCopy.First().Topic;

        //             if (_buffer.ContainsKey(key))
        //             {
        //                 _buffer[key] = receivedDataCopy;
        //             }
        //             else
        //             {
        //                 _buffer.Add(key, receivedDataCopy);
        //             }

        //             Log.Information("Received data topic is not the same as local data topic");
        //             return new SyncResult
        //             {
        //                 changed = false,
        //                 Data = _localDataCopy,
        //             };
        //         }
        //     }

        //     // check if the topic of the receivedDataCopy is the same as the topic of localDataCopy
        //     if (receivedDataCopy == null || receivedDataCopy.Count < localDataCopy.Count)
        //     {
        //         Log.Information("Received data is null or smaller than local data");
        //         SyncResult earlyResult = new SyncResult
        //         {
        //             changed = (_synced || false),
        //             Data = _localDataCopy,
        //         };
        //         _synced = _synced ? false : _synced;
        //         return earlyResult;
        //     }


        //     // Merge the received data with the local data
        //     var mergedData = receivedDataCopy.Union(localDataCopy, new ConsumeResultComparer()).ToList();


        //     // foreach (var item in localDataCopy)
        //     // {
        //     //     Log.Debug($"[{_listenPort}] - localDataCopy: {item.Message.Value}");
        //     // }
        //     // Log.Debug("--------------------------------------------------");
        //     // foreach (var item in receivedDataCopy)
        //     // {
        //     //     Log.Debug($"[{_listenPort}] - receivedDataCopy: {item.Message.Value}");
        //     // }
        //     // Log.Debug("--------------------------------------------------");
        //     // foreach (var item in mergedData)
        //     // {
        //     //     Log.Debug($"[{_listenPort}] - mergedData: {item.Message.Value}");
        //     // }

        //     if (mergedData == null || !mergedData.Any())
        //     {
        //         Log.Error("Merged data is null or empty");
        //         return new SyncResult
        //         {
        //             changed = false,
        //             Data = _localDataCopy,
        //         };
        //     }

        //     bool isNotSame = false;

        //     if (mergedData != null && localDataCopy != null)
        //     {
        //         isNotSame = mergedData.Count() != localDataCopy.Count();
        //     }
        //     else
        //     {
        //         Log.Error("Merged data or local data is null");
        //         return new SyncResult
        //         {
        //             changed = false,
        //             Data = _localDataCopy,
        //         };
        //     }

        //     Log.Debug("isNotSame: {IsNotSame}, localDataCopy.Count: {LocalDataCount}, receivedDataCopy.Count: {ReceivedDataCount}, mergedData.Count: {MergedDataCount}", isNotSame, localDataCopy.Count(), receivedDataCopy.Count(), mergedData.Count());


        //     // check if the merged data has the same topic as the collection name. if this is not the case, store the merged data in the buffer
        //     if (mergedData != null && mergedData.Any() && mergedData.First().Topic != collectionName)
        //     {
        //         if (_buffer.ContainsKey(topic))
        //         {
        //             _buffer[topic] = mergedData;
        //         }
        //         else
        //         {
        //             _buffer.Add(topic, mergedData);
        //         }

        //         Log.Information($"Merged data topic is not the same as collection name -- {mergedData.First().Topic} != {collectionName}");
        //         return new SyncResult
        //         {
        //             changed = false,
        //             Data = _localDataCopy,
        //         };
        //     }

        //     SyncResult result = new SyncResult
        //     {
        //         changed = isNotSame || _synced,
        //         Data = mergedData
        //     };

        //     _localDataCopy = mergedData;
        //     _synced = _synced ? false : _synced;

        //     // Clear the received data
        //     _receivedData = null;

        //     Log.Information("Updated local data for collection {CollectionName}", collectionName);
        //     return result;
        // }

        private SyncResult UpdateLocalData(List<ConsumeResult<Null, string>> localData, string collectionName)
        {
            // Guard clauses for initial checks
            if (localData == null)
            {
                Log.Error("Local data is null");
                return new SyncResult {changed = false, Data = localData};
            }

            _localData[collectionName] = localData;

            if (_receivedData == null)
            {
                Log.Error("Received data is null");
                return new SyncResult {changed = false, Data = _localData[collectionName]};
            }

            var localDataCopy = _localData[collectionName].ToList();
            var (receivedDataCopy, earlyResult) = ManageReceivedData(localDataCopy);

            _synced = _synced ? false : _synced;

            if (earlyResult != null)
            {
                return new SyncResult {changed = _synced, Data = earlyResult.Data};
            }

            Log.Information($"Local count: {localDataCopy.Count} - Received count: {receivedDataCopy.Count}");

            // Print the number of each topic in the local and received data
            var localTopicCount = localDataCopy.Select(cr => cr.Topic).Distinct().Count();
            var receivedTopicCount = receivedDataCopy.Select(cr => cr.Topic).Distinct().Count();

            Log.Information($"Number of topics in local data: {localTopicCount} - Number of topics in received data: {receivedTopicCount}");

            var mergedData = MergeReceivedLocalData(receivedDataCopy, localDataCopy);
            var isDataChanged = mergedData.Count() != localDataCopy.Count();

            if (!isDataChanged)
            {
                Log.Information("Data has not changed");
                return new SyncResult {changed = _synced, Data = localDataCopy};
            }

            Log.Information($"Local count: {localDataCopy.Count} - Received count: {receivedDataCopy.Count} - Merged count: {mergedData.Count}");

            return HandleDataChange(mergedData, collectionName, isDataChanged);
        }

        private (List<ConsumeResult<Null, string>>, SyncResult) ManageReceivedData(List<ConsumeResult<Null, string>> localDataCopy)
        {
            var topic = localDataCopy.First().Topic;

            // check if received data is not null
            if (_receivedData == null)
            {
                Log.Error("Received data is null");
                return (null!, new SyncResult {changed = false, Data = localDataCopy});
            }

            // Make a local copy of _receivedData
            var receivedDataCopy = _receivedData.ToList();

            // Check if received data is empty and if there is buffered data for the current topic that can be used.
            if (!receivedDataCopy.Any())
            {
                if (_buffer.ContainsKey(topic))
                {
                    receivedDataCopy = _buffer[topic];
                    _buffer.Remove(topic);
                    Log.Information($"Received data was empty, using buffered data for topic {topic}");
                }
                else
                {
                    Log.Information($"Received data was empty and no buffered data for topic {topic} was found, returning early");
                    return (null!, new SyncResult {changed = false, Data = localDataCopy});
                }
            }

            // Check if received data topic is the same as local data topic and if not, buffer the received data and use the buffered data for the current topic.
            if (receivedDataCopy.First().Topic != topic)
            {
                // Check if there is buffered data for the current topic
                if (_buffer.ContainsKey(topic))
                {
                    Log.Information($"Buffered data found for topic {topic}, using this data");
                    receivedDataCopy = _buffer[topic];
                    _buffer.Remove(topic);
                }
                else
                {
                    BufferReceivedData(receivedDataCopy);
                    Log.Information($"No buffered data for topic {topic}, returning early");
                    return (null!, new SyncResult {changed = false, Data = localDataCopy});
                }
            }
            return (receivedDataCopy, null!);
        }

        private void BufferReceivedData(List<ConsumeResult<Null, string>> receivedDataCopy)
        {
            var receivedTopic = receivedDataCopy.First().Topic;
            if (_buffer.ContainsKey(receivedTopic))
            {
                _buffer[receivedTopic] = receivedDataCopy;
                Log.Information($"Buffered data updated for topic {receivedTopic}");
            }
            else
            {
                _buffer.TryAdd(receivedTopic, receivedDataCopy);
                Log.Information($"Buffered data added for topic {receivedTopic}");
            }
        }

        private List<ConsumeResult<Null, string>> MergeReceivedLocalData(List<ConsumeResult<Null, string>> receivedDataCopy, List<ConsumeResult<Null, string>> localDataCopy)
        {
            var mergedData = receivedDataCopy.Union(localDataCopy, new ConsumeResultComparer()).ToList();
            Log.Information("Data has been merged");

            // count and log the number of topics there are in the merged data
            var topicCount = mergedData.Select(cr => cr.Topic).Distinct().Count();
            Log.Information($"Number of topics in merged data: {topicCount}");

            return mergedData.Any() ? mergedData : throw new InvalidOperationException("Merged data is null or empty");
        }

        private SyncResult HandleDataChange(List<ConsumeResult<Null, string>> mergedData, string collectionName, bool isDataChanged)
        {
            var topic = mergedData.First().Topic;
            if (topic != collectionName)
            {
                if (_buffer.ContainsKey(topic))
                {
                    _buffer[topic] = mergedData;
                    Log.Information($"Buffered data updated for topic {topic}");
                }
                else
                {
                    _buffer.TryAdd(topic, mergedData);
                    Log.Information($"Buffered data added for topic {topic}");
                }

                Log.Information($"Merged data topic is not the same as collection name -- {mergedData.First().Topic} != {collectionName}, data buffered");
                return new SyncResult {changed = false, Data = _localData[collectionName]};
            }

            _localData[collectionName] = mergedData;
            _receivedData = null;
            Log.Information($"Data has changed. Updated local data for collection {collectionName}");
            return new SyncResult {changed = isDataChanged, Data = mergedData};
        }


        // private async Task HandleSyncRequestAsync(JObject messageObject)
        // {

        //     var syncRequestPort = messageObject["port"]?.ToObject<int>();
        //     var collectionName = messageObject["collectionName"]?.ToString();
        //     var syncRequestHost = messageObject["host"]?.ToString();

        //     // check for null
        //     if (syncRequestPort == null || collectionName == null)
        //     {
        //         Log.Error("SyncRequest port or collectionName is null");
        //         return;
        //     }
        //     Log.Information($"Received Sync Request from host {syncRequestHost} for data in {collectionName}",syncRequestHost, collectionName);


        //     List<ConsumeResultWrapper<Null, string>> wrapperList = _localData[collectionName]!.Select(cr => new ConsumeResultWrapper<Null, string>(cr)).ToList();
        //     string consumeResultWrapped = JsonConvert.SerializeObject(wrapperList, Formatting.Indented);

        //     // var syncResponse = new JObject
        //     // {
        //     //     { "eventType", "syncResponse" },
        //     //     { "port", _listenPort },
        //     //     { "host", _host},
        //     //     { "data", consumeResultWrapped },
        //     //     { "collection", collectionName}
        //     // };

        //     Log.Information("Sending Sync Response to host {Host} for data in {collectionName} with number of items {count}", syncRequestHost, collectionName, consumeResultWrapped.Count());

        //     // await SendMessageOverTcp(syncResponse.ToString(), syncRequestPort.Value, syncRequestHost!);

        //     // create a producer and send the data to the host

        //     await _producer.ProduceAsync("syncResponse", new Message<Null, string> { Value = consumeResultWrapped });

        //     _synced = true;

        //     // increment the sync for the port and colleciton combination
        //     var collection = _dyconitCollections.Collections
        //         ?.FirstOrDefault(c => c.Name == collectionName);
        //     var node = collection?.Nodes
        //         ?.FirstOrDefault(n => n.Port == _listenPort);
        //     if (node != null)
        //     {
        //         node.SyncCount++;
        //     }
        // }

        private async Task HandleHeartbeatEventAsync(JObject messageObject)
        {
            var heartbeatResponse = new JObject
            {
                ["eventType"] = "heartbeatResponse",
                ["port"] = _listenPort,
                ["host"] = _host
            };

            var heartbeatResponseString = heartbeatResponse.ToString();
            await SendMessageOverTcp(heartbeatResponseString, 6666, "app1");
        }

        private Task HandleUpdateConitEventAsync(JObject messageObject)
        {
            // Get the collection name and the corresponding node based on the port
            var collectionName = messageObject["collectionName"]?.ToString();
            var collection = _dyconitCollections.Collections
                ?.FirstOrDefault(c => c.Name == collectionName);

            // Get the node based on the port
            var nodePort = messageObject["port"]?.ToObject<int>();
            var node = collection?.Nodes
                ?.FirstOrDefault(n => n.Port == nodePort);

            // Update the node's bounds
            var newStaleness = messageObject["bounds"]?["Staleness"]?.ToObject<double>();
            if (newStaleness != null && node != null && node.Bounds != null)
            {
                node.Bounds.Staleness = newStaleness;
            }

            var newNumericalError = messageObject["bounds"]?["NumericalError"]?.ToObject<double>();
            if (newNumericalError != null && node != null && node.Bounds != null)
            {
                node.Bounds.NumericalError = newNumericalError;
            }

            Log.Information($"Updated bounds for node {nodePort} in collection {collectionName} to staleness {newStaleness} and numerical error {newNumericalError}");

            return Task.CompletedTask;
        }

        private Task HandleRemoveNodeEventAsync(JObject messageObject)
        {
            var removeNodePort = messageObject["port"]?.ToObject<int>();
            var collectionName = messageObject["collectionName"]?.ToString();

            // remove the node from the collection
            var collection = _dyconitCollections.Collections
                ?.FirstOrDefault(c => c.Name == collectionName);
            collection?.Nodes?.RemoveAll(n => n.Port == removeNodePort);

            return Task.CompletedTask;
        }

        private Task HandleNewNodeEventAsync(JObject messageObject)
        {
            var newNodePort = messageObject["port"]?.ToObject<int>();
            var collectionName = messageObject["collectionName"]?.ToString();
            var host = messageObject["host"]?.ToString();

            Log.Information($"New node event received for port {newNodePort} and collection {collectionName} and host {host}");

            // create a new node and add it to the collection
            var newNode = new Node
            {
                Port = newNodePort,
                Host = host,
                LastHeartbeatTime = DateTime.Now,
                LastTimeSincePull = DateTime.Now,
                SyncCount = 0,
                Bounds = new Bounds
                {
                    Staleness = messageObject["staleness"]?.ToObject<int>(),
                    NumericalError = messageObject["numericalError"]?.ToObject<int>()
                }
            };

            // add the new node to the collection
            var collection = _dyconitCollections.Collections
                ?.FirstOrDefault(c => c.Name == collectionName);
            collection?.Nodes?.Add(newNode);

            return Task.CompletedTask;

        }

        private async Task SendSyncCount()
        {
            while (true)
            {
                var startTime = DateTime.UtcNow;

                await SendSyncCountAsync();

                var endTime = DateTime.UtcNow;
                var elapsed = endTime - startTime;
                var delay = TimeSpan.FromSeconds(20) - elapsed;

                if (delay > TimeSpan.Zero)
                {
                    await Task.Delay(delay);
                }
            }
        }

        private async Task SendSyncCountAsync()
        {
            var message = new JObject
            {
                ["eventType"] = "overheadMessage",
                ["port"] = _listenPort,
                ["host"] = _host,
                ["data"] = new JObject()
            };

            var syncNodes = new List<Node>();

            // loop through each collection and get the total sync count
            foreach (var collection in _dyconitCollections.Collections ?? Enumerable.Empty<Collection>())
            {
                double syncCount = 0;

                foreach (var node in collection.Nodes ?? Enumerable.Empty<Node>())
                {
                    if (node.SyncCount.HasValue)
                    {
                        syncCount += node.SyncCount.Value;
                        if (node.SyncCount.Value > 0)
                        {
                            syncNodes.Add(node);
                        }
                    }
                }

                // add the sync count to the message
                message["data"]![collection.Name ?? string.Empty] = syncCount;
                Log.Information($"Collection name: {collection.Name} overhead throughput {syncCount} messages/s");
            }

            await SendMessageOverTcp(message.ToString(), 6666, "app1");

            // Reset the sync count
            foreach (var node in syncNodes)
            {
                node.SyncCount = 0;
            }
        }


        private RootObject CreateDyconitCollections(JObject conitCollection, int adminPort, string host)
        {
            var dyconitCollections = new RootObject
            {
                Collections = new List<Collection>()
            };
            if (conitCollection != null)
            {
                foreach (var conit in conitCollection)
                {
                    var collection = new Collection
                    {
                        Name = conit.Key,
                        Nodes = new List<Node>(),
                        Rules = new List<Rule>(),
                    };
                    dyconitCollections.Collections.Add(collection);

                    var node = new Node
                    {
                        Port = adminPort,
                        Host = host,
                        LastHeartbeatTime = DateTime.Now,
                        LastTimeSincePull = DateTime.Now,
                        SyncCount = 0,
                        Bounds = new Bounds
                        {
                            Staleness = conit.Value?["Staleness"]?.ToObject<int>(),
                            NumericalError = conit.Value?["NumericalError"]?.ToObject<int>()
                        }
                    };
                    collection.Nodes.Add(node);
                }
            }
            return dyconitCollections;
        }

        // private async Task SendMessageOverTcp(string message, int port, string host)
        // {
        //     try
        //     {
        //         using (var client = new TcpClient())
        //         {
        //             client.Connect(host, port);

        //             using (var stream = client.GetStream())
        //             using (var writer = new StreamWriter(stream))
        //             {
        //                 await writer.WriteLineAsync(message);
        //                 await writer.FlushAsync();
        //             }
        //         }
        //     }
        //     catch (Exception)
        //     {
        //         Log.Error($"Error sending message to {host}:{port}");
        //         // remove this node from every collection
        //         foreach (var collection in _dyconitCollections.Collections ?? Enumerable.Empty<Collection>())
        //         {
        //             collection.Nodes?.RemoveAll(n => n.Port == port);
        //         }

        //     }
        // }

        private async Task SendMessageOverTcp(string message, int port, string host, int maxRetries = 3)
        {
            int retries = 0;
            while (retries < maxRetries)
            {
                try
                {
                    using (var client = new TcpClient())
                    {
                        client.Connect(host, port);

                        using (var stream = client.GetStream())
                        using (var writer = new StreamWriter(stream))
                        {
                            await writer.WriteLineAsync(message);
                            await writer.FlushAsync();
                        }
                    }
                    return; // Success, exit the method
                }
                catch (Exception)
                {
                    Log.Error($"Error sending message to {host}:{port}. Retrying {retries + 1} of {maxRetries}");
                    // Increment the retry counter
                    retries++;

                    if (retries >= maxRetries)
                    {
                        Log.Error($"Error sending message to {host}:{port}. Max retries exceeded.");
                        // remove this node from every collection
                        foreach (var collection in _dyconitCollections.Collections ?? Enumerable.Empty<Collection>())
                        {
                            collection.Nodes?.RemoveAll(n => n.Port == port);
                        }
                    }
                    // Optionally, you might introduce a delay before retrying
                    await Task.Delay(TimeSpan.FromSeconds(1));
                }
            }
        }



        public async Task<SyncResult> BoundStaleness(List<ConsumeResult<Null, string>> localData, string collectionName)
        {
            var portsStalenessExceeded = new List<int>();

            // Check if we have received new data from an other node since the last time we checked
            SyncResult result = UpdateLocalData(localData, collectionName);

            // retrieve all the other nodes in the collection
            var otherNodes = _dyconitCollections.Collections
                ?.FirstOrDefault(c => c.Name == collectionName)
                ?.Nodes
                ?.Where(n => n.Port != _listenPort)
                ?.ToList();

            Log.Information($"There are {otherNodes?.Count} other nodes in the collection {collectionName}");

            // if there are no other nodes, we can't do anything
            if (otherNodes == null || !otherNodes.Any())
            {
                return result;
            }

            var consumedTime = DateTime.Now;

            // Compare the consume time with the LastTimeSincePull for each node.
            // If the difference is greater than the staleness bound, we need to pull data from the other node
            foreach (var node in otherNodes)
            {
                var timeSincePull = consumedTime - node.LastTimeSincePull;
                var stalenessBound = node.Bounds?.Staleness;

                if (!stalenessBound.HasValue)
                {
                    continue;
                }

                Log.Information($"Time since pull for port {node.Port} is {timeSincePull!.Value.TotalMilliseconds} milliseconds, staleness bound is {stalenessBound.Value} milliseconds");

                if (timeSincePull.HasValue && timeSincePull.Value.TotalMilliseconds > stalenessBound.Value)
                {
                    portsStalenessExceeded.Add(node.Port!.Value);
                    Log.Information($"Host {node.Host} port {node.Port} staleness bound exceeded. Sending pull request by producing to topic syncRequest_{node.Host}");

                    // let the producer send the syncRequest
                    await _producer.ProduceAsync($"syncRequest_{node.Host}", new Message<Null, string> { Value = collectionName + ";"+ _host });

                    // update the LastTimeSincePull for the node
                    node.LastTimeSincePull = DateTime.Now;
                }
            }

            // if (portsStalenessExceeded.Any())
            // {
            //     await WaitForResponseAsync(portsStalenessExceeded);
            // }

            var lateResult = UpdateLocalData(localData, collectionName);

            // comapre result with lateResult. Return the result with the highest data count
            if (lateResult.Data!.Count > result.Data!.Count)
            {
                return lateResult;
            }

            // Return the local data
            return result;
        }

        private async Task WaitForResponseAsync(List<int> portsStalenessExceeded)
        {
            int timeout = 15000; // Set timeout to 15 seconds
            var startTime = DateTime.UtcNow;

            Log.Information($"Start waiting for responses from ports: {string.Join(",", portsStalenessExceeded)}");

            while (true)
            {
                if (_syncResponses.Count >= portsStalenessExceeded.Count)
                {
                    Log.Information($"Received all expected sync responses. Count: {_syncResponses.Count}");
                    _syncResponses.Clear();
                    break;
                }

                // Check if timeout has been exceeded
                if (DateTime.UtcNow - startTime > TimeSpan.FromMilliseconds(timeout))
                {
                    Log.Warning($"Timeout exceeded while waiting for sync responses from ports: {string.Join(",", portsStalenessExceeded)}. Received responses: {_syncResponses.Count}");
                    break;
                }

                Log.Information($"Still waiting for responses. Received responses: {_syncResponses.Count}");
                await Task.Delay(100);
            }
        }


        public async Task<bool> BoundNumericalError(List<ConsumeResult<Null, string>> list, string topic, double totalLocalWeight)
        {
            bool exceeded = false;
            // get the number of nodes in the collection
            var numberOfNodes = _dyconitCollections.Collections
                ?.FirstOrDefault(c => c.Name == topic)
                ?.Nodes
                ?.Count;

            Log.Information($"There are {numberOfNodes} nodes in the collection {topic}");

            // if the number of nodes is null, we can't do anything
            if (!numberOfNodes.HasValue || numberOfNodes.Value == 0 || numberOfNodes.Value == 1)
            {
                return _synced || false;
            }
            else
            {
                var otherNodes = _dyconitCollections.Collections
                    ?.FirstOrDefault(c => c.Name == topic)
                    ?.Nodes
                    ?.Where(n => n.Port != _listenPort).ToList();

                // go through the numerical error bounds for each node which is not the current node
                foreach (var node in otherNodes!)
                {
                    double numericalErrorBound = node.Bounds!.NumericalError!.Value / (numberOfNodes.Value - 1);

                    Log.Information($"Average weight per node is {totalLocalWeight}, numerical error bound is {numericalErrorBound}");

                    // if the average weight per node is greater than the numerical error bound, we need to send data to the other node
                    if (totalLocalWeight > numericalErrorBound)
                    {
                        exceeded = true;

                        Log.Information($"host {node.Host} port {node.Port} numerical error bound exceeded. Sending sync request to topic syncRequest_{node.Host}");

                        // await SendMessageOverTcp(message.ToString(), _listenPort, _host);
                        await _producer.ProduceAsync($"syncRequest_{_host}", new Message<Null, string> { Value = topic + ";"+ node.Host });

                    }
                }

                Log.Information($"Numerical error bound exceeded: {exceeded}, synced: {_synced}");

                return exceeded || _synced;
            }
        }

    }
}
