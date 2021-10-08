using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Mime;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using MassTransit;
using MassTransit.Definition;
using MassTransit.KafkaIntegration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace KafkaTest
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var producer = false;
            var consumer = 3;

            if (args.Length > 0)
            {
                switch (args[0])
                {
                    case "p":
                        producer = true;
                        consumer = 0;
                        Console.WriteLine("Producer mode");
                        break;
                    case "1":
                        consumer = 1;
                        Console.WriteLine("Consumer 1 mode");
                        break;
                    case "2":
                        consumer = 2;
                        Console.WriteLine("Consumer 2 mode");
                        break;
                }
            }
            else
            {
                Console.WriteLine("Consumer mode");
            }

            var services = new ServiceCollection();

            services.AddLogging(c =>
            {
                c.AddConsole();
                c.SetMinimumLevel(LogLevel.Debug);
            });

            var clientConfig = new ClientConfig()
            {
                BootstrapServers = "localhost:9092"
            };

            var commandsTopicId = "global-commands-10";
            var groupEventsTopicId = "global-group-events-10";
            var pubSubEventsTopicId = "global-pub-sub-events-10";

            var commandsGroupId = KebabCaseEndpointNameFormatter.Instance.SanitizeName(Assembly.GetCallingAssembly().GetName().FullName) + consumer;
            var groupEventsGroupId = KebabCaseEndpointNameFormatter.Instance.SanitizeName(Assembly.GetCallingAssembly().GetName().FullName) + consumer;
            var pubSubEventsGroupId = Guid.NewGuid().ToString();

            services.AddMassTransit(x =>
            {
                x.UsingInMemory((context, cfg) => cfg.ConfigureEndpoints(context));

                x.AddRider(rider =>
                {
                    if (consumer > 0)
                    {
                        var consumer1 = new []
                        {
                            typeof(TestCommand1Consumer),
                            typeof(TestGroupEvent1Consumer),
                            typeof(TestPubSubEvent1Consumer)
                        };

                        var consumer2 = new[]
                        {
                            typeof(TestCommand2Consumer),
                            typeof(TestGroupEvent2Consumer),
                            typeof(TestPubSubEvent2Consumer)
                        };

                        switch (consumer)
                        {
                            case > 2:
                                rider.AddConsumers(consumer1.Concat(consumer2).ToArray());
                                break;
                            case > 1:
                                rider.AddConsumers(consumer2);
                                break;
                            default:
                                rider.AddConsumers(consumer1);
                                break;
                        }
                    }

                    if (producer)
                    {
                        rider.AddProducer<Command, ICommand>(commandsTopicId,
                            (_, producerConfigurator) =>
                            {
                                producerConfigurator.SetValueSerializer(new ConsumerMessageSerializationHandler());
                            });
                        rider.AddProducer<GroupEvent, IEvent>(groupEventsTopicId,
                            (_, producerConfigurator) =>
                            {
                                producerConfigurator.SetValueSerializer(new ConsumerMessageSerializationHandler());
                            });
                        rider.AddProducer<PubSubEvent, IEvent>(pubSubEventsTopicId,
                            (_, producerConfigurator) =>
                            {
                                producerConfigurator.SetValueSerializer(new ConsumerMessageSerializationHandler());
                            });
                    }

                    rider.UsingKafka(clientConfig, (context, cfg) =>
                    {
                        if (consumer > 0)
                        {
                            cfg.TopicEndpoint<Command, ICommand>(commandsTopicId, commandsGroupId, c =>
                            {
                                if (consumer != 2)
                                    c.ConfigureConsumer<TestCommand1Consumer>(context);
                                if (consumer != 1)
                                    c.ConfigureConsumer<TestCommand2Consumer>(context);
                                c.SetValueDeserializer(new ConsumerMessageSerializationHandler());
                                c.AutoOffsetReset = AutoOffsetReset.Earliest;
                                c.CheckpointMessageCount = 100;
                                c.CheckpointInterval = TimeSpan.FromSeconds(60);
                                c.PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky;
                                c.CreateIfMissing(options =>
                                {
                                    options.NumPartitions = 2;
                                    options.ReplicationFactor = 1;
                                });
                            });

                            cfg.TopicEndpoint<GroupEvent, IEvent>(groupEventsTopicId, groupEventsGroupId, c =>
                            {
                                if (consumer != 2)
                                    c.ConfigureConsumer<TestGroupEvent1Consumer>(context);
                                if (consumer != 1)
                                    c.ConfigureConsumer<TestGroupEvent2Consumer>(context);
                                c.SetValueDeserializer(new ConsumerMessageSerializationHandler());
                                c.AutoOffsetReset = AutoOffsetReset.Earliest;
                                c.CheckpointMessageCount = 100;
                                c.CheckpointInterval = TimeSpan.FromSeconds(60);
                                c.PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky;
                                c.CreateIfMissing(options =>
                                {
                                    options.NumPartitions = 2;
                                    options.ReplicationFactor = 1;
                                });
                            });

                            cfg.TopicEndpoint<PubSubEvent, IEvent>(pubSubEventsTopicId, pubSubEventsGroupId, c =>
                            {
                                if (consumer != 2)
                                    c.ConfigureConsumer<TestPubSubEvent1Consumer>(context);
                                if (consumer != 1)
                                    c.ConfigureConsumer<TestPubSubEvent2Consumer>(context);
                                c.SetValueDeserializer(new ConsumerMessageSerializationHandler());
                                c.AutoOffsetReset = AutoOffsetReset.Latest;
                                c.CheckpointMessageCount = 100;
                                c.CheckpointInterval = TimeSpan.FromSeconds(60);
                                c.PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky;
                                c.CreateIfMissing(options =>
                                {
                                    options.NumPartitions = 2;
                                    options.ReplicationFactor = 1;
                                });
                            });
                        }

                        //cfg.UseSendFilter();
                    });
                });
            });

            var provider = services.BuildServiceProvider();

            var busControl = provider.GetRequiredService<IBusControl>();

            await busControl.StartAsync();

            if (producer)
            {
                var commandsProducer = provider.GetRequiredService<ITopicProducer<Command, ICommand>>();
                var groupEventsProducer = provider.GetRequiredService<ITopicProducer<GroupEvent, IEvent>>();
                var pubSubEventsProducer = provider.GetRequiredService<ITopicProducer<PubSubEvent, IEvent>>();

                await commandsProducer.Produce(new Command(), new TestCommand1());
                await commandsProducer.Produce(new Command(), new TestCommand2());

                await groupEventsProducer.Produce(new GroupEvent(), new TestGroupEvent1());
                await groupEventsProducer.Produce(new GroupEvent(), new TestGroupEvent2());

                await pubSubEventsProducer.Produce(new PubSubEvent(), new TestPubSubEvent1());
                await pubSubEventsProducer.Produce(new PubSubEvent(), new TestPubSubEvent2());
            }

            await Task.Delay(30000);

            await busControl.StopAsync();
        }
    }

    public class ConsumerMessageSerializationHandler : ISerializer<ICommand>, IDeserializer<ICommand>, ISerializer<IEvent>, IDeserializer<IEvent>
    {
        private const string MessagePrefix = "__MESSAGE_PREFIX__";

        private static byte[] Serialize<T>(T data, SerializationContext context)
        {
            var type = data.GetType();
            var typeAndAssemblyName = $"{type.FullName}, {type.Assembly.GetName().Name}";
            var message = $"{typeAndAssemblyName}{MessagePrefix}{JsonConvert.SerializeObject(data)}";
            return Encoding.UTF8.GetBytes(message);
        }

        private static T Deserialize<T>(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            var message = Encoding.UTF8.GetString(data);
            var arMessage = message.Split(MessagePrefix);
            if (arMessage.Length != 2)
                throw new InvalidOperationException($"Cannot read message: {message}");

            var type = Type.GetType(arMessage[0]);
            if (type == null)
                throw new InvalidOperationException($"Cannot find message type: {arMessage[0]}");

            return (T) JsonConvert.DeserializeObject(arMessage[1], type);
        }

        public byte[] Serialize(ICommand data, SerializationContext context)
        {
            return Serialize<ICommand>(data, context);
        }

        ICommand IDeserializer<ICommand>.Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            return Deserialize<ICommand>(data, isNull, context);
        }

        public byte[] Serialize(IEvent data, SerializationContext context)
        {
            return Serialize<IEvent>(data, context);
        }

        IEvent IDeserializer<IEvent>.Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            return Deserialize<IEvent>(data, isNull, context);
        }
    }

    public class Command
    {
    }

    public class GroupEvent
    {
    }

    public class PubSubEvent
    {
    }

    public interface ICommand
    {
    }

    public interface IEvent
    {
    }

    public class TestCommand1 : ICommand
    {
    }

    public class TestCommand2 : ICommand
    {
    }

    public class TestGroupEvent1 : IEvent
    {
    }

    public class TestGroupEvent2 : IEvent
    {
    }

    public class TestPubSubEvent1 : IEvent
    {
    }

    public class TestPubSubEvent2 : IEvent
    {
    }

    public class TestCommand1Consumer : IConsumer<TestCommand1>
    {
        public async Task Consume(ConsumeContext<TestCommand1> context)
        {
            Console.WriteLine($"{DateTime.UtcNow}~~Received {nameof(TestCommand1)} by {this.GetType().Name}");
        }
    }

    public class TestCommand2Consumer : IConsumer<TestCommand2>
    {
        public async Task Consume(ConsumeContext<TestCommand2> context)
        {
            Console.WriteLine($"{DateTime.UtcNow}~~Received {nameof(TestCommand2)} by {this.GetType().Name}");
        }
    }

    public class TestGroupEvent1Consumer : IConsumer<TestGroupEvent1>
    {
        public async Task Consume(ConsumeContext<TestGroupEvent1> context)
        {
            Console.WriteLine($"{DateTime.UtcNow}~~Received {nameof(TestGroupEvent1)} by {this.GetType().Name}");
        }
    }

    public class TestGroupEvent2Consumer : IConsumer<TestGroupEvent2>
    {
        public async Task Consume(ConsumeContext<TestGroupEvent2> context)
        {
            Console.WriteLine($"{DateTime.UtcNow}~~Received {nameof(TestGroupEvent2)} by {this.GetType().Name}");
        }
    }

    public class TestPubSubEvent1Consumer : IConsumer<TestPubSubEvent1>
    {
        public async Task Consume(ConsumeContext<TestPubSubEvent1> context)
        {
            Console.WriteLine($"{DateTime.UtcNow}~~Received {nameof(TestPubSubEvent1)} by {this.GetType().Name}");
        }
    }

    public class TestPubSubEvent2Consumer : IConsumer<TestPubSubEvent2>
    {
        public async Task Consume(ConsumeContext<TestPubSubEvent2> context)
        {
            Console.WriteLine($"{DateTime.UtcNow}~~Received {nameof(TestPubSubEvent2)} by {this.GetType().Name}");
        }
    }
}
