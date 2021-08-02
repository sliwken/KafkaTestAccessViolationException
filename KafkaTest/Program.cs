using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using MassTransit;
using MassTransit.Definition;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace KafkaTest
{
    class Program
    {
        static async Task Main(string[] args)
        {
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

            services.AddMassTransit(x =>
            {
                x.UsingInMemory((context, cfg) => cfg.ConfigureEndpoints(context));

                x.AddRider(rider =>
                {
                    rider.AddConsumers(typeof(TestCommandConsumer1), typeof(TestCommandConsumer2), typeof(TestCommandConsumer3));

                    rider.UsingKafka(clientConfig, (context, cfg) =>
                    {
                        var topic1 = KebabCaseEndpointNameFormatter.Instance.SanitizeName(typeof(TestCommand1).FullName);

                        cfg.TopicEndpoint<TestCommand1>(topic1, topic1, c =>
                        {
                            c.ConfigureConsumer<TestCommandConsumer1>(context);
                            c.AutoOffsetReset = AutoOffsetReset.Earliest;
                            c.CheckpointMessageCount = 100;
                            c.CheckpointInterval = TimeSpan.FromSeconds(60);
                            c.PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky;
                            c.CreateIfMissing(options =>
                            {
                                options.NumPartitions = 1;
                                options.ReplicationFactor = 1;
                            });
                        });

                        var topic2 = KebabCaseEndpointNameFormatter.Instance.SanitizeName(typeof(TestCommand2).FullName);

                        cfg.TopicEndpoint<TestCommand2>(topic2, topic2, c =>
                        {
                            c.ConfigureConsumer<TestCommandConsumer2>(context);
                            c.AutoOffsetReset = AutoOffsetReset.Earliest;
                            c.CheckpointMessageCount = 100;
                            c.CheckpointInterval = TimeSpan.FromSeconds(60);
                            c.PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky;
                            c.CreateIfMissing(options =>
                            {
                                options.NumPartitions = 1;
                                options.ReplicationFactor = 1;
                            });
                        });

                        var topic3 = KebabCaseEndpointNameFormatter.Instance.SanitizeName(typeof(TestCommand3).FullName);

                        cfg.TopicEndpoint<TestCommand3>(topic3, topic3, c =>
                        {
                            c.ConfigureConsumer<TestCommandConsumer3>(context);
                            c.AutoOffsetReset = AutoOffsetReset.Earliest;
                            c.CheckpointMessageCount = 100;
                            c.CheckpointInterval = TimeSpan.FromSeconds(60);
                            c.PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky;
                            c.CreateIfMissing(options =>
                            {
                                options.NumPartitions = 1;
                                options.ReplicationFactor = 1;
                            });
                        });
                    });
                });
            });

            var provider = services.BuildServiceProvider();

            var busControl = provider.GetRequiredService<IBusControl>();

            await busControl.StartAsync();

            await Task.Delay(10000);

            await busControl.StopAsync();
        }
    }

    public class TestCommand1
    {
    }

    public class TestCommand2
    {
    }

    public class TestCommand3
    {
    }

    public class TestCommandConsumer1 : IConsumer<TestCommand1>
    {
        public async Task Consume(ConsumeContext<TestCommand1> context)
        {
            Console.WriteLine($"{DateTime.UtcNow}~~Received {nameof(TestCommand1)}");
        }
    }

    public class TestCommandConsumer2 : IConsumer<TestCommand2>
    {
        public async Task Consume(ConsumeContext<TestCommand2> context)
        {
            Console.WriteLine($"{DateTime.UtcNow}~~Received {nameof(TestCommand2)}");
        }
    }

    public class TestCommandConsumer3 : IConsumer<TestCommand3>
    {
        public async Task Consume(ConsumeContext<TestCommand3> context)
        {
            Console.WriteLine($"{DateTime.UtcNow}~~Received {nameof(TestCommand3)}");
        }
    }
}
