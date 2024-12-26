using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Consumer
{
    internal class Program
    {
        static void Main(string[] args)
        {
            string bootstrapServers = "localhost:9092";
            string topicName = "TimeFix";
            string groupId = "test-group";

            // Define the Consumer configuration
            var config = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest // Start reading at the earliest message
            };

            // Create a new Consumer using the configuration
            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                Console.WriteLine("Press Ctrl+C to stop the Consumer.");
                // Subscribe to the specified topic
                consumer.Subscribe(topicName);

                Console.WriteLine("Connected. Press Ctrl+C to exit.\nListening for messages...");

                try
                {
                    while (true)
                    {
                        try
                        {
                            // Wait for new messages and handle them as they come
                            var consumeResult = consumer.Consume();

                            // Display the message to the console
                            Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");
                        }
                        catch (ConsumeException e)
                        {
                            // Handle the exception if there's an error in consuming messages
                            Console.WriteLine($"Consume error: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Handle cancellation (typically when Ctrl+C is pressed)
                    ////consumer.Close(); // Commit offsets and leave the group cleanly
                    ////Console.WriteLine("Consumer closed.");
                }
            }
        }
    }
}
