using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Producer
{
    internal class Program
    {
        static void Main(string[] args)
        {
            string bootstrapServers = "localhost:9092";
            string topicName = "TimeFix";

            // Define the Producer configuration
            var config = new ProducerConfig { BootstrapServers = bootstrapServers };

            // Create a new Producer using the configuration
            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                Console.WriteLine("Press Ctrl+C to stop the producer.");

                // Continuously send messages until the program is stopped
                while (true)
                {
                    string timeNow = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
                    string messageValue = $"Sample message sent at {timeNow}";

                    // Construct a message to send
                    var message = new Message<Null, string> { Value = messageValue };

                    try
                    {
                        // Send the message to the specified topic and await the result
                        var deliveryResult = producer.ProduceAsync(topicName, message).GetAwaiter().GetResult();

                        // Output the result to the console
                        Console.WriteLine($"Delivered '{deliveryResult.Value}' to: {deliveryResult.TopicPartitionOffset}");
                    }
                    catch (ProduceException<Null, string> e)
                    {
                        // Handle the exception if the message delivery fails
                        Console.WriteLine($"Failed to deliver message: {e.Message} [{e.Error.Code}]");
                    }

                    // Sleep for a second before sending the next message
                    //Thread.Sleep(10);
                }
            }
        }
    }
}
