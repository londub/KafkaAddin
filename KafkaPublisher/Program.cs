using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Newtonsoft.Json;
using System.Configuration;

namespace KafkaPublisher
{

    // ===============================
    //Step1:
    //write a c# program that generates like above data and publish to kafka topic time is increasing order date time, value is float number between 0 and 1
    //use random generator generating 2 message in a second
    // ===============================
    class Program
    {
        static void Main(string[] args)
        {
            var bootstrapServers = ConfigurationManager.AppSettings["BootstrapServers"];
            var topic = ConfigurationManager.AppSettings["KafkaTopic"];

            var config = new ProducerConfig
            {
                BootstrapServers = bootstrapServers
            };

            var random = new Random();

            while (true)
            {
                var time = DateTime.UtcNow;
                var value = random.NextDouble();
                var key = random.Next(1, 3);

                var message = new
                {
                    key = "k" + key,
                    value = new
                    {
                        time = time.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"),
                        value = value
                    }
                };

                var serializedMessage = JsonConvert.SerializeObject(message);
                var kafkaMessage = new Message<string, string>
                {
                    Key = "k" + key,
                    Value = serializedMessage
                };

                using (var producer = new ProducerBuilder<string, string>(config).Build())
                {
                    var result = producer.ProduceAsync(topic, kafkaMessage).GetAwaiter().GetResult();
                }

                Console.WriteLine($"Published Message: {serializedMessage}");

                System.Threading.Thread.Sleep(500);
            }
        }
    }
}
