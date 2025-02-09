using RabbitMQ.Client.Events;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DW.IPR.RabbitMQ.Test.ExchangeExamples
{
    public static class TopicExample
    {
        async public static Task StartExample()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using var connection = await factory.CreateConnectionAsync();
            using var channel = await connection.CreateChannelAsync();

            // Объявляем exchange типа topic
            await channel.ExchangeDeclareAsync(exchange: "topic_exchange", type: ExchangeType.Topic, durable: true);

            // Создаем очередь и связываем ее с exchange по routing key
            var queueName = "topic_queue";
            await channel.QueueDeclareAsync(queue: queueName,
                                    durable: false,
                                    exclusive: false,
                                    autoDelete: false,
                                    arguments: null);

            var routingKey = "my.topic.*";
            await channel.QueueBindAsync(queue: queueName,
                              exchange: "topic_exchange",
                              routingKey: routingKey);

            // Producer
            string message = "Hello RabbitMQ with Topic Exchange!";
            var body = Encoding.UTF8.GetBytes(message);

            var messageRoutingKey = "my.topic.key";
            await channel.BasicPublishAsync(exchange: "topic_exchange",
                                     routingKey: messageRoutingKey,
                                     body: body);
            Console.WriteLine(" [Producer] Sent '{0}' to '{1}' with routing key '{2}'", message, "topic_exchange", messageRoutingKey);

            // Consumer
            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += async (model, ea) =>
            {
                var receivedBody = ea.Body.ToArray();
                var receivedMessage = Encoding.UTF8.GetString(receivedBody);
                Console.WriteLine(" [Consumer] Received '{0}' from queue '{1}' with routing key '{2}'", receivedMessage, queueName, ea.RoutingKey);
                await Task.Yield();
            };

            await channel.BasicConsumeAsync(queue: queueName,
                                 autoAck: true,
                                 consumer: consumer);
        }
    }
}
