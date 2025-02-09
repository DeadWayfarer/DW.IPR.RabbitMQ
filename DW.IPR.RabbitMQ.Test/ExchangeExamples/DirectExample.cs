using RabbitMQ.Client.Events;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DW.IPR.RabbitMQ.Test.ExchangeExamples
{
    public static class DirectExample
    {
        async public static Task StartExample()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using var connection = await factory.CreateConnectionAsync();
            using var channel = await connection.CreateChannelAsync();

            // Объявляем exchange (если он уже существует, это не создаст новый)
            await channel.ExchangeDeclareAsync(exchange: "amq.direct", type: ExchangeType.Direct, durable: true);

            // Создаем очередь и связываем ее с exchange по routing key
            var queueName = "direct_queue";
            await channel.QueueDeclareAsync(queue: queueName,
                                    durable: false,
                                    exclusive: false,
                                    autoDelete: false,
                                    arguments: null);

            var routingKey = "my.routing.key";
            await channel.QueueBindAsync(queue: queueName,
                              exchange: "amq.direct",
                              routingKey: routingKey);

            // Producer
            string message = "Hello RabbitMQ with Direct Exchange!";
            var body = Encoding.UTF8.GetBytes(message);

            await channel.BasicPublishAsync(exchange: "amq.direct",
                                     routingKey: routingKey,
                                     body: body);
            Console.WriteLine(" [Producer] Sent '{0}' to '{1}' with routing key '{2}'", message, "amq.direct", routingKey);

            // Consumer
            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += async (model, ea) =>
            {
                var receivedBody = ea.Body.ToArray();
                var receivedMessage = Encoding.UTF8.GetString(receivedBody);
                Console.WriteLine(" [Consumer] Received '{0}' from queue '{1}'", receivedMessage, queueName);
                await Task.Yield(); // Для асинхронного контекста
            };

            await channel.BasicConsumeAsync(queue: queueName,
                                 autoAck: true,
                                 consumer: consumer);
        }
    }
}
