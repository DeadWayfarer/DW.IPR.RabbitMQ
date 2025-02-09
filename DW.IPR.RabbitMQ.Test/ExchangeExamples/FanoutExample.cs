using RabbitMQ.Client.Events;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DW.IPR.RabbitMQ.Test.ExchangeExamples
{
    public static class FanoutExample
    {
        async public static Task StartExample()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using var connection = await factory.CreateConnectionAsync();
            using var channel = await connection.CreateChannelAsync();

            // Объявляем exchange типа fanout
            await channel.ExchangeDeclareAsync(exchange: "fanout_exchange", type: ExchangeType.Fanout, durable: true);

            // Создаем очередь и связываем ее с exchange
            var queueName = "fanout_queue";
            await channel.QueueDeclareAsync(queue: queueName,
                                    durable: false,
                                    exclusive: false,
                                    autoDelete: false,
                                    arguments: null);

            await channel.QueueBindAsync(queue: queueName,
                              exchange: "fanout_exchange",
                              routingKey: "");

            // Producer
            string message = "Hello RabbitMQ with Fanout Exchange!";
            var body = Encoding.UTF8.GetBytes(message);

            await channel.BasicPublishAsync(exchange: "fanout_exchange",
                                     routingKey: "",
                                     body: body);
            Console.WriteLine(" [Producer] Sent '{0}' to '{1}'", message, "fanout_exchange");

            // Consumer
            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += async (model, ea) =>
            {
                var receivedBody = ea.Body.ToArray();
                var receivedMessage = Encoding.UTF8.GetString(receivedBody);
                Console.WriteLine(" [Consumer] Received '{0}' from queue '{1}'", receivedMessage, queueName);
                await Task.Yield();
            };

            await channel.BasicConsumeAsync(queue: queueName,
                                 autoAck: true,
                                 consumer: consumer);
        }
    }
}
