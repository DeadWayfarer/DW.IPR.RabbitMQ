using RabbitMQ.Client.Events;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Security.AccessControl;

namespace DW.IPR.RabbitMQ.Test.ExchangeExamples
{
    public static class HeadersExample
    {
        async public static Task StartExample()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using var connection = await factory.CreateConnectionAsync();
            using var channel = await connection.CreateChannelAsync();

            // Объявляем exchange типа headers
            await channel.ExchangeDeclareAsync(exchange: "headers_exchange", type: ExchangeType.Topic, durable: true);

            // Создаем очередь и связываем ее с exchange по headers
            var queueName1 = "headers_queue_eu";
            var queueArgs1 = new Dictionary<string, object?>() { { "x-match", "all" }, { "region", "eu" } };

            var queueName2 = "headers_queue_ru";
            var queueArgs2 = new Dictionary<string, object?>() { { "x-match", "all" }, { "region", "eu" } };

            await DeclareQueue(channel, queueName1, queueArgs1);
            await DeclareQueue(channel, queueName2, queueArgs2);

            // Producer
            string message = "Hello RabbitMQ with Headers Exchange!";
            var body = Encoding.UTF8.GetBytes(message);
            var messageRoutingKey = "my.headers";

            var properties1 = new BasicProperties();
            properties1.Headers = queueArgs1;
            await channel.BasicPublishAsync(exchange: "headers_exchange",
                                             routingKey: messageRoutingKey,
                                             body: body,
                                             mandatory: false,
                                             basicProperties: properties1
                                             );

            var properties2 = new BasicProperties();
            properties2.Headers = queueArgs2;
            Console.WriteLine(" [Producer] Sent '{0}' to '{1}' with routing key '{2}' with region '{3}'", message, "headers_exchange", messageRoutingKey, queueArgs1["region"]);


            await channel.BasicPublishAsync(exchange: "headers_exchange",
                                             routingKey: messageRoutingKey,
                                             body: body,
                                             mandatory: false,
                                             basicProperties: properties2
                                             );
            Console.WriteLine(" [Producer] Sent '{0}' to '{1}' with routing key '{2}' with region '{3}'", message, "headers_exchange", messageRoutingKey, queueArgs2["region"]);

            // Consumer
            var consumer1 = new AsyncEventingBasicConsumer(channel);
            consumer1.ReceivedAsync += async (model, ea) =>
            {
                var receivedBody = ea.Body.ToArray();
                var receivedMessage = Encoding.UTF8.GetString(receivedBody);
                Console.WriteLine(" [Consumer] Received '{0}' from queue '{1}' with routing key '{2}'", receivedMessage, queueName1, ea.RoutingKey);
                await Task.Yield();
            };

            await channel.BasicConsumeAsync(queue: queueName1,
                                 autoAck: true,
                                 consumer: consumer1);

            var consumer2 = new AsyncEventingBasicConsumer(channel);
            consumer2.ReceivedAsync += async (model, ea) =>
            {
                var receivedBody = ea.Body.ToArray();
                var receivedMessage = Encoding.UTF8.GetString(receivedBody);
                Console.WriteLine(" [Consumer] Received '{0}' from queue '{1}' with routing key '{2}'", receivedMessage, queueName2, ea.RoutingKey);
                await Task.Yield();
            };

            await channel.BasicConsumeAsync(queue: queueName1,
                                 autoAck: true,
                                 consumer: consumer2);
        }

        async static private Task DeclareQueue(IChannel channel, string queueName, IDictionary<string, object?> queueArgs)
        {
            await channel.QueueDeclareAsync(queue: queueName,
                                            durable: false,
                                            exclusive: false,
                                            autoDelete: false,
                                            arguments: null);

            var routingKey = "my.headers";
            await channel.QueueBindAsync(queue: queueName,
                              exchange: "headers_exchange",
                              routingKey: routingKey,
                              queueArgs);

            await Task.Yield();
        }
    }
}
