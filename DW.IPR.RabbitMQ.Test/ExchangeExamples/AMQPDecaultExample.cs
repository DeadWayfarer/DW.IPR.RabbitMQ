using RabbitMQ.Client.Events;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DW.IPR.RabbitMQ.Test.ExchangeExamples
{
    public static class AMQPDecaultExample
    {
        async public static Task StartExample()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using var connection = await factory.CreateConnectionAsync();
            using var channel = await connection.CreateChannelAsync();
            await channel.QueueDeclareAsync(queue: "hello",
                                            durable: false,
                                            exclusive: false,
                                            autoDelete: false,
                                            arguments: null);

            // Producer
            string message = "Hello RabbitMQ!";
            var body = Encoding.UTF8.GetBytes(message);
            var bodyMessage = new ReadOnlyMemory<byte>(body);

            await channel.BasicPublishAsync("", "hello", body);
            Console.WriteLine(" [Producer] Sent {0}", message);

            // Consumer
            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += async (model, ea) =>
            {
                var receivedBody = ea.Body.ToArray();
                var receivedMessage = Encoding.UTF8.GetString(receivedBody);
                Console.WriteLine(" [Consumer] Received {0}", receivedMessage);
                await Task.Yield();
            };

            await channel.BasicConsumeAsync(queue: "hello",
                                            autoAck: true,
                                            consumer: consumer);
        }
    }
}
