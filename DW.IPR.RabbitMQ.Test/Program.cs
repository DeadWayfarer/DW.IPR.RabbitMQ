using DW.IPR.RabbitMQ.Test.ExchangeExamples;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

class Program
{
    static void Main(string[] args)
    {
        GoToAsync().Wait();
        Console.WriteLine(" Press [enter] to exit.");
        Console.ReadLine();
    }

    async static Task GoToAsync()
    {
        await AMQPDecaultExample.StartExample();
        //await DirectExample.StartExample();
        //await FanoutExample.StartExample();
        //await TopicExample.StartExample();
        //await HeadersExample.StartExample();
    }
}