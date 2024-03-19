using EventBus.Base;
using EventBus.Base.Events;
using Newtonsoft.Json;
using Polly;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace EventBus.RabbitMQ
{
    public class EventBusRabbitMQ : BaseEventBus
    {
        RabbitMQPersistentConnection persistentConnection;
        private readonly IConnectionFactory connectionFactory;
        private readonly IModel consumerChannel;

        public EventBusRabbitMQ(EventBusConfig config ,IServiceProvider serviceProvider):base(config,serviceProvider)
        {
            if (config.Connection != null)
            {
                var connJson = JsonConvert.SerializeObject(eventBusConfig, new JsonSerializerSettings
                {
                    ReferenceLoopHandling = ReferenceLoopHandling.Ignore
                });

            }
            else
                connectionFactory = new ConnectionFactory();
            persistentConnection = new RabbitMQPersistentConnection(connectionFactory,config.ConnectionRetryCount);

            consumerChannel = CreateConsumerChannel();
        }

        public override void Publish(IntegrationEvent @event)
        {
            if(!persistentConnection.IsConnected)
            {
                persistentConnection.TryConnect();
            }
            var policy = Policy.Handle<BrokerUnreachableException>()
                .Or<SocketException>()
                .WaitAndRetry(eventBusConfig.ConnectionRetryCount, retryAttemp => TimeSpan.FromSeconds(Math.Pow(2, retryAttemp)), (ex, time) =>
                {
                    //logging
                });
            var eventName =@event.GetType().Name;
            eventName=ProcessEventName(eventName);

            consumerChannel.ExchangeDeclare(exchange: eventBusConfig.DefaultTopicName, type: "direct");

            var message =JsonConvert.SerializeObject(@event);
            var body =Encoding.UTF8.GetBytes(message);

            policy.Execute(()
                =>
            {
                var properties = consumerChannel.CreateBasicProperties();
                properties.DeliveryMode = 2;

                consumerChannel.QueueDeclare(queue: GetSubName(eventName),
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null
                    );
            });
        }

        public override void Subscribe<T, TH>()
        {
            var eventName =typeof(T).Name;
            eventName=ProcessEventName(eventName);

            if (!SubsManager.HasSubscriptionsForEvent(eventName))
            {
                if(persistentConnection.IsConnected)
                {
                    persistentConnection.TryConnect();
                }
                consumerChannel.QueueDeclare(queue: GetSubName(eventName),
                    durable:true,
                    autoDelete:false,
                    exclusive:false,
                    arguments:null
                    );
                consumerChannel.QueueBind(queue:GetSubName(eventName),
                    exchange:eventBusConfig.DefaultTopicName,
                    routingKey:eventName
                    );
            }
            SubsManager.AddSubscription<T,TH>();
            StartBasicConsume(eventName);

        }

        public override void UnSubscribe<T, TH>()
        {
            SubsManager.RemoveSubscription<T,TH>();
        }

        private IModel CreateConsumerChannel()
        {
            if (!persistentConnection.IsConnected)
            {
                persistentConnection.TryConnect();
            }
            var channel =persistentConnection.CreateModel();
            channel.ExchangeDeclare(exchange: eventBusConfig.DefaultTopicName, type: "direct");
            return channel;
        }

        private void StartBasicConsume(string eventName)
        {
            if(consumerChannel != null)
            {
                var consumer =new EventingBasicConsumer (consumerChannel);
                consumer.Received += Consumer_Received;

                consumerChannel.BasicConsume(
                    queue: GetSubName(eventName),
                    autoAck: false,
                    consumer: consumer);
            }
        }

        private async void Consumer_Received(object sender, BasicDeliverEventArgs eventArgs)
        {
            var eventName = eventArgs.RoutingKey;
            eventName=ProcessEventName(eventName);
            var message = Encoding.UTF8.GetString(eventArgs.Body.Span);

            try
            {
                await ProcessEvent(eventName, message);
            }catch (Exception ex)
            {
                //logging
            }
        }
        private void StartBasicConsumer(string eventName)
        {
            var consumer =new EventingBasicConsumer(consumerChannel);
            consumer .Received += Consumer_Received;

            consumerChannel.BasicConsume(
                queue: GetSubName(eventName),
                autoAck: false,
                consumer: consumer
                );
        }
    }
}
