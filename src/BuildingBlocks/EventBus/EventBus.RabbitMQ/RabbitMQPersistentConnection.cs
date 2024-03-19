using Polly;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace EventBus.RabbitMQ
{
    public class RabbitMQPersistentConnection:IDisposable
    {
        private IConnection connection;
        private readonly IConnectionFactory connectionFactory;
        private readonly object lock_object =new object();
        private readonly int retryCount;
        private bool _disposed;

        public RabbitMQPersistentConnection(IConnectionFactory connectionFactory,int retryCount=5)
        {
            this.connectionFactory = connectionFactory;
            this.retryCount = retryCount;
        }

        
        public bool IsConnected=>connection!=null && connection.IsOpen;

        public IModel CreateModel()
        {
            return connection.CreateModel();
        }

        public void Dispose()
        {
            connection.Dispose();
            _disposed = true;
        }

        public bool TryConnect()
        {
            lock (lock_object)
            {
                var policy = Policy.Handle<SocketException>()
                    .Or<BrokerUnreachableException>()
                    .WaitAndRetry(retryCount, retryAttemp => TimeSpan.FromSeconds(Math.Pow(2, retryAttemp)), (ex, time) =>
                    {

                    });

                policy.Execute(() =>
                {
                    connection = connectionFactory.CreateConnection();
                    connection.CallbackException += Connection_CallbackException;
                    connection.ConnectionBlocked += Connection_ConnectionBlocked;

                    return true;
                });

                if (IsConnected)
                {
                    connection.ConnectionShutdown += Connection_ConnectionShutdown;
                                    
                }
                return false;

            }

        }

        private void Connection_ConnectionBlocked(object sender, global::RabbitMQ.Client.Events.ConnectionBlockedEventArgs e)
        {
            if (_disposed) return;
        }

        private void Connection_CallbackException(object sender, global::RabbitMQ.Client.Events.CallbackExceptionEventArgs e)
        {
            throw new NotImplementedException();
        }

        private void Connection_ConnectionShutdown1(object sender, ShutdownEventArgs e)
        {
            throw new NotImplementedException();
        }

        private void Connection_ConnectionShutdown(object sender,ShutdownEventArgs e)
        {
            //log connection_ConnectionShutdown
            TryConnect();
        }
    }
}
