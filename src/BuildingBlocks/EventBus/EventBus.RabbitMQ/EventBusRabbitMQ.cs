using EventBus.Base.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventBus.RabbitMQ
{
    public class EventBusRabbitMQ : BaseEventBus
    {
        public override void Publish(IntegrationEvent @event)
        {
            
        }

        public override void Subscribe<T, TH>()
        {
            
        }

        public override void UnSubscribe<T, TH>()
        {
            
        }
    }
}
