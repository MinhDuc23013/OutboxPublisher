using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OutboxPublisherBackgroundService.Kafka
{
    public interface IKafkaProducer
    {
        Task<bool> PublishOrderCreatedAsync(object order, string topic, CancellationToken ct);
    }
}
