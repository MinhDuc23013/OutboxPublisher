using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OutboxPublisherBackgroundService.Kafka
{
    public class KafkaOptions
    {
        public string BootstrapServers { get; set; } = "";
        public string ClientId { get; set; } = "";
    }
}
