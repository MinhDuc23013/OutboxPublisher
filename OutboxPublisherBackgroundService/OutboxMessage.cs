using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OutboxPublisherBackgroundService
{
    public class OutboxMessage
    {
        public Guid Id { get; set; }
        public string Type { get; set; } = default!;
        public string Destination { get; set; } = default!; // exchange / queue
        public string Payload { get; set; } = default!;
        public DateTime OccurredAt { get; set; }
        public DateTime? PublishedAt { get; set; }
        public string Status { get; set; }
    }

}
