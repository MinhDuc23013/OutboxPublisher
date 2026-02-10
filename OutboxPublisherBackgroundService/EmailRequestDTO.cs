using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OutboxPublisherBackgroundService
{
    public class EmailRequestDTO
    {
        public EmailRequestDTO() { }
        public string To { get; set; } = default!;
        public string Template { get; set; } = default!;
        public string Email { get; set; } = default!;
        public string Body { get; set; } = default!;
        public Guid OutboxEmailId { get; set; } = default!;
    }
}
