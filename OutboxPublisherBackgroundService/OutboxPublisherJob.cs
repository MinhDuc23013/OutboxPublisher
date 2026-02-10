using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OutboxPublisherBackgroundService
{
    public class OutboxPublisherJob : BackgroundService
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly IRabbitMqPublisher _publisher;
        private readonly ILogger<OutboxPublisherJob> _logger;

        private const int BatchSize = 100;

        public OutboxPublisherJob(
            IServiceScopeFactory scopeFactory,
            IRabbitMqPublisher publisher,
            ILogger<OutboxPublisherJob> logger)
        {
            _scopeFactory = scopeFactory;
            _publisher = publisher;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var processed = await PublishBatch(ct);

                    if (processed == 0)
                        await Task.Delay(1000, ct);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Outbox publisher crashed");
                    await Task.Delay(3000, ct);
                }
            }
        }

        private async Task<int> PublishBatch(CancellationToken ct)
        {
            using var scope = _scopeFactory.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

            await using var tx = await db.Database.BeginTransactionAsync(ct);

            var messages = await db.OutboxMessages
                .FromSqlRaw(@"
                SELECT *
                FROM ""outbox_messages""
                WHERE ""PublishedAt"" IS NULL
                ORDER BY ""OccurredAt""
                LIMIT {0}
                FOR UPDATE SKIP LOCKED", BatchSize)
                .AsTracking()
                .ToListAsync(ct);

            foreach (var msg in messages)
            {
                await PublishMessageToQueue(msg, ct);
                msg.PublishedAt = DateTime.UtcNow;
            }

            await db.SaveChangesAsync(ct);
            await tx.CommitAsync(ct);

            if (messages.Count > 0)
                _logger.LogInformation("Published {count} outbox messages", messages.Count);

            return messages.Count;
        }

        private async Task PublishMessageToQueue(OutboxMessage msg, CancellationToken ct)
        {
            const int maxRetry = 3;

            for (int i = 0; i < maxRetry; i++)
            {
                try
                {
                    await _publisher.PublishAsync(
                        exchange: "",
                        routingKey: msg.Destination,
                        payload: msg.Payload,
                        messageId: msg.Id);

                    return;
                }
                catch
                {
                    if (i == maxRetry - 1) throw;
                    await Task.Delay(200 * (i + 1), ct);
                }
            }
        }
    }
}