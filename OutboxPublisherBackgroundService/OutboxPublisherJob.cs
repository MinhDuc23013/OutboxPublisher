using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OutboxPublisherBackgroundService.Kafka;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using static Confluent.Kafka.ConfigPropertyNames;

namespace OutboxPublisherBackgroundService
{
    public static class Telemetry
    {
        public static readonly ActivitySource Source =
            new("loan-service");
    }

    public class OutboxPublisherJob : BackgroundService
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly IRabbitMqPublisher _publisher;
        private readonly ILogger<OutboxPublisherJob> _logger;
        private readonly IKafkaProducer _kafkaProducer;

        private const int BatchSize = 100;

        public OutboxPublisherJob(
            IServiceScopeFactory scopeFactory,
            IRabbitMqPublisher publisher,
            IKafkaProducer kafkaProducer,
            ILogger<OutboxPublisherJob> logger)
        {
            _scopeFactory = scopeFactory;
            _publisher = publisher;
            _logger = logger;
            _kafkaProducer = kafkaProducer;
        }

        protected override async Task ExecuteAsync(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var processed = await PublishBatchKafka(ct);

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

        private async Task<int> PublishBatchKafka(CancellationToken ct)
        {
            using var scope = _scopeFactory.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

            await using var tx = await db.Database.BeginTransactionAsync(ct);

            var items = await db.OutboxMessages
                .FromSqlRaw(@"
            SELECT *
            FROM outbox_messages
            WHERE ""PublishedAt"" IS NULL
              AND ""Status"" = 'Draft'
              AND ""Type"" = 'LoanApplicationCreated'
              AND ""Destination"" = 'loan-applications'
            ORDER BY ""OccurredAt""
            FOR UPDATE SKIP LOCKED
            LIMIT 100")
                .ToListAsync(ct);
            const string topic = "loan.created.draft";

            int successCount = 0;

            foreach (var item in items)
            {
                ct.ThrowIfCancellationRequested();

                try
                {
                    var result = await _kafkaProducer.PublishOrderCreatedAsync(item, topic, ct);
                    item.PublishedAt = DateTime.UtcNow;
                    if (result)
                        successCount++;
                }
                catch (ProduceException<string, string> ex)
                {
                    _logger.LogError(ex, "Kafka publish failed for Id {Id}", item.Id);
                }
            }

            await db.SaveChangesAsync(ct);
            await tx.CommitAsync(ct);

            if (successCount > 0)
                _logger.LogInformation("Published {count} outbox messages", successCount);

            return successCount;
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

            ActivityContext parentContext = default;
            var hasParent = !string.IsNullOrWhiteSpace(msg.TraceParent);

            if (hasParent)
            {
                parentContext = ActivityContext.Parse(
                    msg.TraceParent,
                    msg.TraceState);
            }

            using var activity = Telemetry.Source.StartActivity(
                "outbox.publish",
                ActivityKind.Producer,
                hasParent ? parentContext : default);

            activity?.SetTag("messaging.system", "outbox");
            activity?.SetTag("messaging.destination", msg.Destination);
            activity?.SetTag("messaging.message_id", msg.Id);

            for (int i = 0; i < maxRetry; i++)
            {
                try
                {
                    _logger.LogInformation(
                        "Publishing message {MessageId} attempt {Attempt}",
                        msg.Id,
                        i + 1);

                    var headers = new Dictionary<string, object>();

                    if (activity != null)
                    {
                        headers["traceparent"] = activity.Id;
                        headers["tracestate"] = activity.TraceStateString;
                    }

                    await _publisher.PublishAsync(
                        exchange: "",
                        routingKey: msg.Destination,
                        payload: msg.Payload,
                        messageId: msg.Id
                        //headers: headers
                        );

                    _logger.LogInformation(
                        "Published message {MessageId} TraceId={TraceId}",
                        msg.Id,
                        activity?.TraceId);

                    return;
                }
                catch (Exception ex)
                {
                    activity?.SetTag("retry.attempt", i + 1);
                    activity?.SetTag("error", true);
                    activity?.SetTag("error.message", ex.Message);

                    _logger.LogWarning(ex,
                        "Publish failed for message {MessageId} attempt {Attempt}",
                        msg.Id,
                        i + 1);

                    if (i == maxRetry - 1)
                    {
                        _logger.LogError(ex,
                            "Publish permanently failed for message {MessageId}",
                            msg.Id);
                        throw;
                    }

                    await Task.Delay(200 * (i + 1), ct);
                }
            }
        }
    }
}