using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace OutboxPublisherBackgroundService.Kafka
{
    public class KafkaProducer : IKafkaProducer
    {
        private readonly IProducer<string, string> _producer;
        private readonly ILogger<KafkaProducer> _logger;

        public KafkaProducer(IOptions<KafkaOptions> options, ILogger<KafkaProducer> logger)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = options.Value.BootstrapServers,
                ClientId = options.Value.ClientId
            };

            _producer = new ProducerBuilder<string, string>(config).Build();
            _logger = logger;
        }

        public async Task<bool> PublishOrderCreatedAsync(
            object order,
            string topic,
            CancellationToken ct)
        {
            var message = new Message<string, string>
            {
                Key = Guid.NewGuid().ToString(),
                Value = JsonSerializer.Serialize(order)
            };

            try
            {
                var result = await _producer.ProduceAsync(topic, message, ct);

                if (result.Status == PersistenceStatus.Persisted)
                {
                    _logger.LogInformation(
                        "Kafka published: {Topic} {Key}",
                        topic,
                        message.Key);

                    return true;
                }

                _logger.LogWarning(
                    "Kafka not persisted: {Topic} {Key}",
                    topic,
                    message.Key);

                return false;
            }
            catch (ProduceException<string, string> ex)
            {
                _logger.LogError(ex,
                    "Kafka publish failed: {Topic} {Key}",
                    topic,
                    message.Key);

                return false;
            }
        }
    }
}
