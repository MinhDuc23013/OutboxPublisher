using RabbitMQ.Client;
using System.Text;

namespace OutboxPublisherBackgroundService
{
    public interface IRabbitMqPublisher
    {
        Task PublishAsync(string exchange, string routingKey, string payload, Guid messageId);
    }

    public class RabbitMqPublisher : IRabbitMqPublisher, IDisposable
    {
        private readonly IConnection _connection;
        private readonly IModel _channel;

        public RabbitMqPublisher(IConnectionFactory factory)
        {
            _connection = factory.CreateConnection();
            _channel = _connection.CreateModel();
        }

        public Task PublishAsync(string exchange, string routingKey, string payload, Guid messageId)
        {
            var props = _channel.CreateBasicProperties();
            props.Persistent = true;
            props.MessageId = messageId.ToString();
            props.ContentType = "application/json";

            var body = Encoding.UTF8.GetBytes(payload);

            //_channel.BasicPublish(
            //    exchange: exchange,
            //    routingKey: routingKey,
            //    basicProperties: props,
            //    body: body);

            _channel.BasicPublish(
                exchange: "",
                routingKey: routingKey,
                basicProperties: props,
                body: body);

            return Task.CompletedTask;
        }

        public void Dispose()
        {
            _channel.Dispose();
            _connection.Dispose();
        }
    }
}
