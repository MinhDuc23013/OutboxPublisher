using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using OutboxPublisherBackgroundService;
using RabbitMQ.Client;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddDbContext<AppDbContext>(options =>
{
    options.UseNpgsql(
        builder.Configuration.GetConnectionString("DefaultConnection"));
});

builder.Services.AddSingleton<IConnectionFactory>(_ =>
    new ConnectionFactory
    {
        HostName = "localhost",
        DispatchConsumersAsync = true
    });

builder.Services.AddSingleton<IRabbitMqPublisher, RabbitMqPublisher>();
builder.Services.AddHostedService<OutboxPublisherJob>();


var host = builder.Build();

host.Run();
