using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using OutboxPublisherBackgroundService;
using OutboxPublisherBackgroundService.Kafka;
using RabbitMQ.Client;
using Serilog;
using Serilog.Enrichers.Span;

var builder = Host.CreateApplicationBuilder(args);


Log.Logger = new LoggerConfiguration()
    .Enrich.FromLogContext()
    .Enrich.WithSpan() // lấy TraceId từ Activity.Current
    .WriteTo.Console(outputTemplate:
        "[{Timestamp:HH:mm:ss} {Level:u3}] " +
        "[TraceId={TraceId} SpanId={SpanId}] " +
        "{Message:lj}{NewLine}{Exception}")
    .CreateLogger();

// ⭐ dùng Services thay vì builder.Host
builder.Services.AddSerilog(Log.Logger);

// ⭐ dùng Services thay vì builder.Host
builder.Services.AddSerilog(Log.Logger);

var endpointTracing = builder.Configuration["Otel:EndpointTracing"]
               ?? "http://localhost:4318";

var endpointMetric = builder.Configuration["Otel:EndpointMetric"]
               ?? "http://localhost:4318";

Console.WriteLine("OTEL ENDPOINT TRACING = " + endpointTracing);
Console.WriteLine("OTEL ENDPOINT METRIC = " + endpointMetric);

builder.Services.AddOpenTelemetry()
    .ConfigureResource(resource => resource
        .AddService(serviceName: "loan-service"))
    .WithTracing(tracing => tracing
        .SetSampler(new AlwaysOnSampler())
        .AddAspNetCoreInstrumentation()
        .AddHttpClientInstrumentation()
        .AddSource("loan-service")
        .AddOtlpExporter(opt =>
        {
            opt.Endpoint = new Uri(endpointTracing);
            opt.Protocol = OtlpExportProtocol.HttpProtobuf;
            opt.ExportProcessorType = ExportProcessorType.Batch;
            opt.HttpClientFactory = () =>
            {
                var handler = new HttpClientHandler();
                handler.ServerCertificateCustomValidationCallback =
                    HttpClientHandler.DangerousAcceptAnyServerCertificateValidator;
                return new HttpClient(handler);
            };
        })
     )
    .WithMetrics(metrics =>
    {
        metrics
            .AddAspNetCoreInstrumentation()
            .AddHttpClientInstrumentation()
            .AddRuntimeInstrumentation()
            .AddOtlpExporter(opt =>
            {
                opt.Endpoint = new Uri(endpointMetric);
                opt.Protocol = OtlpExportProtocol.HttpProtobuf;
                opt.ExportProcessorType = ExportProcessorType.Batch;
                opt.HttpClientFactory = () =>
                {
                    var handler = new HttpClientHandler();
                    handler.ServerCertificateCustomValidationCallback =
                        HttpClientHandler.DangerousAcceptAnyServerCertificateValidator;
                    return new HttpClient(handler);
                };
            });
    });


builder.Services.Configure<KafkaOptions>(
    builder.Configuration.GetSection("Kafka"));

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
builder.Services.AddSingleton<IKafkaProducer, KafkaProducer>();
builder.Services.AddHostedService<OutboxPublisherJob>();


var host = builder.Build();

host.Run();
