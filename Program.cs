using System.Text;
using System.Text.Json;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSingleton(sp => { var s = sp.GetRequiredService<IConfiguration>().GetSection("Mongo"); return new MongoClient(s["ConnectionString"] ?? "mongodb://localhost:27017").GetDatabase(s["Database"] ?? "oficina_execution"); });
builder.Services.AddSingleton<EventPublisher>();
builder.Services.AddHostedService<PaymentConsumer>();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
var app = builder.Build();
app.UseSwagger(); app.UseSwaggerUI();
app.MapGet("/health", () => Results.Ok(new { service = "execution-service", status = "UP" }));
app.MapGet("/api/execution", async (IMongoDatabase db) => Results.Ok(await db.GetCollection<ExecutionOrder>("execution").Find(FilterDefinition<ExecutionOrder>.Empty).SortByDescending(x => x.CreatedAt).ToListAsync()));
app.MapPatch("/api/execution/{orderId:guid}/status", async (Guid orderId, UpdateExecutionRequest request, IMongoDatabase db, EventPublisher events) =>
{
    if (!ExecutionStatuses.All.Contains(request.Status)) return Results.BadRequest(new { error = "status invalido" });
    var collection = db.GetCollection<ExecutionOrder>("execution");
    var item = await collection.FindOneAndUpdateAsync(x => x.OrderId == orderId, Builders<ExecutionOrder>.Update.Set(x => x.Status, request.Status).Set(x => x.UpdatedAt, DateTimeOffset.UtcNow));
    if (item is null) return Results.NotFound();
    if (request.Status == ExecutionStatuses.Finished) await events.PublishAsync("EXECUTION_FINISHED", new { orderId });
    if (request.Status == ExecutionStatuses.Failed) await events.PublishAsync("EXECUTION_FAILED", new { orderId, reason = request.Reason });
    return Results.Ok(item);
});
app.Run();

public record UpdateExecutionRequest(string Status, string? Reason);
public static class ExecutionStatuses { public const string Queued = "NA_FILA"; public const string Diagnosing = "EM_DIAGNOSTICO"; public const string Repairing = "EM_REPARO"; public const string Finished = "FINALIZADA"; public const string Failed = "EXECUCAO_COM_ERRO"; public static readonly HashSet<string> All = new(StringComparer.OrdinalIgnoreCase) { Queued, Diagnosing, Repairing, Finished, Failed }; }
public class ExecutionOrder { [BsonId] public string Id { get; set; } = ObjectId.GenerateNewId().ToString(); public Guid OrderId { get; set; } public string Status { get; set; } = ExecutionStatuses.Queued; public DateTimeOffset CreatedAt { get; set; } = DateTimeOffset.UtcNow; public DateTimeOffset? UpdatedAt { get; set; } }

public sealed class EventPublisher(IConfiguration configuration)
{
    public Task PublishAsync(string type, object payload)
    {
        var s = configuration.GetSection("RabbitMq"); var factory = new ConnectionFactory { HostName = s["Host"] ?? "localhost", Port = s.GetValue("Port", 5672), UserName = s["User"] ?? "oficina", Password = s["Password"] ?? "oficina" }; using var connection = factory.CreateConnection(); using var channel = connection.CreateModel(); channel.ExchangeDeclare("oficina.events", ExchangeType.Topic, durable: true); channel.BasicPublish("oficina.events", type, null, Encoding.UTF8.GetBytes(JsonSerializer.Serialize(new { type, payload, occurredAt = DateTimeOffset.UtcNow }))); return Task.CompletedTask;
    }
}

public sealed class PaymentConsumer(IConfiguration configuration, IMongoDatabase db, ILogger<PaymentConsumer> logger) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var s = configuration.GetSection("RabbitMq"); var factory = new ConnectionFactory { HostName = s["Host"] ?? "localhost", Port = s.GetValue("Port", 5672), UserName = s["User"] ?? "oficina", Password = s["Password"] ?? "oficina" }; var connection = factory.CreateConnection(); var channel = connection.CreateModel(); channel.ExchangeDeclare("oficina.events", ExchangeType.Topic, durable: true); var queue = channel.QueueDeclare("execution-service-events", true, false, false).QueueName; channel.QueueBind(queue, "oficina.events", "OS_READY_FOR_EXECUTION");
        var consumer = new EventingBasicConsumer(channel);
        consumer.Received += async (_, args) =>
        {
            try
            {
                using var doc = JsonDocument.Parse(args.Body.ToArray()); var orderId = doc.RootElement.GetProperty("payload").GetProperty("orderId").GetGuid(); var collection = db.GetCollection<ExecutionOrder>("execution"); var existing = await collection.Find(x => x.OrderId == orderId).FirstOrDefaultAsync(); if (existing is null) await collection.InsertOneAsync(new ExecutionOrder { OrderId = orderId }); channel.BasicAck(args.DeliveryTag, false);
            }
            catch (Exception ex) { logger.LogError(ex, "Falha ao inserir OS na execucao"); channel.BasicNack(args.DeliveryTag, false, true); }
        };
        channel.BasicConsume(queue, false, consumer); return Task.CompletedTask;
    }
}
