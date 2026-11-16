using System.Text;
using System.Text.Json;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var builder = WebApplication.CreateBuilder(args);
builder.Services.Configure<HostOptions>(options => options.BackgroundServiceExceptionBehavior = BackgroundServiceExceptionBehavior.StopHost);
builder.Services.AddSingleton(sp => { var s = sp.GetRequiredService<IConfiguration>().GetSection("Mongo"); return new MongoClient(s["ConnectionString"] ?? "mongodb://localhost:27017").GetDatabase(s["Database"] ?? "oficina_execution"); });
builder.Services.AddSingleton<EventPublisher>();
builder.Services.AddHostedService<PaymentConsumer>();
builder.Services.AddHostedService<ExecutionOutboxWorker>();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
var app = builder.Build();
app.UseSwagger(); app.UseSwaggerUI();
app.MapGet("/health", () => Results.Ok(new { service = "execution-service", status = "UP" }));
app.MapGet("/api/execution", async (IMongoDatabase db) => Results.Ok(await db.GetCollection<ExecutionOrder>("execution").Find(FilterDefinition<ExecutionOrder>.Empty).SortByDescending(x => x.CreatedAt).ToListAsync()));
app.MapPatch("/api/execution/{orderId:guid}/status", async (Guid orderId, UpdateExecutionRequest request, IMongoDatabase db) =>
{
    if (!ExecutionStatuses.All.Contains(request.Status)) return Results.BadRequest(new { error = "status invalido" });
    var status = ExecutionStatuses.All.First(x => string.Equals(x, request.Status, StringComparison.OrdinalIgnoreCase));
    var collection = db.GetCollection<ExecutionOrder>("execution");
    var current = await collection.Find(x => x.OrderId == orderId).FirstOrDefaultAsync();
    if (current is null) return Results.NotFound();
    if (current.Status == status) return Results.Ok(current);
    if (!ExecutionSaga.CanTransition(current.Status, status)) return Results.Conflict(new { error = "transicao de execucao invalida" });
    var update = Builders<ExecutionOrder>.Update.Set(x => x.Status, status).Set(x => x.UpdatedAt, DateTimeOffset.UtcNow);
    if (status == ExecutionStatuses.Finished) update = update.Push(x => x.PendingEvents, SagaOutboxEvent.Create("EXECUTION_FINISHED", new { orderId }));
    if (status == ExecutionStatuses.Failed) update = update.Push(x => x.PendingEvents, SagaOutboxEvent.Create("EXECUTION_FAILED", new { orderId, reason = request.Reason }));
    var item = await collection.FindOneAndUpdateAsync<ExecutionOrder>(x => x.OrderId == orderId && x.Status == current.Status, update, new FindOneAndUpdateOptions<ExecutionOrder> { ReturnDocument = ReturnDocument.After });
    if (item is null) return Results.Conflict(new { error = "status alterado por outra requisicao; tente novamente" });
    return Results.Ok(item);
});
app.Run();

public record UpdateExecutionRequest(string Status, string? Reason);
public static class ExecutionStatuses { public const string Queued = "NA_FILA"; public const string Diagnosing = "EM_DIAGNOSTICO"; public const string Repairing = "EM_REPARO"; public const string Finished = "FINALIZADA"; public const string Failed = "EXECUCAO_COM_ERRO"; public static readonly HashSet<string> All = new(StringComparer.OrdinalIgnoreCase) { Queued, Diagnosing, Repairing, Finished, Failed }; }
public static class ExecutionSaga
{
    public static bool CanTransition(string current, string next) => (current, next) switch
    {
        (ExecutionStatuses.Queued, ExecutionStatuses.Diagnosing) => true,
        (ExecutionStatuses.Queued, ExecutionStatuses.Repairing) => true,
        (ExecutionStatuses.Diagnosing, ExecutionStatuses.Repairing) => true,
        (ExecutionStatuses.Repairing, ExecutionStatuses.Finished) => true,
        (ExecutionStatuses.Queued, ExecutionStatuses.Failed) => true,
        (ExecutionStatuses.Diagnosing, ExecutionStatuses.Failed) => true,
        (ExecutionStatuses.Repairing, ExecutionStatuses.Failed) => true,
        _ => false
    };
}
public class ExecutionOrder { [BsonId] public string Id { get; set; } = ObjectId.GenerateNewId().ToString(); public Guid OrderId { get; set; } public string Status { get; set; } = ExecutionStatuses.Queued; public DateTimeOffset CreatedAt { get; set; } = DateTimeOffset.UtcNow; public DateTimeOffset? UpdatedAt { get; set; } [System.Text.Json.Serialization.JsonIgnore] public List<SagaOutboxEvent> PendingEvents { get; set; } = []; }
public class SagaOutboxEvent
{
    public string Id { get; set; } = Guid.NewGuid().ToString("N");
    public string Type { get; set; } = "";
    public string Body { get; set; } = "";
    public static SagaOutboxEvent Create(string type, object payload) => new() { Type = type, Body = JsonSerializer.Serialize(new { type, payload, occurredAt = DateTimeOffset.UtcNow }) };
}

public sealed class EventPublisher(IConfiguration configuration)
{
    public Task PublishAsync(string type, string body)
    {
        var s = configuration.GetSection("RabbitMq"); var factory = new ConnectionFactory { HostName = s["Host"] ?? "localhost", Port = s.GetValue("Port", 5672), UserName = s["User"] ?? "oficina", Password = s["Password"] ?? "oficina" }; using var connection = factory.CreateConnection(); using var channel = connection.CreateModel(); channel.ExchangeDeclare("oficina.events", ExchangeType.Topic, durable: true);
        channel.QueueDeclare("os-service-events", true, false, false);
        foreach (var key in new[] { "BUDGET_CREATED", "BUDGET_REJECTED", "PAYMENT_APPROVED", "PAYMENT_REJECTED", "EXECUTION_FINISHED", "EXECUTION_FAILED" }) channel.QueueBind("os-service-events", "oficina.events", key);
        channel.ConfirmSelect(); var properties = channel.CreateBasicProperties(); properties.Persistent = true;
        channel.BasicPublish("oficina.events", type, properties, Encoding.UTF8.GetBytes(body)); channel.WaitForConfirmsOrDie(TimeSpan.FromSeconds(5)); return Task.CompletedTask;
    }
}

public sealed class ExecutionOutboxWorker(IMongoDatabase db, EventPublisher events, ILogger<ExecutionOutboxWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var collection = db.GetCollection<ExecutionOrder>("execution");
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var pending = await collection.Find(Builders<ExecutionOrder>.Filter.Exists("PendingEvents.0")).Limit(50).ToListAsync(stoppingToken);
                foreach (var order in pending)
                    foreach (var item in order.PendingEvents)
                    {
                        await events.PublishAsync(item.Type, item.Body);
                        await collection.UpdateOneAsync(x => x.Id == order.Id, Builders<ExecutionOrder>.Update.PullFilter(x => x.PendingEvents, x => x.Id == item.Id), cancellationToken: stoppingToken);
                    }
            }
            catch (Exception ex) { logger.LogError(ex, "Falha ao publicar outbox de execucao; sera tentado novamente"); }
            await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
        }
    }
}

public sealed class PaymentConsumer(IConfiguration configuration, IMongoDatabase db, ILogger<PaymentConsumer> logger) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var s = configuration.GetSection("RabbitMq"); var factory = new ConnectionFactory { HostName = s["Host"] ?? "localhost", Port = s.GetValue("Port", 5672), UserName = s["User"] ?? "oficina", Password = s["Password"] ?? "oficina" }; var connection = factory.CreateConnection(); var channel = connection.CreateModel(); channel.ExchangeDeclare("oficina.events", ExchangeType.Topic, durable: true); var queue = channel.QueueDeclare("execution-service-events", true, false, false).QueueName; channel.QueueBind(queue, "oficina.events", "OS_READY_FOR_EXECUTION"); channel.BasicQos(0, 1, false);
        var consumer = new EventingBasicConsumer(channel);
        consumer.Received += async (_, args) =>
        {
            try
            {
                using var doc = JsonDocument.Parse(args.Body.ToArray()); var orderId = doc.RootElement.GetProperty("payload").GetProperty("orderId").GetGuid(); var collection = db.GetCollection<ExecutionOrder>("execution"); var existing = await collection.Find(x => x.OrderId == orderId).FirstOrDefaultAsync(); if (existing is null) await collection.InsertOneAsync(new ExecutionOrder { OrderId = orderId }); channel.BasicAck(args.DeliveryTag, false);
            }
            catch (JsonException ex) { logger.LogError(ex, "Evento de execucao malformado; descartando"); channel.BasicReject(args.DeliveryTag, false); }
            catch (Exception ex) { logger.LogError(ex, "Falha ao inserir OS na execucao"); channel.BasicNack(args.DeliveryTag, false, true); }
        };
        channel.BasicConsume(queue, false, consumer); return Task.CompletedTask;
    }
}

public partial class Program { }
