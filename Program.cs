using System.Text.Json;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSingleton(sp => { var s = sp.GetRequiredService<IConfiguration>().GetSection("Mongo"); return new MongoClient(s["ConnectionString"] ?? "mongodb://localhost:27017").GetDatabase(s["Database"] ?? "oficina_execution"); });
builder.Services.AddHostedService<PaymentConsumer>();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
var app = builder.Build();
app.UseSwagger(); app.UseSwaggerUI();
app.MapGet("/health", () => Results.Ok(new { service = "execution-service", status = "UP" }));
app.MapGet("/api/execution", async (IMongoDatabase db) => Results.Ok(await db.GetCollection<ExecutionOrder>("execution").Find(FilterDefinition<ExecutionOrder>.Empty).ToListAsync()));
app.MapPatch("/api/execution/{orderId:guid}", async (Guid orderId, UpdateExecutionRequest request, IMongoDatabase db) =>
{
    var item = await db.GetCollection<ExecutionOrder>("execution").FindOneAndUpdateAsync(x => x.OrderId == orderId, Builders<ExecutionOrder>.Update.Set(x => x.Status, request.Status).Set(x => x.UpdatedAt, DateTimeOffset.UtcNow));
    return item is null ? Results.NotFound() : Results.Ok(item);
});
app.Run();

public record UpdateExecutionRequest(string Status);
public class ExecutionOrder { [BsonId] public string Id { get; set; } = ObjectId.GenerateNewId().ToString(); public Guid OrderId { get; set; } public string Status { get; set; } = "NA_FILA"; public DateTimeOffset CreatedAt { get; set; } = DateTimeOffset.UtcNow; public DateTimeOffset? UpdatedAt { get; set; } }

public sealed class PaymentConsumer(IConfiguration configuration, IMongoDatabase db, ILogger<PaymentConsumer> logger) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var s = configuration.GetSection("RabbitMq"); var factory = new ConnectionFactory { HostName = s["Host"] ?? "localhost", Port = s.GetValue("Port", 5672), UserName = s["User"] ?? "oficina", Password = s["Password"] ?? "oficina" };
        var connection = factory.CreateConnection(); var channel = connection.CreateModel(); channel.ExchangeDeclare("oficina.events", ExchangeType.Topic, durable: true); var queue = channel.QueueDeclare("execution-service-events", true, false, false).QueueName; channel.QueueBind(queue, "oficina.events", "PAYMENT_APPROVED");
        var consumer = new EventingBasicConsumer(channel); consumer.Received += async (_, args) => { try { using var doc = JsonDocument.Parse(args.Body.ToArray()); var orderId = doc.RootElement.GetProperty("payload").GetProperty("orderId").GetGuid(); await db.GetCollection<ExecutionOrder>("execution").InsertOneAsync(new ExecutionOrder { OrderId = orderId }); channel.BasicAck(args.DeliveryTag, false); } catch (Exception ex) { logger.LogError(ex, "Falha ao inserir OS na execução"); channel.BasicNack(args.DeliveryTag, false, true); } }; channel.BasicConsume(queue, false, consumer); return Task.CompletedTask;
    }
}
