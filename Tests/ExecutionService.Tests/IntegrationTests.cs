using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using MongoDB.Driver;
using RabbitMQ.Client;
using Testcontainers.MongoDb;
using Testcontainers.RabbitMq;
using Xunit;

[CollectionDefinition("integration")]
public sealed class IntegrationCollection : ICollectionFixture<IntegrationEnvironment> { }

public sealed class IntegrationEnvironment : IAsyncLifetime
{
    private readonly MongoDbContainer _mongo = new MongoDbBuilder().WithImage("mongo:7").Build();
    private readonly RabbitMqContainer _rabbit = new RabbitMqBuilder().WithImage("rabbitmq:3-management-alpine").WithUsername("test").WithPassword("test").Build();
    private WebApplicationFactory<Program>? _app;

    public IMongoDatabase Database { get; private set; } = null!;
    public HttpClient Client { get; private set; } = null!;
    public string RabbitHost => _rabbit.Hostname;
    public int RabbitPort => _rabbit.GetMappedPublicPort(5672);

    public async Task InitializeAsync()
    {
        await Task.WhenAll(_mongo.StartAsync(), _rabbit.StartAsync());
        var databaseName = "execution_tests_" + Guid.NewGuid().ToString("N");
        Database = new MongoClient(_mongo.GetConnectionString()).GetDatabase(databaseName);
        _app = new WebApplicationFactory<Program>().WithWebHostBuilder(builder =>
        {
            builder.ConfigureAppConfiguration((_, config) => config.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Mongo:ConnectionString"] = _mongo.GetConnectionString(),
                ["Mongo:Database"] = databaseName,
                ["RabbitMq:Host"] = RabbitHost,
                ["RabbitMq:Port"] = RabbitPort.ToString(),
                ["RabbitMq:User"] = "test",
                ["RabbitMq:Password"] = "test"
            }));
            builder.ConfigureTestServices(services =>
            {
                var worker = services.Single(x => x.ImplementationType == typeof(ExecutionOutboxWorker));
                services.Remove(worker);
            });
        });
        Client = _app.CreateClient();
    }

    public async Task DisposeAsync()
    {
        Client?.Dispose();
        if (_app is not null) await _app.DisposeAsync();
        await Task.WhenAll(_mongo.DisposeAsync().AsTask(), _rabbit.DisposeAsync().AsTask());
    }

    public IModel OpenChannel()
    {
        var connection = new ConnectionFactory { HostName = RabbitHost, Port = RabbitPort, UserName = "test", Password = "test" }.CreateConnection();
        return connection.CreateModel();
    }

    public IMongoCollection<ExecutionOrder> Orders => Database.GetCollection<ExecutionOrder>("execution");

    public static async Task UntilAsync(Func<Task<bool>> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        while (!await condition())
        {
            timeout.Token.ThrowIfCancellationRequested();
            await Task.Delay(100, timeout.Token);
        }
    }
}

[Collection("integration")]
public sealed class IntegrationTests(IntegrationEnvironment env)
{
    private static string Path(Guid id) => $"/api/execution/{id}/status";

    private async Task<ExecutionOrder> InsertAsync(string status = ExecutionStatuses.Queued)
    {
        var order = new ExecutionOrder { OrderId = Guid.NewGuid(), Status = status };
        await env.Orders.InsertOneAsync(order);
        return order;
    }

    [Fact]
    public async Task HealthAndList_ReturnServiceAndNewestFirst_WithoutExposingOutbox()
    {
        var health = await env.Client.GetAsync("/health");
        Assert.Equal(HttpStatusCode.OK, health.StatusCode);
        using var healthJson = JsonDocument.Parse(await health.Content.ReadAsStringAsync());
        Assert.Equal("execution-service", healthJson.RootElement.GetProperty("service").GetString());
        Assert.Equal("UP", healthJson.RootElement.GetProperty("status").GetString());

        var older = new ExecutionOrder { OrderId = Guid.NewGuid(), CreatedAt = DateTimeOffset.UtcNow.AddDays(-2) };
        var newer = new ExecutionOrder { OrderId = Guid.NewGuid(), CreatedAt = DateTimeOffset.UtcNow.AddDays(2), PendingEvents = [SagaOutboxEvent.Create("TEST", new { })] };
        await env.Orders.InsertManyAsync([older, newer]);
        var response = await env.Client.GetAsync("/api/execution");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        using var json = JsonDocument.Parse(body);
        Assert.Equal(newer.OrderId.ToString(), json.RootElement[0].GetProperty("orderId").GetString());
        Assert.Contains(json.RootElement.EnumerateArray(), x => x.GetProperty("orderId").GetString() == older.OrderId.ToString());
        Assert.DoesNotContain("pendingEvents", body, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Patch_RejectsInvalidMissingAndIllegalTransitions()
    {
        var order = await InsertAsync();
        Assert.Equal(HttpStatusCode.BadRequest, (await env.Client.PatchAsJsonAsync(Path(order.OrderId), new UpdateExecutionRequest("UNKNOWN", null))).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await env.Client.PatchAsJsonAsync(Path(Guid.NewGuid()), new UpdateExecutionRequest(ExecutionStatuses.Diagnosing, null))).StatusCode);
        Assert.Equal(HttpStatusCode.Conflict, (await env.Client.PatchAsJsonAsync(Path(order.OrderId), new UpdateExecutionRequest(ExecutionStatuses.Finished, null))).StatusCode);
        var unchanged = await env.Orders.Find(x => x.OrderId == order.OrderId).SingleAsync();
        Assert.Equal(ExecutionStatuses.Queued, unchanged.Status);
        Assert.Empty(unchanged.PendingEvents);
    }

    [Fact]
    public async Task Patch_AdvancesWorkflow_CanonicalizesCase_AndEmitsCompletionOnce()
    {
        var order = await InsertAsync();
        foreach (var status in new[] { "em_diagnostico", ExecutionStatuses.Repairing, "finalizada" })
        {
            var response = await env.Client.PatchAsJsonAsync(Path(order.OrderId), new UpdateExecutionRequest(status, null));
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        }
        var finished = await env.Orders.Find(x => x.OrderId == order.OrderId).SingleAsync();
        Assert.Equal(ExecutionStatuses.Finished, finished.Status);
        Assert.NotNull(finished.UpdatedAt);
        Assert.Single(finished.PendingEvents);
        Assert.Equal("EXECUTION_FINISHED", finished.PendingEvents[0].Type);
        using var eventJson = JsonDocument.Parse(finished.PendingEvents[0].Body);
        Assert.Equal(order.OrderId.ToString(), eventJson.RootElement.GetProperty("payload").GetProperty("orderId").GetString());
        Assert.Equal("EXECUTION_FINISHED", eventJson.RootElement.GetProperty("type").GetString());
        Assert.Equal(HttpStatusCode.OK, (await env.Client.PatchAsJsonAsync(Path(order.OrderId), new UpdateExecutionRequest(ExecutionStatuses.Finished, null))).StatusCode);
        Assert.Equal(HttpStatusCode.Conflict, (await env.Client.PatchAsJsonAsync(Path(order.OrderId), new UpdateExecutionRequest(ExecutionStatuses.Failed, "late"))).StatusCode);
        Assert.Single((await env.Orders.Find(x => x.OrderId == order.OrderId).SingleAsync()).PendingEvents);
    }

    [Theory]
    [InlineData(ExecutionStatuses.Queued)]
    [InlineData(ExecutionStatuses.Diagnosing)]
    [InlineData(ExecutionStatuses.Repairing)]
    public async Task Failure_FromEachNonterminalState_RecordsReasonAndCannotReopen(string initial)
    {
        var order = await InsertAsync(initial);
        var response = await env.Client.PatchAsJsonAsync(Path(order.OrderId), new UpdateExecutionRequest(ExecutionStatuses.Failed, "parts unavailable"));
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var failed = await env.Orders.Find(x => x.OrderId == order.OrderId).SingleAsync();
        Assert.Equal(ExecutionStatuses.Failed, failed.Status);
        Assert.Single(failed.PendingEvents);
        using var json = JsonDocument.Parse(failed.PendingEvents[0].Body);
        Assert.Equal("parts unavailable", json.RootElement.GetProperty("payload").GetProperty("reason").GetString());
        Assert.Equal(HttpStatusCode.Conflict, (await env.Client.PatchAsJsonAsync(Path(order.OrderId), new UpdateExecutionRequest(ExecutionStatuses.Repairing, null))).StatusCode);
    }

    [Fact]
    public async Task Consumer_InsertsOnceForDuplicateDelivery_AndDiscardsMalformedEvent()
    {
        using var channel = env.OpenChannel();
        var id = Guid.NewGuid();
        var sentinel = Guid.NewGuid();
        channel.BasicPublish("oficina.events", "OS_READY_FOR_EXECUTION", null, Encoding.UTF8.GetBytes("{broken"));
        var body = JsonSerializer.Serialize(new { payload = new { orderId = id } });
        channel.BasicPublish("oficina.events", "OS_READY_FOR_EXECUTION", null, Encoding.UTF8.GetBytes(body));
        channel.BasicPublish("oficina.events", "OS_READY_FOR_EXECUTION", null, Encoding.UTF8.GetBytes(body));
        channel.BasicPublish("oficina.events", "OS_READY_FOR_EXECUTION", null, Encoding.UTF8.GetBytes(JsonSerializer.Serialize(new { payload = new { orderId = sentinel } })));
        await IntegrationEnvironment.UntilAsync(async () => await env.Orders.CountDocumentsAsync(x => x.OrderId == sentinel) == 1);
        Assert.Equal(1, await env.Orders.CountDocumentsAsync(x => x.OrderId == id));
        Assert.Equal(ExecutionStatuses.Queued, (await env.Orders.Find(x => x.OrderId == id).SingleAsync()).Status);
    }

    [Fact]
    public async Task Outbox_RetainsEventOnPublishFailure_ThenPublishesAndRemovesIt()
    {
        // Separate database prevents the application's hosted worker from racing this retry test.
        var database = new MongoClient(env.Database.Client.Settings).GetDatabase("outbox_tests_" + Guid.NewGuid().ToString("N"));
        var orders = database.GetCollection<ExecutionOrder>("execution");
        var order = new ExecutionOrder { OrderId = Guid.NewGuid(), PendingEvents = [SagaOutboxEvent.Create("EXECUTION_FINISHED", new { orderId = Guid.NewGuid() })] };
        await orders.InsertOneAsync(order);
        var badConfig = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?> { ["RabbitMq:Host"] = "127.0.0.1", ["RabbitMq:Port"] = "1" }).Build();
        using (var failedWorker = new ExecutionOutboxWorker(database, new EventPublisher(badConfig), NullLogger<ExecutionOutboxWorker>.Instance))
        {
            await failedWorker.StartAsync(CancellationToken.None);
            await Task.Delay(1000);
            Assert.Single((await orders.Find(x => x.Id == order.Id).SingleAsync()).PendingEvents);
            await failedWorker.StopAsync(CancellationToken.None);
        }
        var goodConfig = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["RabbitMq:Host"] = env.RabbitHost, ["RabbitMq:Port"] = env.RabbitPort.ToString(),
            ["RabbitMq:User"] = "test", ["RabbitMq:Password"] = "test"
        }).Build();
        using var worker = new ExecutionOutboxWorker(database, new EventPublisher(goodConfig), NullLogger<ExecutionOutboxWorker>.Instance);
        await worker.StartAsync(CancellationToken.None);
        await IntegrationEnvironment.UntilAsync(async () => (await orders.Find(x => x.Id == order.Id).SingleAsync()).PendingEvents.Count == 0);
        await worker.StopAsync(CancellationToken.None);
        using var channel = env.OpenChannel();
        var delivery = channel.BasicGet("os-service-events", true);
        Assert.NotNull(delivery);
        Assert.Equal("EXECUTION_FINISHED", delivery.RoutingKey);
        Assert.Equal(2, delivery.BasicProperties.DeliveryMode);
        Assert.Equal(order.PendingEvents[0].Body, Encoding.UTF8.GetString(delivery.Body.ToArray()));
    }
}
