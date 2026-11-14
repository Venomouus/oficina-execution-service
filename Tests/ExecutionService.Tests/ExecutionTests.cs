using System;
using Xunit;

public class ExecutionTests
{
    [Fact]
    public void ExecutionStatuses_Contain_Complete_Workflow()
    {
        Assert.Contains(ExecutionStatuses.Queued, ExecutionStatuses.All);
        Assert.Contains(ExecutionStatuses.Diagnosing, ExecutionStatuses.All);
        Assert.Contains(ExecutionStatuses.Repairing, ExecutionStatuses.All);
        Assert.Contains(ExecutionStatuses.Finished, ExecutionStatuses.All);
    }

    [Fact]
    public void NewExecutionOrder_Starts_In_Queue()
    {
        var order = new ExecutionOrder { OrderId = Guid.NewGuid() };

        Assert.Equal(ExecutionStatuses.Queued, order.Status);
        Assert.NotEmpty(order.Id);
        Assert.NotEqual(default, order.CreatedAt);
    }
}
