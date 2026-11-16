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

    [Fact]
    public void Execution_Only_Advances_And_Final_States_Do_Not_Reopen()
    {
        Assert.True(ExecutionSaga.CanTransition(ExecutionStatuses.Queued, ExecutionStatuses.Diagnosing));
        Assert.True(ExecutionSaga.CanTransition(ExecutionStatuses.Diagnosing, ExecutionStatuses.Repairing));
        Assert.True(ExecutionSaga.CanTransition(ExecutionStatuses.Repairing, ExecutionStatuses.Finished));
        Assert.True(ExecutionSaga.CanTransition(ExecutionStatuses.Diagnosing, ExecutionStatuses.Failed));
        Assert.False(ExecutionSaga.CanTransition(ExecutionStatuses.Queued, ExecutionStatuses.Finished));
        Assert.False(ExecutionSaga.CanTransition(ExecutionStatuses.Finished, ExecutionStatuses.Repairing));
        Assert.False(ExecutionSaga.CanTransition(ExecutionStatuses.Failed, ExecutionStatuses.Finished));
    }
}
