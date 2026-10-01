namespace Messaging.Filters;

[AttributeUsage(AttributeTargets.Class | AttributeTargets.Method, AllowMultiple = true, Inherited = true)]
public abstract class NatsActionFilterAttribute : Attribute, INatsActionFilter, IOrderedFilter
{
    public int Order { get; set; } = FilterOrders.Default;

    public abstract Task InvokeAsync(NatsExecutionContext context, NatsExecutionDelegate next);
}
