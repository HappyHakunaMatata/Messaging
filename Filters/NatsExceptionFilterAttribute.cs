namespace Messaging.Filters;

[AttributeUsage(AttributeTargets.Class | AttributeTargets.Method, AllowMultiple = true, Inherited = true)]
public abstract class NatsExceptionFilterAttribute : Attribute, INatsExceptionFilter, IOrderedFilter
{
    public int Order { get; set; } = FilterOrders.Default;

    public abstract Task OnExceptionAsync(NatsExceptionContext context);
}
