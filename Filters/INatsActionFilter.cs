namespace Messaging.Filters;

public interface INatsActionFilter : INatsFilter
{
    Task InvokeAsync(NatsExecutionContext context, NatsExecutionDelegate next);
}
