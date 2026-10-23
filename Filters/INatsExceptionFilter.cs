namespace Messaging.Filters;

public interface INatsExceptionFilter : INatsFilter
{
    Task OnExceptionAsync(NatsExceptionContext context);
}
