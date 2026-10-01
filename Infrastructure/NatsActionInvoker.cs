using System.Runtime.ExceptionServices;
using Messaging.Filters;
using Messaging.Routing;

namespace Messaging.Infrastructure;

internal sealed class NatsActionInvoker(NatsContext context, NatsEndpoint endpoint)
{
    public async Task InvokeAsync()
    {
        NatsFilterSet filters = endpoint.Filters.GetFilters(context.Services);

        try
        {
            object handler = endpoint.HandlerFactory(context.Services);
            IDictionary<string, object?> arguments = endpoint.Binder.Bind(context);

            NatsExecutionContext execution = new(context, handler, arguments, filters.All);

            await new FilterPipeline(execution, filters.ActionFilters, ExecuteHandlerAsync).RunAsync();
        }
        catch (Exception exception)
        {
            context.Exception = exception;

            if (await HandleAsync(filters, exception) is { } unhandled)
            {
                ExceptionDispatchInfo.Capture(unhandled).Throw();
            }
        }
    }

    private async Task ExecuteHandlerAsync(NatsExecutionContext execution)
        => execution.Result = await endpoint.Executor.ExecuteAsync(
            execution.Handler,
            endpoint.Binder.ToArguments(execution.Arguments));

    private async Task<Exception?> HandleAsync(NatsFilterSet filters, Exception exception)
    {
        if (filters.ExceptionFilters.Length == 0)
        {
            return exception;
        }

        NatsExceptionContext exceptionContext = new(context, filters.All, exception);

        INatsExceptionFilter[] exceptionFilters = filters.ExceptionFilters;

        for (int index = 0; index < exceptionFilters.Length; index++)
        {
            await exceptionFilters[index].OnExceptionAsync(exceptionContext);

            if (exceptionContext.Handled)
            {
                return null;
            }
        }

        return exceptionContext.Exception;
    }
}
