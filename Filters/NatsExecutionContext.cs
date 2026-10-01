using Messaging.Routing;

namespace Messaging.Filters;

public sealed class NatsExecutionContext
{
    public NatsExecutionContext(
        NatsContext natsContext,
        object handler,
        IDictionary<string, object?> arguments,
        IReadOnlyList<INatsFilter> filters)
    {
        ArgumentNullException.ThrowIfNull(natsContext);
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentNullException.ThrowIfNull(arguments);
        ArgumentNullException.ThrowIfNull(filters);

        NatsContext = natsContext;
        Handler = handler;
        Arguments = arguments;
        Filters = filters;
    }

    public NatsContext NatsContext { get; }

    public NatsActionDescriptor ActionDescriptor => NatsContext.ActionDescriptor;

    public object Handler { get; }

    public IDictionary<string, object?> Arguments { get; }

    public IReadOnlyList<INatsFilter> Filters { get; }

    public object? Result
    {
        get => NatsContext.Result;
        set => NatsContext.Result = value;
    }

    public CancellationToken CancellationToken => NatsContext.CancellationToken;
}
