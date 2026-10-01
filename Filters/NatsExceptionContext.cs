using Messaging.Routing;

namespace Messaging.Filters;

public sealed class NatsExceptionContext
{
    private Exception _exception;

    public NatsExceptionContext(NatsContext natsContext, IReadOnlyList<INatsFilter> filters, Exception exception)
    {
        ArgumentNullException.ThrowIfNull(natsContext);
        ArgumentNullException.ThrowIfNull(filters);
        ArgumentNullException.ThrowIfNull(exception);

        NatsContext = natsContext;
        Filters = filters;
        _exception = exception;
    }

    public NatsContext NatsContext { get; }

    public NatsActionDescriptor ActionDescriptor => NatsContext.ActionDescriptor;

    public IReadOnlyList<INatsFilter> Filters { get; }

    public Exception Exception
    {
        get => _exception;
        set
        {
            ArgumentNullException.ThrowIfNull(value);
            _exception = value;
            NatsContext.Exception = value;
        }
    }

    public bool Handled { get; set; }

    public object? Result
    {
        get => NatsContext.Result;
        set => NatsContext.Result = value;
    }
}
