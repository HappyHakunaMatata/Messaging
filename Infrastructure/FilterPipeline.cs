using Messaging.Filters;

namespace Messaging.Infrastructure;

internal sealed class FilterPipeline
{
    private readonly NatsExecutionContext _context;
    private readonly Func<NatsExecutionContext, Task> _handler;
    private readonly NatsExecutionDelegate _next;

    private FilterCursor _cursor;

    public FilterPipeline(NatsExecutionContext context, INatsActionFilter[] filters, Func<NatsExecutionContext, Task> handler)
    {
        _context = context;
        _handler = handler;
        _cursor = new FilterCursor(filters);
        _next = NextAsync;
    }

    public Task RunAsync() => NextAsync();

    private Task NextAsync()
    {
        INatsActionFilter? filter = _cursor.GetNextFilter();

        return filter is null ? _handler(_context) : filter.InvokeAsync(_context, _next);
    }
}
