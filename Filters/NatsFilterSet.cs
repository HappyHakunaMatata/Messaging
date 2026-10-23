namespace Messaging.Filters;

internal sealed class NatsFilterSet
{
    private static readonly INatsActionFilter[] _noActionFilters = [];
    private static readonly INatsExceptionFilter[] _noExceptionFilters = [];

    public NatsFilterSet(FilterItem[] items)
    {
        INatsFilter[] all = new INatsFilter[items.Length];

        for (int index = 0; index < items.Length; index++)
        {
            all[index] = items[index].Filter
                ?? throw new InvalidOperationException(
                    $"No {nameof(IFilterProvider)} produced an instance for '{items[index].Descriptor.Filter.GetType()}'.");
        }

        All = all;
        ActionFilters = all.Length == 0 ? _noActionFilters : [.. all.OfType<INatsActionFilter>()];

        if (all.Length == 0)
        {
            ExceptionFilters = _noExceptionFilters;
            return;
        }

        INatsExceptionFilter[] exceptionFilters = [.. all.OfType<INatsExceptionFilter>()];
        Array.Reverse(exceptionFilters);
        ExceptionFilters = exceptionFilters;
    }

    public IReadOnlyList<INatsFilter> All { get; }

    public INatsActionFilter[] ActionFilters { get; }

    public INatsExceptionFilter[] ExceptionFilters { get; }
}
