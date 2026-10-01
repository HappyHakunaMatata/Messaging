using Messaging.Routing;

namespace Messaging.Filters;

internal sealed class FilterCache(NatsActionDescriptor action, FilterFactory factory)
{
    private NatsFilterSet? _reusable;

    public NatsFilterSet GetFilters(IServiceProvider services)
    {
        if (_reusable is { } cached)
        {
            return cached;
        }

        FilterItem[] items = factory.CreateItems(action, services);
        NatsFilterSet set = new(items);

        if (Array.TrueForAll(items, static item => item.IsReusable))
        {
            _reusable = set;
        }

        return set;
    }
}
