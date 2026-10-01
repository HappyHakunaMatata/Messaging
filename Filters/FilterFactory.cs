using Messaging.Routing;

namespace Messaging.Filters;

internal sealed class FilterFactory(IEnumerable<IFilterProvider> providers, IOptions<NatsMessagingOptions> options)
{
    private readonly IFilterProvider[] _providers = [.. providers.OrderBy(provider => provider.Order)];
    private readonly INatsFilter[] _globalFilters = [.. options.Value.Filters];

    public FilterDescriptor[] CreateDescriptors(Type handlerType, MethodInfo method)
    {
        List<FilterDescriptor> descriptors = new(_globalFilters.Length + 4);

        for (int index = 0; index < _globalFilters.Length; index++)
        {
            descriptors.Add(new FilterDescriptor(_globalFilters[index], FilterScope.Global));
        }

        Collect(handlerType.GetCustomAttributes(inherit: true), FilterScope.Handler, descriptors);
        Collect(method.GetCustomAttributes(inherit: true), FilterScope.Action, descriptors);

        return [.. descriptors.OrderBy(descriptor => descriptor.Order).ThenBy(descriptor => descriptor.Scope)];
    }

    public FilterItem[] CreateItems(NatsActionDescriptor action, IServiceProvider services)
    {
        FilterItem[] items = new FilterItem[action.Filters.Count];

        for (int index = 0; index < items.Length; index++)
        {
            items[index] = new FilterItem(action.Filters[index]);
        }

        FilterProviderContext context = new(action, items, services);

        for (int index = 0; index < _providers.Length; index++)
        {
            _providers[index].OnProvidersExecuting(context);
        }

        for (int index = _providers.Length - 1; index >= 0; index--)
        {
            _providers[index].OnProvidersExecuted(context);
        }

        return items;
    }

    private static void Collect(object[] attributes, int scope, List<FilterDescriptor> descriptors)
    {
        for (int index = 0; index < attributes.Length; index++)
        {
            if (attributes[index] is INatsFilter filter)
            {
                descriptors.Add(new FilterDescriptor(filter, scope));
            }
        }
    }
}
