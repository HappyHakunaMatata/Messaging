namespace Messaging.Filters;

internal sealed class DefaultFilterProvider : IFilterProvider
{
    private const int ProviderOrder = -1000;

    public int Order => ProviderOrder;

    public void OnProvidersExecuting(FilterProviderContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        IReadOnlyList<FilterItem> results = context.Results;

        for (int index = 0; index < results.Count; index++)
        {
            Provide(context, results[index]);
        }
    }

    public void OnProvidersExecuted(FilterProviderContext context)
    {
    }

    private static void Provide(FilterProviderContext context, FilterItem item)
    {
        if (item.Filter is not null)
        {
            return;
        }

        if (item.Descriptor.Filter is not IFilterFactory factory)
        {
            item.Filter = item.Descriptor.Filter;
            item.IsReusable = true;
            return;
        }

        item.Filter = factory.CreateInstance(context.Services)
            ?? throw new InvalidOperationException(
                $"'{factory.GetType()}.{nameof(IFilterFactory.CreateInstance)}' returned null.");

        item.IsReusable = factory.IsReusable;
    }
}
