using Messaging.Routing;

namespace Messaging.Filters;

public sealed class FilterProviderContext(
    NatsActionDescriptor actionDescriptor,
    IReadOnlyList<FilterItem> results,
    IServiceProvider services)
{
    public NatsActionDescriptor ActionDescriptor { get; } = actionDescriptor;

    public IReadOnlyList<FilterItem> Results { get; } = results;

    public IServiceProvider Services { get; } = services;
}
