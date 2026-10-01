namespace Messaging.Routing;

internal sealed class DefaultActionDescriptorCollectionProvider : IActionDescriptorCollectionProvider
{
    private const int InitialVersion = 1;

    private readonly Lazy<NatsRoutingSnapshot> _snapshot;

    public DefaultActionDescriptorCollectionProvider(
        IEnumerable<IActionDescriptorProvider> providers,
        EndpointRouterFactory routerFactory)
    {
        IActionDescriptorProvider[] sources = [.. providers];
        _snapshot = new Lazy<NatsRoutingSnapshot>(() => Build(sources, routerFactory));
    }

    public ActionDescriptorCollection Descriptors => _snapshot.Value.Descriptors;

    public EndpointRouter Router => _snapshot.Value.Router;

    private static NatsRoutingSnapshot Build(
        IReadOnlyList<IActionDescriptorProvider> sources,
        EndpointRouterFactory routerFactory)
    {
        List<NatsActionDescriptor> descriptors = [];

        for (int index = 0; index < sources.Count; index++)
        {
            descriptors.AddRange(sources[index].GetDescriptors());
        }

        return new NatsRoutingSnapshot(
            new ActionDescriptorCollection(descriptors.AsReadOnly(), InitialVersion),
            routerFactory.Create(descriptors));
    }
}
