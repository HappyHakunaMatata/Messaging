namespace Messaging.Routing;

public sealed class ActionDescriptorCollection(IReadOnlyList<NatsActionDescriptor> items, int version)
{
    public IReadOnlyList<NatsActionDescriptor> Items { get; } = items;

    public int Version { get; } = version;
}
