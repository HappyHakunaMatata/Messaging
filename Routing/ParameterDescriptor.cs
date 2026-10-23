namespace Messaging.Routing;

public sealed class ParameterDescriptor
{
    public required string Name { get; init; }

    public required Type ParameterType { get; init; }

    public required NatsBindingSource BindingSource { get; init; }
}
