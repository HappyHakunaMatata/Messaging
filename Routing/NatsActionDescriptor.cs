using Messaging.Filters;

namespace Messaging.Routing;

public sealed class NatsActionDescriptor
{
    public required string Subject { get; init; }

    public string? ClassSubject { get; init; }

    public string? Action { get; init; }

    public required Type HandlerType { get; init; }

    public required MethodInfo MethodInfo { get; init; }

    public required string DisplayName { get; init; }

    public Type? MessageType { get; init; }

    public required IReadOnlyList<ParameterDescriptor> Parameters { get; init; }

    public required IReadOnlyList<FilterDescriptor> Filters { get; init; }

    public override string ToString() => DisplayName;
}
