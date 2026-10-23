using Messaging.Routing;

namespace Messaging.Abstractions;

public abstract class NatsContext
{
    public abstract string Subject { get; }

    public abstract IReadOnlyDictionary<string, string> Headers { get; }

    public abstract ReadOnlyMemory<byte> Payload { get; }

    public abstract ClaimsPrincipal User { get; }

    public abstract IServiceProvider Services { get; }

    public abstract NatsActionDescriptor ActionDescriptor { get; }

    public abstract IDictionary<object, object?> Items { get; }

    public abstract object? Result { get; set; }

    public abstract Exception? Exception { get; set; }

    public abstract CancellationToken CancellationToken { get; }
}
