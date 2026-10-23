using Messaging.Routing;

namespace Messaging.Infrastructure;

internal sealed class DefaultNatsContext(
    string subject,
    IReadOnlyDictionary<string, string> headers,
    ReadOnlyMemory<byte> payload,
    IServiceProvider services,
    NatsActionDescriptor actionDescriptor,
    CancellationToken cancellationToken) : NatsContext
{
    private IDictionary<object, object?>? _items;

    public override string Subject { get; } = subject;

    public override IReadOnlyDictionary<string, string> Headers { get; } = headers;

    public override ReadOnlyMemory<byte> Payload { get; } = payload;

    public override IServiceProvider Services { get; } = services;

    public override NatsActionDescriptor ActionDescriptor { get; } = actionDescriptor;

    public override CancellationToken CancellationToken { get; } = cancellationToken;

    public override IDictionary<object, object?> Items => _items ??= new Dictionary<object, object?>();

    public override object? Result { get; set; }

    public override Exception? Exception { get; set; }
}
