using Messaging.Routing;

namespace Messaging.Infrastructure;

internal sealed class DefaultNatsContext(
    string subject,
    IReadOnlyDictionary<string, string> headers,
    ReadOnlyMemory<byte> payload,
    IServiceProvider services,
    NatsActionDescriptor actionDescriptor,
    IPrincipalFactory principalFactory,
    CancellationToken cancellationToken) : NatsContext
{
    private ClaimsPrincipal? _user;
    private IDictionary<object, object?>? _items;

    public override string Subject { get; } = subject;

    public override IReadOnlyDictionary<string, string> Headers { get; } = headers;

    public override ReadOnlyMemory<byte> Payload { get; } = payload;

    public override IServiceProvider Services { get; } = services;

    public override NatsActionDescriptor ActionDescriptor { get; } = actionDescriptor;

    public override CancellationToken CancellationToken { get; } = cancellationToken;

    public override ClaimsPrincipal User => _user ??= principalFactory.Create(Headers);

    public override IDictionary<object, object?> Items => _items ??= new Dictionary<object, object?>();

    public override object? Result { get; set; }

    public override Exception? Exception { get; set; }
}
