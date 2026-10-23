namespace Messaging.Abstractions;

public interface IPublisher
{
    Task PublishAsync<TPayload>(
        string subject,
        TPayload payload,
        IReadOnlyDictionary<string, string>? headers = null,
        CancellationToken cancellationToken = default);
}
