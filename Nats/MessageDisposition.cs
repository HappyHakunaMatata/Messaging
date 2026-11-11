namespace Messaging.Nats;

internal enum MessageDispositionKind
{
    Acknowledge,
    Redeliver,
    Terminate,
}

internal readonly record struct MessageDisposition(MessageDispositionKind Kind, TimeSpan RedeliveryDelay = default)
{
    public static MessageDisposition Acknowledge => new(MessageDispositionKind.Acknowledge);

    public static MessageDisposition Terminate => new(MessageDispositionKind.Terminate);

    public static MessageDisposition Redeliver(TimeSpan delay) => new(MessageDispositionKind.Redeliver, delay);
}
