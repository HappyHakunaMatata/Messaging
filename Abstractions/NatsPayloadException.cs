namespace Messaging.Abstractions;

public sealed class NatsPayloadException(string message, Exception innerException) : NatsMessageRejectedException(message, innerException);
