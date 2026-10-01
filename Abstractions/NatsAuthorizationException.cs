namespace Messaging.Abstractions;

public sealed class NatsAuthorizationException(string message) : NatsMessageRejectedException(message);
