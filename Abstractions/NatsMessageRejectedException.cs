namespace Messaging.Abstractions;

public abstract class NatsMessageRejectedException : Exception
{
    protected NatsMessageRejectedException(string message) : base(message)
    {
    }

    protected NatsMessageRejectedException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
