namespace Messaging.Abstractions;

public interface INatsContextAccessor
{
    NatsContext? Context { get; }
}
