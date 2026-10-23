namespace Messaging.Infrastructure;

internal sealed class NatsContextAccessor : INatsContextAccessor
{
    public NatsContext? Context { get; set; }
}
