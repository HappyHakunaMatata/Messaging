namespace Messaging.Nats;

public interface INatsConnectionProvider
{
    INatsJSContext Context { get; }
}
