using Messaging.Filters;

namespace Messaging.Options;

public sealed class NatsMessagingOptions
{
    public IList<INatsFilter> Filters { get; } = [];
}
