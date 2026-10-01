using Messaging.Routing;

namespace Messaging.Infrastructure;

internal sealed class NatsActionInvokerFactory
{
    public NatsActionInvoker CreateInvoker(NatsContext context, NatsEndpoint endpoint)
        => new(context, endpoint);
}
