using Messaging.Routing;

namespace Messaging.Infrastructure;

internal static class NatsActionInvokerFactory
{
    public static NatsActionInvoker CreateInvoker(NatsContext context, NatsEndpoint endpoint)
        => new(context, endpoint);
}
