using Messaging.Filters;
using Messaging.Routing;

namespace Messaging.Infrastructure;

internal sealed class NatsEndpointFactory(
    PayloadDeserializerFactory deserializerFactory,
    FilterFactory filterFactory,
    IServiceProviderIsService? isService = null)
{
    public NatsEndpoint Create(NatsActionDescriptor descriptor)
    {
        ArgumentNullException.ThrowIfNull(descriptor);

        return new NatsEndpoint
        {
            Descriptor = descriptor,
            HandlerFactory = CreateHandlerFactory(descriptor.HandlerType),
            Binder = new ParameterBinder(
                descriptor.Parameters,
                descriptor.MessageType is null ? null : deserializerFactory.Create(descriptor.MessageType)),
            Executor = ActionMethodExecutor.Create(descriptor.HandlerType, descriptor.MethodInfo),
            Filters = new FilterCache(descriptor, filterFactory)
        };
    }

    private Func<IServiceProvider, object> CreateHandlerFactory(Type handlerType)
    {
        if (isService?.IsService(handlerType) == true)
        {
            return services => services.GetRequiredService(handlerType);
        }

        ObjectFactory factory = ActivatorUtilities.CreateFactory(handlerType, Type.EmptyTypes);

        return services => factory(services, null);
    }
}
