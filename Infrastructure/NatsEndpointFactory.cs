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

        bool fromServices = isService?.IsService(descriptor.HandlerType) == true;

        return new NatsEndpoint
        {
            Descriptor = descriptor,
            HandlerFactory = CreateHandlerFactory(descriptor.HandlerType, fromServices),
            HandlerReleaser = CreateHandlerReleaser(fromServices),
            Binder = new ParameterBinder(
                descriptor.Parameters,
                descriptor.MessageType is null ? null : deserializerFactory.Create(descriptor.MessageType)),
            Executor = ActionMethodExecutor.Create(descriptor.HandlerType, descriptor.MethodInfo),
            Filters = new FilterCache(descriptor, filterFactory)
        };
    }

    private static Func<IServiceProvider, object> CreateHandlerFactory(Type handlerType, bool fromServices)
    {
        if (fromServices)
        {
            return services => services.GetRequiredService(handlerType);
        }

        ObjectFactory factory = ActivatorUtilities.CreateFactory(handlerType, Type.EmptyTypes);

        return services => factory(services, null);
    }

    private static Func<object, ValueTask> CreateHandlerReleaser(bool fromServices)
    {
        if (fromServices)
        {
            return static _ => ValueTask.CompletedTask;
        }

        return static handler =>
        {
            if (handler is IAsyncDisposable asyncDisposable)
            {
                return asyncDisposable.DisposeAsync();
            }

            (handler as IDisposable)?.Dispose();
            return ValueTask.CompletedTask;
        };
    }
}
