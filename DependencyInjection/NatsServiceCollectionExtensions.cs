using Messaging.Filters;
using Messaging.Infrastructure;
using Messaging.Nats;
using Messaging.Routing;
using Messaging.Security;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Messaging.DependencyInjection;

public static class NatsServiceCollectionExtensions
{
    extension(IServiceCollection services)
    {
        public IServiceCollection AddNatsMessaging()
        {
            ArgumentNullException.ThrowIfNull(services);

            _ = services.AddOptions<NatsOptions>()
                .BindConfiguration(NatsOptions.SectionName)
                .ValidateDataAnnotations()
                .ValidateOnStart();

            _ = services.AddOptions<ConsumerOptions>()
                .BindConfiguration(ConsumerOptions.SectionName)
                .ValidateDataAnnotations()
                .ValidateOnStart();

            services.TryAddSingleton<INatsConnectionProvider, NatsConnectionProvider>();
            services.TryAddSingleton<IPublisher, NatsManager>();

            return services;
        }

        public IServiceCollection AddNatsHandlers(Assembly assembly)
            => services.AddNatsHandlers(assembly, static _ => { });

        public IServiceCollection AddNatsHandlers(Assembly assembly, Action<NatsMessagingOptions> configure)
        {
            ArgumentNullException.ThrowIfNull(services);
            ArgumentNullException.ThrowIfNull(assembly);
            ArgumentNullException.ThrowIfNull(configure);

            GetOrAddCatalog(services).Add(assembly);

            _ = services.AddNatsMessaging();
            _ = services.Configure(configure);

            services.TryAddSingleton<PayloadDeserializerFactory>();
            services.TryAddSingleton<FilterFactory>();
            services.TryAddSingleton<NatsEndpointFactory>();
            services.TryAddSingleton<EndpointRouterFactory>();
            services.TryAddSingleton<NatsHeaderCollectionFactory>();
            services.TryAddSingleton<NatsActionInvokerFactory>();
            services.TryAddSingleton<NatsMessageDispatcher>();
            services.TryAddSingleton<NatsStreamProvisioner>();
            services.TryAddSingleton<IPrincipalFactory, HeaderPrincipalFactory>();
            services.TryAddSingleton<IActionDescriptorCollectionProvider, DefaultActionDescriptorCollectionProvider>();
            services.TryAddSingleton<SubscriptionManager>();

            services.TryAddEnumerable(ServiceDescriptor.Singleton<IActionDescriptorProvider, AssemblyActionDescriptorProvider>());
            services.TryAddEnumerable(ServiceDescriptor.Singleton<IFilterProvider, DefaultFilterProvider>());

            services.TryAddScoped<NatsContextAccessor>();
            services.TryAddScoped<INatsContextAccessor>(provider => provider.GetRequiredService<NatsContextAccessor>());

            return services;
        }
    }

    private static HandlerAssemblyCatalog GetOrAddCatalog(IServiceCollection services)
    {
        for (int index = 0; index < services.Count; index++)
        {
            ServiceDescriptor descriptor = services[index];

            if (descriptor.ServiceType == typeof(HandlerAssemblyCatalog) &&
                descriptor.ImplementationInstance is HandlerAssemblyCatalog registered)
            {
                return registered;
            }
        }

        HandlerAssemblyCatalog catalog = new();
        _ = services.AddSingleton(catalog);
        return catalog;
    }
}
