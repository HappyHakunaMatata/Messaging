using Messaging.Nats;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Hosting;

namespace Messaging.DependencyInjection;

public static class NatsApplicationBuilderExtensions
{
    extension(IApplicationBuilder app)
    {
        public IApplicationBuilder MapNatsHandlers()
        {
            ArgumentNullException.ThrowIfNull(app);

            IServiceProvider services = app.ApplicationServices;
            services
                .GetRequiredService<SubscriptionManager>()
                .Start(services.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping);

            return app;
        }
    }
}
