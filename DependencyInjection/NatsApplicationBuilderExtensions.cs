using Microsoft.AspNetCore.Builder;

namespace Messaging.DependencyInjection;

public static class NatsApplicationBuilderExtensions
{
    extension(IApplicationBuilder app)
    {
        [Obsolete("Handlers are consumed by a hosted service that AddNatsHandlers registers. This call does nothing and can be removed.")]
        public IApplicationBuilder MapNatsHandlers()
        {
            ArgumentNullException.ThrowIfNull(app);

            return app;
        }
    }
}
