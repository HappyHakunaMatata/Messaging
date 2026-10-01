using Messaging.Filters;
using Messaging.Infrastructure;

namespace Messaging.Routing;

internal sealed class NatsEndpoint
{
    public required NatsActionDescriptor Descriptor { get; init; }

    public required Func<IServiceProvider, object> HandlerFactory { get; init; }

    public required ParameterBinder Binder { get; init; }

    public required ActionMethodExecutor Executor { get; init; }

    public required FilterCache Filters { get; init; }

    public string DisplayName => Descriptor.DisplayName;
}
