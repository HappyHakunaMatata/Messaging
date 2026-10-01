namespace Messaging.Filters;

[AttributeUsage(AttributeTargets.Class | AttributeTargets.Method, AllowMultiple = true, Inherited = true)]
public sealed class NatsServiceFilterAttribute(Type filterType) : Attribute, IFilterFactory, IOrderedFilter
{
    public Type FilterType { get; } = filterType;

    public int Order { get; set; } = FilterOrders.Default;

    public bool IsReusable { get; set; }

    public INatsFilter CreateInstance(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);

        return services.GetRequiredService(FilterType) as INatsFilter
            ?? throw new InvalidOperationException(
                $"'{FilterType}' is registered but does not implement '{nameof(INatsFilter)}'.");
    }
}
