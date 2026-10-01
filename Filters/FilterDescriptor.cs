namespace Messaging.Filters;

public sealed class FilterDescriptor
{
    public FilterDescriptor(INatsFilter filter, int scope)
    {
        ArgumentNullException.ThrowIfNull(filter);

        Filter = filter;
        Scope = scope;
        Order = (filter as IOrderedFilter)?.Order ?? FilterOrders.Default;
    }

    public INatsFilter Filter { get; }

    public int Order { get; }

    public int Scope { get; }
}
