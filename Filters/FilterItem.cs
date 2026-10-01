namespace Messaging.Filters;

public sealed class FilterItem(FilterDescriptor descriptor)
{
    public FilterDescriptor Descriptor { get; } = descriptor;

    public INatsFilter? Filter { get; set; }

    public bool IsReusable { get; set; }
}
