namespace Messaging.Filters;

public interface IOrderedFilter : INatsFilter
{
    int Order { get; }
}
