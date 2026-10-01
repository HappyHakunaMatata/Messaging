namespace Messaging.Filters;

public interface IFilterFactory : INatsFilter
{
    bool IsReusable { get; }

    INatsFilter CreateInstance(IServiceProvider services);
}
