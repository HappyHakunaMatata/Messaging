namespace Messaging.Routing;

internal sealed class HandlerAssemblyCatalog
{
    private readonly HashSet<Assembly> _assemblies = [];

    public void Add(Assembly assembly)
    {
        lock (_assemblies)
        {
            _ = _assemblies.Add(assembly);
        }
    }

    public IReadOnlyList<Assembly> Assemblies
    {
        get
        {
            lock (_assemblies)
            {
                return [.. _assemblies];
            }
        }
    }
}
