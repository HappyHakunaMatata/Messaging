namespace Messaging.Filters;

internal struct FilterCursor(INatsActionFilter[] filters)
{
    private readonly INatsActionFilter[] _filters = filters;
    private int _index = 0;

    public INatsActionFilter? GetNextFilter() => _index < _filters.Length ? _filters[_index++] : null;
}
