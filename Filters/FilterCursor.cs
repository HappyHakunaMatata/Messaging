namespace Messaging.Filters;

internal struct FilterCursor
{
    private readonly INatsActionFilter[] _filters;
    private int _index;

    public FilterCursor(INatsActionFilter[] filters)
    {
        _filters = filters;
        _index = 0;
    }

    public INatsActionFilter? GetNextFilter() => _index < _filters.Length ? _filters[_index++] : null;
}
