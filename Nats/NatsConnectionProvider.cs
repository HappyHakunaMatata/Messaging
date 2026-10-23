using NATS.Net;

namespace Messaging.Nats;

internal sealed class NatsConnectionProvider(IOptions<NatsOptions> options) : INatsConnectionProvider, IAsyncDisposable
{
    private readonly Lock _gate = new();
    private readonly NatsOptions _options = options.Value;

    private NatsClient? _client;
    private INatsJSContext? _context;

    public INatsJSContext Context
    {
        get
        {
            lock (_gate)
            {
                _client ??= new NatsClient(_options.Url);
                return _context ??= _client.CreateJetStreamContext();
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        NatsClient? client;

        lock (_gate)
        {
            client = _client;
            _client = null;
            _context = null;
        }

        if (client is not null)
        {
            await client.DisposeAsync();
        }
    }
}
