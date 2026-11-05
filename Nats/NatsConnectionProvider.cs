using NATS.Net;

namespace Messaging.Nats;

internal sealed class NatsConnectionProvider(IOptions<NatsOptions> options) : INatsConnectionProvider, IDisposable, IAsyncDisposable
{
    private readonly Lock _gate = new();
    private readonly NatsOptions _options = options.Value;

    private NatsClient? _client;
    private INatsJSContext? _context;
    private bool _disposed;

    public INatsJSContext Context
    {
        get
        {
            lock (_gate)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);

                _client ??= new NatsClient(new NatsOpts
                {
                    Url = _options.Url,
                    AuthOpts = _options.AuthOpts,
                    DrainSubscriptionsOnDispose = true
                });

                return _context ??= _client.CreateJetStreamContext();
            }
        }
    }

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    public async ValueTask DisposeAsync()
    {
        NatsClient? client;

        lock (_gate)
        {
            _disposed = true;
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
