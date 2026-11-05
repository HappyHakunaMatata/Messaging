using System.ComponentModel.DataAnnotations;
using System.Text.Json;

namespace Messaging.Options;

public sealed class NatsOptions
{
    private const string Scheme = "nats://";

    public const string DefaultHost = "localhost";

    public const ushort DefaultPort = 4222;

    private string? _url;

    public static string SectionName => "Nats";

    [Required]
    public string Host { get; set; } = DefaultHost;

    [Range(1, ushort.MaxValue)]
    public ushort Port { get; set; } = DefaultPort;

    public string? Username { get; set; }

    public string? Password { get; set; }

    public string? Token { get; set; }

    public string? CredentialsFile { get; set; }

    public JsonSerializerOptions SerializerOptions { get; set; } = new(JsonSerializerDefaults.Web);

    public string Url
    {
        get => string.IsNullOrWhiteSpace(_url) ? string.Concat(Scheme, FormatHost(Host), ":", Port.ToString()) : _url;
        set => _url = value;
    }

    internal NatsAuthOpts AuthOpts => new()
    {
        Username = Username,
        Password = Password,
        Token = Token,
        CredsFile = CredentialsFile
    };

    private static string FormatHost(string host)
        => host.Contains(':', StringComparison.Ordinal) && !host.StartsWith('[') ? $"[{host}]" : host;
}
