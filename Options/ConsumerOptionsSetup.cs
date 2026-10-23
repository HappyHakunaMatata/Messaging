using Microsoft.Extensions.Hosting;

namespace Messaging.Options;

internal sealed class ConsumerOptionsSetup(IHostEnvironment? environment = null) : IPostConfigureOptions<ConsumerOptions>
{
    public void PostConfigure(string? name, ConsumerOptions options)
    {
        string? application = environment?.ApplicationName;

        if (string.IsNullOrWhiteSpace(application))
        {
            application = Assembly.GetEntryAssembly()?.GetName().Name;
        }

        if (string.IsNullOrWhiteSpace(application))
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(options.StreamName))
        {
            options.StreamName = ConsumerOptions.DefaultStreamName(application);
        }

        if (string.IsNullOrWhiteSpace(options.DurableName))
        {
            options.DurableName = ConsumerOptions.DefaultDurableName(application);
        }
    }
}
