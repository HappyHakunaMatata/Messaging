namespace Messaging.Abstractions;

public sealed class RetryPolicy
{
    public required int MaxDeliver { get; init; }

    public required IReadOnlyList<int> BackoffSeconds { get; init; }

    internal static bool IsValidBackoff(IReadOnlyList<int> backoffSeconds) => backoffSeconds.All(seconds => seconds > 0);

    internal TimeSpan RedeliveryDelay(ulong numDelivered)
    {
        int index = (int)Math.Min(Math.Max(numDelivered, 1) - 1, (ulong)(BackoffSeconds.Count - 1));

        return TimeSpan.FromSeconds(BackoffSeconds[index]);
    }
}
