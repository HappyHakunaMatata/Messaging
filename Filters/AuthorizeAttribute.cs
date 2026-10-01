namespace Messaging.Filters;

[AttributeUsage(AttributeTargets.Class | AttributeTargets.Method, AllowMultiple = false, Inherited = true)]
public sealed class AuthorizeAttribute : Attribute, INatsActionFilter, IOrderedFilter
{
    public int Order { get; set; } = FilterOrders.Authorization;

    public string Roles { get; set; } = string.Empty;

    public Task InvokeAsync(NatsExecutionContext context, NatsExecutionDelegate next)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(next);

        ClaimsPrincipal user = context.NatsContext.User;

        if (user.Identity?.IsAuthenticated != true)
        {
            throw new NatsAuthorizationException(
                $"Message on subject '{context.NatsContext.Subject}' carries no identity, but " +
                $"'{context.ActionDescriptor.DisplayName}' requires one.");
        }

        if (Roles.Length > 0 && !IsInAnyRole(user))
        {
            throw new NatsAuthorizationException(
                $"Message on subject '{context.NatsContext.Subject}' carries none of the roles '{Roles}' that " +
                $"'{context.ActionDescriptor.DisplayName}' requires.");
        }

        return next();
    }

    private bool IsInAnyRole(ClaimsPrincipal user)
    {
        string[] roles = Roles.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        for (int index = 0; index < roles.Length; index++)
        {
            if (user.IsInRole(roles[index]))
            {
                return true;
            }
        }

        return false;
    }
}
