using System.Linq.Expressions;

namespace Messaging.Infrastructure;

internal sealed class ActionMethodExecutor
{
    private static readonly Task<object?> _noResult = Task.FromResult<object?>(null);

    private readonly Func<object, object?[], Task<object?>> _execute;

    private ActionMethodExecutor(Func<object, object?[], Task<object?>> execute) => _execute = execute;

    public Task<object?> ExecuteAsync(object handler, object?[] arguments) => _execute(handler, arguments);

    public static ActionMethodExecutor Create(Type handlerType, MethodInfo method)
    {
        ParameterExpression target = Expression.Parameter(typeof(object), "target");
        ParameterExpression arguments = Expression.Parameter(typeof(object?[]), "arguments");

        ParameterInfo[] parameters = method.GetParameters();
        Expression[] call = new Expression[parameters.Length];

        for (int index = 0; index < parameters.Length; index++)
        {
            call[index] = Expression.Convert(
                Expression.ArrayIndex(arguments, Expression.Constant(index)),
                parameters[index].ParameterType);
        }

        Expression body = Adapt(
            Expression.Call(Expression.Convert(target, handlerType), method, call),
            method);

        return new ActionMethodExecutor(
            Expression.Lambda<Func<object, object?[], Task<object?>>>(body, target, arguments).Compile());
    }

    private static Expression Adapt(MethodCallExpression call, MethodInfo method)
    {
        Type returnType = method.ReturnType;

        if (returnType == typeof(void))
        {
            return Expression.Block(call, Expression.Constant(_noResult, typeof(Task<object?>)));
        }

        if (returnType == typeof(Task))
        {
            return Expression.Call(Method(nameof(FromTask)), call);
        }

        if (returnType == typeof(ValueTask))
        {
            return Expression.Call(Method(nameof(FromValueTask)), call);
        }

        if (GenericArgumentOf(returnType, typeof(ValueTask<>)) is { } valueTaskResult)
        {
            return Expression.Call(Method(nameof(FromValueTaskOf)).MakeGenericMethod(valueTaskResult), call);
        }

        if (GenericArgumentOf(returnType, typeof(Task<>)) is { } taskResult)
        {
            return Expression.Call(Method(nameof(FromTaskOf)).MakeGenericMethod(taskResult), call);
        }

        if (typeof(Task).IsAssignableFrom(returnType))
        {
            return Expression.Call(Method(nameof(FromTask)), Expression.Convert(call, typeof(Task)));
        }

        return Expression.Call(Method(nameof(FromResult)), Expression.Convert(call, typeof(object)));
    }

    private static Type? GenericArgumentOf(Type returnType, Type definition)
    {
        for (Type? current = returnType; current is not null; current = current.BaseType)
        {
            if (current.IsGenericType && current.GetGenericTypeDefinition() == definition)
            {
                return current.GetGenericArguments()[0];
            }
        }

        return null;
    }

    private static MethodInfo Method(string name)
        => typeof(ActionMethodExecutor).GetMethod(name, BindingFlags.NonPublic | BindingFlags.Static)
           ?? throw new InvalidOperationException($"'{name}' was not found on '{nameof(ActionMethodExecutor)}'.");

    private static async Task<object?> FromTask(Task task)
    {
        await task;
        return null;
    }

    private static async Task<object?> FromTaskOf<TResult>(Task<TResult> task) => await task;

    private static async Task<object?> FromValueTask(ValueTask task)
    {
        await task;
        return null;
    }

    private static async Task<object?> FromValueTaskOf<TResult>(ValueTask<TResult> task) => await task;

    private static Task<object?> FromResult(object? result) => Task.FromResult(result);
}
