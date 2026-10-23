namespace Messaging.Routing;

internal interface IActionDescriptorCollectionProvider
{
    ActionDescriptorCollection Descriptors { get; }

    EndpointRouter Router { get; }
}
