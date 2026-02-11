using Docker.DotNet;
using Docker.DotNet.Models;

namespace PlatformApi.Services;

public class DockerService
{
    public DockerService(ILogger<DockerService> logger, IConfiguration configuration)
    {
        _logger = logger;
        NetworkName = configuration["Docker:NetworkName"]!;
        string dockerHost = configuration["Docker:Host"]!;
        Client = new DockerClientConfiguration(new Uri(dockerHost)).CreateClient();
    }

    public DockerClient Client { get; }

    public string NetworkName { get; }

    public async Task PullImageIfNotExistsAsync(string imageName)
    {
        try
        {
            await Client.Images.InspectImageAsync(imageName);
            _logger.LogDebug("Image {Image} already exists", imageName);
        }
        catch (DockerImageNotFoundException)
        {
            _logger.LogInformation("Pulling image {Image}...", imageName);
            await Client.Images.CreateImageAsync(
                new() { FromImage = imageName },
                null,
                new Progress<JSONMessage>(m =>
                {
                    if (!string.IsNullOrEmpty(m.Status))
                    {
                        _logger.LogDebug("Pull: {Status}", m.Status);
                    }
                }));
            _logger.LogInformation("Image {Image} pulled successfully", imageName);
        }
    }

    public async Task<ContainerListResponse?> FindContainerByNameAsync(string containerName)
    {
        IList<ContainerListResponse> containers = await Client.Containers.ListContainersAsync(new()
        {
            All = true,
            Filters = new Dictionary<string, IDictionary<string, bool>>
            {
                ["name"] = new Dictionary<string, bool> { [containerName] = true }
            }
        });

        return containers.FirstOrDefault();
    }

    public async Task StopAndRemoveContainerAsync(string containerId, uint waitBeforeKillSeconds = 5)
    {
        try
        {
            await Client.Containers.StopContainerAsync(containerId, new()
            {
                WaitBeforeKillSeconds = waitBeforeKillSeconds
            });

            await Client.Containers.RemoveContainerAsync(containerId, new() { Force = true });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to stop/remove container {ContainerId}", containerId);
        }
    }

    public async Task<ContainerWaitResponse> WaitContainerAsync(string containerId, bool removeAfter = true)
    {
        ContainerWaitResponse waitResponse = await Client.Containers.WaitContainerAsync(containerId);

        if (!removeAfter)
        {
            return waitResponse;
        }

        try
        {
            await Client.Containers.RemoveContainerAsync(containerId, new());
        }
        catch
        {
            // Container might already be removed (AutoRemove=true)
        }

        return waitResponse;
    }

    private readonly ILogger<DockerService> _logger;
}