using System.Collections.Concurrent;
using Docker.DotNet.Models;
using PlatformApi.Models;

namespace PlatformApi.Services;

public class FailureSimulationService(ILogger<FailureSimulationService> logger, DockerService dockerService)
{
    public async Task<NetworkDelayResponse> ApplyNetworkDelay(NetworkDelayRequest request)
    {
        string failureId = $"pumba-delay-{Guid.NewGuid():N}";

        try
        {
            await dockerService.PullImageIfNotExistsAsync(PumbaImage);
            await dockerService.PullImageIfNotExistsAsync(TcImage);

            List<string> cmd =
            [
                "--log-level", "info",
                "netem",
                "--duration", $"{request.DurationSeconds}s",
                "--tc-image", TcImage,
                "delay",
                "--time", request.DelayMs.ToString()
            ];

            if (request.JitterMs > 0)
            {
                cmd.Add("--jitter");
                cmd.Add(request.JitterMs.ToString()!);
            }

            cmd.Add($"re2:^{request.ContainerName}$");

            CreateContainerResponse? createResponse = await dockerService.Client.Containers.CreateContainerAsync(new()
            {
                Image = PumbaImage,
                Name = failureId,
                Cmd = cmd,
                HostConfig = new()
                {
                    Binds = ["/var/run/docker.sock:/var/run/docker.sock:ro"],
                    AutoRemove = true,
                    Privileged = true
                },
                Labels = new Dictionary<string, string>
                {
                    ["platform"] = "distributed-system-platform",
                    ["type"] = "pumba-failure"
                }
            });

            await dockerService.Client.Containers.StartContainerAsync(createResponse.ID, new());

            FailureStatus failure = new(
                Id: failureId,
                Type: "network-delay",
                ContainerName: request.ContainerName,
                Status: "active",
                StartedAt: DateTime.UtcNow,
                ExpiresAt: DateTime.UtcNow.AddSeconds(request.DurationSeconds));
            _activeFailures[failureId] = failure;

            _ = MonitorFailureAsync(failureId, createResponse.ID);

            logger.LogInformation(
                "Applied network delay of {DelayMs}ms (jitter: {JitterMs}ms) to container {Container} for {Duration}s",
                request.DelayMs, request.JitterMs, request.ContainerName, request.DurationSeconds);

            return new(
                failureId,
                "active",
                $"Network delay of {request.DelayMs}ms applied to {request.ContainerName} for {request.DurationSeconds}s");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to apply network delay to container {Container}", request.ContainerName);
            return new(failureId, "failed", ex.Message);
        }
    }

    public async Task<NetworkDelayResponse> StopFailureAsync(string failureId)
    {
        try
        {
            ContainerListResponse? container = await dockerService.FindContainerByNameAsync(failureId);
            if (container == null)
            {
                _activeFailures.TryRemove(failureId, out _);
                return new(failureId, "not_found", "Failure simulation not found");
            }

            await dockerService.StopAndRemoveContainerAsync(container.ID, 2);

            if (_activeFailures.TryGetValue(failureId, out FailureStatus? failure))
            {
                _activeFailures[failureId] = failure with { Status = "stopped" };
            }

            logger.LogInformation("Stopped failure simulation {FailureId}", failureId);
            return new(failureId, "stopped", "Failure simulation stopped");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to stop failure simulation {FailureId}", failureId);
            return new(failureId, "error", ex.Message);
        }
    }

    public ActiveFailuresResponse GetActiveFailures()
    {
        CleanupExpiredFailures();
        return new(_activeFailures.Values.ToList());
    }

    private async Task MonitorFailureAsync(string failureId, string containerId)
    {
        try
        {
            ContainerWaitResponse waitResponse = await dockerService.WaitContainerAsync(containerId, removeAfter: false);

            if (_activeFailures.TryGetValue(failureId, out FailureStatus? failure))
            {
                string status = waitResponse.StatusCode == 0 ? "completed" : "failed";
                _activeFailures[failureId] = failure with { Status = status };
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error monitoring failure {FailureId}", failureId);
        }
    }

    private void CleanupExpiredFailures()
    {
        DateTime now = DateTime.UtcNow;
        List<string> expired = _activeFailures
            .Where(kvp => kvp.Value.ExpiresAt.HasValue && kvp.Value.ExpiresAt < now)
            .Select(kvp => kvp.Key)
            .ToList();

        foreach (string id in expired)
        {
            if (_activeFailures.TryGetValue(id, out FailureStatus? failure) && failure.Status == "active")
            {
                _activeFailures[id] = failure with { Status = "completed" };
            }
        }
    }

    private const string PumbaImage = "gaiaadm/pumba:latest";
    private const string TcImage = "ghcr.io/alexei-led/pumba-alpine-nettools:latest";
    private readonly ConcurrentDictionary<string, FailureStatus> _activeFailures = new();
}
