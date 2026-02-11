using System.Collections.Concurrent;
using Docker.DotNet.Models;
using PlatformApi.Models;

namespace PlatformApi.Services;

public class TrafficGeneratorService(
    ILogger<TrafficGeneratorService> logger,
    DockerService dockerService,
    IConfiguration configuration)
{
    public async Task<TrafficGenerationResponse> StartTrafficGenerationAsync(TrafficGenerationRequest request)
    {
        string jobId = $"k6-job-{Guid.NewGuid():N}";

        try
        {
            await dockerService.PullImageIfNotExistsAsync(K6Image);

            int maxVUs = request.MaxVUs ?? 100;
            List<string> envVars =
            [
                $"TARGET_URL={request.TargetUrl}",
                $"RPS={request.Rps}",
                $"DURATION={request.DurationSeconds}s",
                $"VUS={Math.Min(request.Rps * 2, maxVUs)}",
                $"MAX_VUS={maxVUs}",
                $"K6_PROMETHEUS_RW_SERVER_URL={_prometheusUrl}"
            ];

            CreateContainerResponse? createResponse = await dockerService.Client.Containers.CreateContainerAsync(new()
            {
                Image = K6Image,
                Name = jobId,
                Env = envVars,
                Cmd = ["run", "-o", "experimental-prometheus-rw", "/scripts/load-test.js"],
                HostConfig = new()
                {
                    Binds = [$"{GetScriptsPath()}:/scripts:ro"],
                    NetworkMode = dockerService.NetworkName,
                    AutoRemove = false
                },
                Labels = new Dictionary<string, string>
                {
                    ["platform"] = "distributed-system-platform",
                    ["type"] = "k6-job"
                }
            });

            await dockerService.Client.Containers.StartContainerAsync(createResponse.ID, new());

            TrafficJobStatus job = new(
                JobId: jobId,
                Status: "running",
                StartedAt: DateTime.UtcNow,
                FinishedAt: null,
                Error: null);
            _jobs[jobId] = job;

            _ = MonitorJobAsync(jobId, createResponse.ID);

            logger.LogInformation(
                "Started traffic generation job {JobId} targeting {TargetUrl} at {Rps} RPS for {Duration}s",
                jobId, request.TargetUrl, request.Rps, request.DurationSeconds);

            return new(jobId, "started",
                $"Traffic generation started with {request.Rps} RPS for {request.DurationSeconds}s");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to start traffic generation job");
            return new(jobId, "failed", ex.Message);
        }
    }

    public async Task<TrafficGenerationResponse> StopTrafficGenerationAsync(string jobId)
    {
        try
        {
            ContainerListResponse? container = await dockerService.FindContainerByNameAsync(jobId);
            if (container == null)
            {
                return new(jobId, "not_found", "Job not found");
            }

            await dockerService.StopAndRemoveContainerAsync(container.ID, 5);

            if (_jobs.TryGetValue(jobId, out TrafficJobStatus? job))
            {
                _jobs[jobId] = job with { Status = "stopped", FinishedAt = DateTime.UtcNow };
            }

            logger.LogInformation("Stopped traffic generation job {JobId}", jobId);
            return new(jobId, "stopped", "Traffic generation stopped");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to stop traffic generation job {JobId}", jobId);
            return new(jobId, "error", ex.Message);
        }
    }

    public IEnumerable<TrafficJobStatus> GetAllJobs() => _jobs.Values.ToList();

    private async Task MonitorJobAsync(string jobId, string containerId)
    {
        try
        {
            ContainerWaitResponse waitResponse = await dockerService.WaitContainerAsync(containerId);

            if (_jobs.TryGetValue(jobId, out TrafficJobStatus? job))
            {
                string status = waitResponse.StatusCode == 0 ? "completed" : "failed";
                string? error = waitResponse.StatusCode != 0 ? $"Exit code: {waitResponse.StatusCode}" : null;
                _jobs[jobId] = job with { Status = status, FinishedAt = DateTime.UtcNow, Error = error };
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error monitoring job {JobId}", jobId);
        }
    }

    private string GetScriptsPath()
    {
        string? path = Environment.GetEnvironmentVariable("K6_SCRIPTS_PATH");
        return path ?? "/app/k6/scripts";
    }

    private const string K6Image = "grafana/k6:0.54.0";
    private readonly string _prometheusUrl = configuration["K6:PrometheusUrl"]!;
    private readonly ConcurrentDictionary<string, TrafficJobStatus> _jobs = new();
}