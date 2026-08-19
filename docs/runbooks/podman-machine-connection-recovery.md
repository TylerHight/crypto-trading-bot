# Podman machine connection recovery on Windows

## Purpose

Use this runbook when the Podman CLI cannot connect to the WSL-backed Podman machine even though `podman machine start` reports that it is already running.

Typical error:

```text
Cannot connect to Podman.
failed to connect: dial tcp 127.0.0.1:<port>: connectex:
No connection could be made because the target machine actively refused it.
```

This procedure preserves the Podman machine, containers, images, and named volumes. Do not initialize another machine and do not remove the existing machine as part of routine recovery.

## Important distinctions

- A Python virtual environment such as `(.venv)` has no effect on Podman connectivity.
- `podman machine init` creates a VM; it does not repair a stopped or stale existing VM.
- An `already running` message describes the recorded VM state, but the Windows-to-WSL socket proxy may still be starting or stale.
- The forwarded localhost port may change across starts. Use `podman system connection list` instead of assuming a fixed port.

## Diagnose

Inspect the VM and its configured connection:

```powershell
podman machine list
podman system connection list
wsl --list --verbose
```

If the machine says `Currently starting`, wait 15–30 seconds and retry:

```powershell
podman info
podman ps
```

Confirm that the VM itself is reachable and its user socket is active:

```powershell
podman machine ssh "echo machine-ok; systemctl --user is-active podman.socket"
```

Healthy evidence:

```text
machine-ok
active
```

If this succeeds, the VM is healthy and the Windows proxy may only need more time. Retry `podman info` before restarting anything.

## Non-destructive recovery

If the CLI remains unreachable or the machine remains stuck in `Currently starting`, force-stop only the Podman VM:

```powershell
podman machine stop --force
```

If that completes, start it again:

```powershell
podman machine start
```

Wait 15–30 seconds, then verify:

```powershell
podman machine list
podman info --format "host={{.Host.OS}} rootless={{.Host.Security.Rootless}} runtime={{.Host.OCIRuntime.Name}}"
podman ps
```

Expected machine state is `Currently running`; `podman info` should report a Linux host.

Bring the project back to its declared state:

```powershell
podman compose up -d
podman compose ps
```

Return to [Local market-data pipeline operations](local-market-data-pipeline.md#verify-data-flow) and verify the data flow, not only the container state.

## WSL recovery

If a forced Podman VM restart does not restore connectivity, stop the VM and shut down WSL:

```powershell
podman machine stop --force
wsl --shutdown
podman machine start
```

Wait 15–30 seconds and verify again:

```powershell
podman info
podman ps
podman compose up -d
podman compose ps
```

`wsl --shutdown` stops every WSL distribution, including unrelated development sessions and Docker Desktop's WSL distribution if present. Save work in those environments first.

## Actions to avoid

Do not use these as routine connection fixes:

```text
podman machine init
podman machine rm
podman system reset
podman compose down --volumes
```

The first is unnecessary when the VM exists. The remaining commands can remove machines or project data and require a separate, explicit decision.

## Escalation

Stop and investigate before destructive recovery if all of these fail:

1. Waiting for `Currently starting` to finish.
2. `podman machine stop --force` followed by `podman machine start`.
3. `wsl --shutdown` followed by `podman machine start`.

Capture:

```powershell
podman machine list
podman system connection list
wsl --list --verbose
Get-Process | Where-Object { $_.ProcessName -match 'podman|win-sshproxy|wsl' }
```

Do not remove or reinitialize `podman-machine-default` until the need to preserve its images, containers, and volumes has been assessed.

