$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$captureScript = Join-Path $PSScriptRoot 'run_daily_breakout_capture.ps1'
$taskName = 'CryptoResearch-BtcDailyBreakoutV1'
$taskIdentity = [System.Security.Principal.WindowsIdentity]::GetCurrent()
$expectedEnd = [DateTimeOffset]::Parse('2027-03-25T12:00:00Z')
if ([DateTimeOffset]::UtcNow -ge $expectedEnd) {
    throw 'The registered observation period has ended; do not reinstall its expired task.'
}
$arguments = '-NoProfile -NonInteractive -ExecutionPolicy Bypass -WindowStyle Hidden -File "{0}" -Action collect' -f $captureScript
$action = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument $arguments -WorkingDirectory $projectRoot
$existingTask = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if ($existingTask) {
    if ($existingTask.Actions.Count -ne 1 -or
        $existingTask.Actions[0].Execute -ine 'powershell.exe' -or
        $existingTask.Actions[0].Arguments -ne $arguments -or
        $existingTask.Actions[0].WorkingDirectory -ne $projectRoot -or
        $existingTask.Triggers.Count -ne 1 -or
        -not $existingTask.Settings.Enabled -or
        -not $existingTask.Triggers[0].Enabled -or
        $existingTask.Triggers[0].DaysInterval -ne 1 -or
        $existingTask.Principal.LogonType -ne 'Interactive' -or
        $existingTask.Principal.RunLevel -ne 'Limited' -or
        $existingTask.Settings.MultipleInstances -ne 'IgnoreNew' -or
        $existingTask.Settings.ExecutionTimeLimit -ne 'PT10M' -or
        -not $existingTask.Settings.StartWhenAvailable -or
        -not $existingTask.Settings.RunOnlyIfNetworkAvailable) {
        throw "An unrelated or changed task already uses $taskName; it has not been overwritten."
    }
    $existingUser = [System.Security.Principal.NTAccount]::new($existingTask.Principal.UserId)
    $existingUserSid = $existingUser.Translate([System.Security.Principal.SecurityIdentifier]).Value
    $existingStart = [DateTimeOffset]::Parse($existingTask.Triggers[0].StartBoundary)
    $existingEnd = [DateTimeOffset]::Parse($existingTask.Triggers[0].EndBoundary)
    if ($existingUserSid -ne $taskIdentity.User.Value -or
        $existingEnd -ne $expectedEnd -or $existingStart -ge $existingEnd -or
        $existingStart.UtcDateTime.TimeOfDay -ne [TimeSpan]::FromMinutes(135)) {
        throw "The existing $taskName has a different owner or schedule; it has not been overwritten."
    }
    $existingTask | Format-List TaskName, State
    Get-ScheduledTaskInfo -TaskName $taskName | Format-List LastRunTime, LastTaskResult, NextRunTime
    exit 0
}

# Run once daily at the local equivalent of tomorrow 02:15 UTC. Later daylight
# saving changes may shift UTC by an hour; every run remains after settlement.
$firstRun = [DateTime]::UtcNow.Date.AddDays(1).AddHours(2).AddMinutes(15).ToLocalTime()
$trigger = New-ScheduledTaskTrigger -Daily -At $firstRun
$trigger.EndBoundary = $expectedEnd.ToString('o')
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -RunOnlyIfNetworkAvailable `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 10)
$principal = New-ScheduledTaskPrincipal -UserId $taskIdentity.Name -LogonType Interactive -RunLevel Limited
Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger `
    -Settings $settings -Principal $principal `
    -Description 'Capture settled public BTC daily candles for the sealed breakout research study; no orders or account credentials.' |
    Format-List TaskName, State
Get-ScheduledTaskInfo -TaskName $taskName | Format-List LastRunTime, LastTaskResult, NextRunTime
