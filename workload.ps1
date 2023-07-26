

# Define the number of iterations
$iterations = 3

# Define the duration for each 'run' and 'pause' in seconds
$run_duration = 3
$pause_duration = 1

for ($i=0; $i -lt $iterations; $i++) {
    Write-Output "Running task..."

    # Start first command and get its process ID
    $job1 = Start-Job -ScriptBlock {
        ksunami --brokers localhost:9092 --topic topic_normal --min 20 --max 50 --up spike-in --down spike-out --up-sec 1 --down-sec 1 --payload bytes:100
    }

    # Start second command and get its process ID
    $job2 = Start-Job -ScriptBlock {
        ksunami --brokers localhost:9092 --topic topic_priority --min 5 --max 30 --up spike-in --down spike-out --up-sec 1 --down-sec 1 --payload bytes:1000
    }

    # Sleep for 1 minute
    Start-Sleep -Seconds $run_duration
    # Stop the processes
    Stop-Job $job1
    Stop-Job $job2

    # Clean up the jobs
    Remove-Job $job1
    Remove-Job $job2

    Write-Output "Pausing task..."

    # Pause the script for a while
    Start-Sleep -Seconds $pause_duration
}
