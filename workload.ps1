# Start first command and get its process ID
$job1 = Start-Job -ScriptBlock {
    ksunami --brokers localhost:9092 --topic topic_normal --min 20 --max 50 --up spike-in --down spike-out --up-sec 5 --down-sec 5 --min-sec 180 --payload bytes:100
}

# Start second command and get its process ID
$job2 = Start-Job -ScriptBlock {
    ksunami --brokers localhost:9092 --topic topic_priority --min 5 --max 30 --up spike-in --down spike-out --up-sec 5 --down-sec 5 --min-sec 180 --payload bytes:1000
}

# Sleep for 1 minute
Start-Sleep -Seconds 20

# Stop the processes
Stop-Job $job1
Stop-Job $job2

# Clean up the jobs
Remove-Job $job1
Remove-Job $job2
