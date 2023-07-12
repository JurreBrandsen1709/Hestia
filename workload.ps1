$command1 = "ksunami --brokers localhost:9092 --topic topic_normal --min 20 --max 50 --up spike-in --down spike-out --up-sec 5 --down-sec 5 --min-sec 180 --payload bytes:100"
$command2 = "ksunami --brokers localhost:9092 --topic topic_priority1234 --min 1 --max 5 --up spike-in --down spike-out --up-sec 5 --down-sec 5 --min-sec 180 --payload bytes:1000"

$job1 = Start-Job -ScriptBlock { Start-Process -NoNewWindow -PassThru -FilePath "cmd.exe" -ArgumentList "/C $command1" }
$job2 = Start-Job -ScriptBlock { Start-Process -NoNewWindow -PassThru -FilePath "cmd.exe" -ArgumentList "/C $command2" }

Start-Sleep -Seconds 10

if ($job1.State -eq "Running") {
    Stop-Job -Job $job1
}
if ($job2.State -eq "Running") {
    Stop-Job -Job $job2
}
