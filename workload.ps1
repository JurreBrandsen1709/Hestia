Start-Process powershell.exe -ArgumentList 'ksunami --brokers localhost:9092 --topic topic_normal --min 20 --max 50 --up spike-in --down spike-out --up-sec 5 --down-sec 5 --min-sec 180 --payload bytes:100' -NoNewWindow
Start-Process powershell.exe -ArgumentList 'ksunami --brokers localhost:9092 --topic topic_priority --min 1 --max 5 --up spike-in --down spike-out --up-sec 5 --down-sec 5 --min-sec 180 --payload bytes:1000' -NoNewWindow
Start-Sleep -Seconds 10
Stop-Process -Name powershell