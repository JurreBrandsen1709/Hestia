run_duration=30

echo "Running task..."

# Start second command and get its process ID
ksunami --brokers broker:9092 --topic topic_priority --min 6 --max 7 --payload bytes:1000 &
job2=$!

# Start first command and get its process ID
ksunami --brokers broker:9092 --topic topic_normal --min 30 --max 31 --payload bytes:100 &
job1=$!

# Sleep for run_duration
sleep $run_duration

# Stop the processes
kill $job1
kill $job2
