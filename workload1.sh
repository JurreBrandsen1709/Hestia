run_duration=60

echo "Running task..."

# Start second command and get its process ID
ksunami --brokers broker:9092 --topic topic_priority --min 15 --max 16 --payload alpha:1 &
job2=$!

# Start first command and get its process ID
ksunami --brokers broker:9092 --topic topic_normal --min 30 --max 31 --payload alpha:2 &
job1=$!

# Sleep for run_duration
sleep $run_duration

# Stop the processes
kill $job1
kill $job2
