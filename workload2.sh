# Define the number of iterations
iterations=10

# Define the duration for each 'run' and 'pause' in seconds
run_duration=5
pause_duration=1

for (( i=0; i<$iterations; i++ )); do
    echo "Running task..."

    # Start second command and get its process ID
    ksunami --brokers broker:9092 --topic topic_priority --min 5 --max 30 --up spike-in --down spike-out --up-sec 3 --down-sec 3 --payload bytes:1000 &
    job2=$!

    # Start first command and get its process ID
    ksunami --brokers broker:9092 --topic topic_normal --min 20 --max 50 --up spike-in --down spike-out --up-sec 1 --down-sec 1 --payload bytes:100 &
    job1=$!

    # Sleep for run_duration
    sleep $run_duration

    # Stop the processes
    kill $job1

    sleep $run_duration
    kill $job2

    echo "Pausing task..."

    # Pause the script for pause_duration
    sleep $pause_duration
done
