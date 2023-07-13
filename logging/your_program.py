import time
from prometheus_client import start_http_server, Gauge

# Create a gauge metric to represent CPU utilization
cpu_utilization = Gauge('cpu_utilization', 'Current CPU Utilization')

# Simulate CPU utilization by incrementing the gauge value
def simulate_cpu_utilization():
    while True:
        # Your CPU utilization logic goes here
        # Here, we'll just increment the gauge value by 10 every second
        cpu_utilization.inc(10)
        time.sleep(1)

# Start the Prometheus HTTP server on port 8000
start_http_server(8000)

# Start simulating CPU utilization
simulate_cpu_utilization()
