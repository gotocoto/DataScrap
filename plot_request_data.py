import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime

# Function to parse each line of the log file
def parse_log_line(line):
    parts = line.split("|")
    timestamp_str = parts[0].strip()
    concurrent_requests_str = parts[-2].strip().split()[0]
    requests_processed = int(parts[1].split()[4])
    duration_seconds = float(parts[3].strip())
    
    timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
    concurrent_requests = int(concurrent_requests_str)
    
    return timestamp, concurrent_requests, requests_processed

# Read data from the log file
log_file = "request_log.log"
timestamps = []
concurrent_requests = []
requests = []

with open(log_file, 'r') as file:
    for line in file:
        timestamp, conc_req, req_count = parse_log_line(line)
        timestamps.append(timestamp)
        concurrent_requests.append(conc_req)
        requests.append(req_count)

# Calculate requests per minute
timestamps_series = pd.Series(timestamps)
timestamps_minutes = (timestamps_series - timestamps_series.min()).dt.total_seconds() / 60
requests_per_minute = pd.Series(requests) / timestamps_minutes

# Plotting
plt.figure(figsize=(10, 6))
plt.plot(concurrent_requests, requests_per_minute, marker='o', linestyle='-', color='b')
plt.title('Rate of Requests per Minute vs Concurrent Requests')
plt.xlabel('Concurrent Requests')
plt.ylabel('Requests per Minute')
plt.grid(True)
plt.xticks(concurrent_requests)
plt.tight_layout()
plt.show()
