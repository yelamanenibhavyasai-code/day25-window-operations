# Day 25: Window Operations in Spark Streaming

## Project Overview

This project demonstrates window-based stream processing using Scala and Apache Spark Streaming (DStreams).

The application calculates rolling sales totals, counts transactions within a 10-minute window, and generates an alert when transaction volume reaches a configured threshold.

## Technologies Used

- Scala 2.12.18
- Apache Spark 3.5.3
- Spark Streaming
- SBT 1.12.15
- Java 17
- Ubuntu WSL2

## Window Configuration

| Setting | Value |
|---|---|
| Batch interval | 10 seconds |
| Window size | 10 minutes |
| Sliding interval | 30 seconds |

## Features

### 1. countByWindow

Counts transactions received within the latest 10-minute window.

### 2. reduceByKeyAndWindow

Calculates rolling sales totals for each product over the latest 10 minutes.

### 3. Transaction Spike Alert

Generates a demo alert when the transaction count reaches 5 or more in the current window.

## Sample Input

```text
Laptop,50000
Phone,20000
Laptop,45000
Phone,15000
Tablet,10000
```

## Sample Results

```text
Laptop -> 95000.00
Phone -> 35000.00
Tablet -> 10000.00

SPIKE ALERT: 5 transactions in the last 10 minutes!
```

## How to Run

### 1. Start the socket server

```bash
nc -lk 9999
```

### 2. Run the Spark application in another terminal

```bash
cd ~/day25-window-operations
sbt "runMain Day25WindowOperations"
```

### 3. Send sales records

Enter records in the socket terminal using the format `product,amount`.

## Key Learnings

- Batch interval, window size, and sliding interval
- `countByWindow`
- `reduceByKeyAndWindow`
- Rolling sales aggregation
- Basic streaming transaction spike detection

## Author

Yelamaneni Bhavyasai
