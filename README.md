


# Apache Spark - Real Time WordCount Application with PySPark

Apache Spark Real-time WordCount Application with PySpark

A distributed streaming system using Spark Structured Streaming for real-time word counting and text analytics. Features a TCP socket simulator mimicking Kafka, windowed aggregations, stop word filtering, and YARN cluster deployment with NFS storage.

# 🚀 Features

**Real-time Stream Processing**: Processes text data in real-time using Spark Structured Streaming

**Socket-based Data Source**: Custom TCP socket server that simulates Kafka for testing purposes

**Advanced Text Processing**:
- Text cleaning and normalization
- Stop word filtering
- Windowed word counting
- Message frequency analysis

**Distributed Architecture**: Designed to run on YARN clusters with NFS shared storage
    
**Configurable Data Source**: Supports multiple text files as input with customizable streaming parameters

# 📋 Prerequisites

- Apache Spark 3.x with PySpark
- Python 3.7+
- YARN cluster environment
- NFS server for shared storage (optional but recommended for distributed deployment)
- SSH access to cluster nodes

# 🏗️ Architecture
The application implements a distributed real-time streaming architecture designed for processing text data at scale. The system consists of two main components running on separate nodes within a YARN cluster environment.

# 🔧 Components
1. **Socket Simulator** (socketExam.py)

A TCP server that simulates Kafka by streaming text data from files:

- Loads messages from multiple text files
- Configurable streaming rate and delays
- Supports message repetition
- Multi-client connection handling

2. **Streaming Application** (structuredStreamingWordcount.py)

The main Spark Structured Streaming application featuring:

- Real-time text processing with cleaning and normalization
- Stop word filtering (English)
- Windowed aggregations (10-second windows, 5-second slides)
- Dual analytics: word counts and message frequency
- Watermark handling for late data (30-second tolerance)

# 🚀 Quick Start

# NFS Server Setup

## Connect to NFS server
```
ssh -p 22 sshuser@provaexam-ssh.azurehdinsight.net
```

## Install NFS services
```
sudo apt update && sudo apt install nfs-kernel-server
sudo systemctl restart nfs-kernel-server
```

## Create shared directory
```
mkdir -p spark_streaming_app
```

## Configure NFS exports
```
sudo nano /etc/exports
```
## Add this row to exports file
```
/home/sshuser/spark_streaming_app <client_ip_address>(rw,sync,no_subtree_check)
```
## Apply exports
```
sudo exportfs -ra
```


# NFS Client Setup

## Connect to NFS client
```
ssh -p 23 sshuser@provaexam-ssh.azurehdinsight.net
```

## Install NFS utilities
```
sudo apt update && sudo apt install nfs-common
```

## Create mount point and mount shared directory
```
mkdir -p spark_streaming_app
sudo mount <server_ip_address>:/home/sshuser/spark_streaming_app /home/sshuser/spark_streaming_app
```

# Deploy Application

## From local machine, copy files to NFS server
```
scp -P 22 socketExam.py structuredStreamingWordcount.py *.txt sshuser@provaexam-ssh.azurehdinsight.net:~/spark_streaming_app
```

## On NFS server - start socket simulator
```
spark-submit --deploy-mode client socketExam.py --host hn0-provae --port 8080 --file *.txt
```

## On NFS client - start streaming application
```
spark-submit --deploy-mode client structuredStreamingWordcount.py hn0-provae 8080
```

# ⚙️ Configuration Options
Socket Simulator Options

```python socketExam.py [options]```

Options:

  ```--host HOST```           Host to bind (default: localhost)

  ```--port PORT```           Port to bind (default: 9999)

  ```--file FILES```          Text files to stream (supports wildcards)

  ```--repeat```              Repeat messages when EOF reached (default: True)

  ```--no-repeat```           Don't repeat messages

  ```--delay-min FLOAT```     Minimum delay between messages (default: 0.5s)

  ```--delay-max FLOAT```     Maximum delay between messages (default: 2.0s)



# 📊 Output Format
