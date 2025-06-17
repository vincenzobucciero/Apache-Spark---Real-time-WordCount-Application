"""
Adapter that simulates Kafka using a TCP socket to test
structured_kafka_wordcount.py without installing Kafka.

This version reads lines from one or more specified text files.
"""

import socket
import sys
import time
import threading
import random
import os

def load_messages_from_files(file_paths):
    """Load messages from a list of text files"""
    messages = []

    if not file_paths:
        print("No files provided. Using default message.")
        return ["No available messages."]

    for file_path in file_paths:
        if not os.path.exists(file_path):
            print(f"WARNING: File {file_path} does not exist. Skipping.")
            continue

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = [line.strip() for line in f if line.strip()]
                messages.extend(lines)
                print(f"Loaded {len(lines)} messages from {file_path}")
        except Exception as e:
            print(f"Error reading {file_path}: {e}")

    if not messages:
        print("No messages loaded. Using default message.")
        return ["No available messages."]

    return messages

def handle_client(client_socket, messages, repeat=True, delay_min=0.5, delay_max=2.0):
    """Handles the connection with a client (e.g., Spark)"""
    print("Client connected, sending messages...")

    try:
        index = 0
        total_messages = len(messages)

        while True:
            message = messages[index]
            print(f"Sending: {message[:50]}..." if len(message) > 50 else f"Sending: {message}")
            client_socket.send((message + "\n").encode('utf-8'))

            index = (index + 1) % total_messages

            if not repeat and index == 0:
                print("All messages sent. Closing connection.")
                break

            time.sleep(random.uniform(delay_min, delay_max))

    except BrokenPipeError:
        print("Client disconnected")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client_socket.close()

def start_server(host='localhost', port=9999, file_paths=None, repeat=True, delay_min=0.5, delay_max=2.0):
    """Starts a TCP server simulating Kafka"""
    messages = load_messages_from_files(file_paths)

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_address = (host, port)
    print(f"Starting Kafka-simulator server on {host}:{port}")
    server_socket.bind(server_address)
    server_socket.listen(5)

    try:
        while True:
            print("Waiting for connections from Spark...")
            client_socket, client_address = server_socket.accept()
            print(f"Connection accepted from {client_address}")

            client_thread = threading.Thread(
                target=handle_client,
                args=(client_socket, messages, repeat, delay_min, delay_max)
            )
            client_thread.daemon = True
            client_thread.start()

    except KeyboardInterrupt:
        print("\nServer interrupted")
    finally:
        server_socket.close()
        print("Server shut down")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='TCP server simulating Kafka')
    parser.add_argument('--host', default='localhost', help='Host to bind (default: localhost)')
    parser.add_argument('--port', type=int, default=9999, help='Port to bind (default: 9999)')
    parser.add_argument('--file', nargs='+', help='List of text files to use as data source (e.g., --file *.txt)')
    parser.add_argument('--repeat', action='store_true', default=True, help='Repeat messages when end of file is reached')
    parser.add_argument('--no-repeat', dest='repeat', action='store_false', help='Do not repeat messages at end of file')
    parser.add_argument('--delay-min', type=float, default=0.5, help='Minimum delay between messages (seconds)')
    parser.add_argument('--delay-max', type=float, default=2.0, help='Maximum delay between messages (seconds)')

    args = parser.parse_args()

    print("Configuration:")
    print(f"- Host: {args.host}")
    print(f"- Port: {args.port}")
    print(f"- Files: {args.file if args.file else 'default message'}")
    print(f"- Repeat messages: {args.repeat}")
    print(f"- Delay: {args.delay_min}–{args.delay_max} s")

    start_server(
        host=args.host,
        port=args.port,
        file_paths=args.file,
        repeat=args.repeat,
        delay_min=args.delay_min,
        delay_max=args.delay_max
    )

