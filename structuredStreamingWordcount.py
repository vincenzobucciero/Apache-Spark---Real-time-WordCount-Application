from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    explode, split, lower, regexp_replace, length,
    window, current_timestamp, col
)
import sys

class StructuredStreamingWordCount:
    def __init__(self, hostname, port):
        self.hostname = hostname
        self.port = port

        # Initialize Spark session
        self.spark = SparkSession \
            .builder \
            .master("yarn") \
            .appName("StructuredSocketWordCount") \
            .getOrCreate()

        # Set log level
        self.spark.sparkContext.setLogLevel("WARN")

        # Stop words list
        self.stop_words = set([
            "a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "has",
            "he", "in", "is", "it", "its", "of", "on", "that", "the", "to", "was",
            "were", "will", "with", "i", "you", "your", "we", "they", "this", "not",
            "but", "or", "if", "so", "what", "when", "which", "who", "how", "http",
            "https", "www", "com"
        ])

    def create_socket_stream(self):
        # Create DataFrame representing the stream of input lines from socket
        lines = self.spark \
            .readStream \
            .format("socket") \
            .option("host", self.hostname) \
            .option("port", self.port) \
            .load()

        # Add metadata columns
        lines_with_metadata = lines \
            .withColumn("original_text", col("value")) \
            .withColumn("timestamp", current_timestamp()) \
            .withColumn("text_length", length(col("value")))

        return lines_with_metadata

    def process_text(self, df):
        # Clean text and split into words
        words = df \
            .withColumn("cleaned_text", lower(regexp_replace(col("original_text"), "[^a-zA-Z0-9\\s]", " "))) \
            .select("original_text", "timestamp", "text_length", explode(split(col("cleaned_text"), " ")).alias("word"))

        # Filter stop words and short words
        filtered_words = words \
            .filter((length(col("word")) > 1) & (~col("word").isin(self.stop_words)))

        return filtered_words

    def aggregate_words(self, df):
        # Add watermark to handle late data
        df_with_watermark = df.withWatermark("timestamp", "30 seconds")

        # Count words within each window
        word_counts = df_with_watermark \
            .groupBy(
                window(col("timestamp"), "10 seconds", "5 seconds"),
                col("word")
            ) \
            .count()

        # Add window start and end timestamp as separate columns
        result = word_counts \
            .withColumn("window_start", col("window.start")) \
            .withColumn("window_end", col("window.end")) \
            .select("word", "count", "window_start", "window_end")

        return result

    def aggregate_messages(self, df):
        # Add watermark to handle late data
        df_with_watermark = df.withWatermark("timestamp", "30 seconds")

        # Count messages within each window
        message_counts = df_with_watermark \
            .groupBy(
                window(col("timestamp"), "10 seconds", "5 seconds")
            ) \
            .count()

        # Add window start and end timestamp as separate columns
        result = message_counts \
            .withColumn("window_start", col("window.start")) \
            .withColumn("window_end", col("window.end")) \
            .select("count", "window_start", "window_end")

        return result

    def output_to_console(self, df, query_name):
        # Start streaming query with the correct output mode
        query = df \
            .writeStream \
            .outputMode("update") \
            .format("console") \
            .option("truncate", "false") \
            .queryName(query_name) \
            .start()

        return query

    def run(self):
        # Create and process streaming data
        lines_df = self.create_socket_stream()
        words_df = self.process_text(lines_df)
        word_counts_df = self.aggregate_words(words_df)
        message_counts_df = self.aggregate_messages(lines_df)

        # Output results
        word_query = self.output_to_console(word_counts_df, "word_counts")
        message_query = self.output_to_console(message_counts_df, "message_counts")

        # Wait for queries to terminate
        word_query.awaitTermination()
        message_query.awaitTermination()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: structured_socket_wordcount.py <hostname> <port>")
        sys.exit(1)

    hostname = sys.argv[1]
    port = int(sys.argv[2])

    counter = StructuredStreamingWordCount(hostname, port)
    counter.run()

