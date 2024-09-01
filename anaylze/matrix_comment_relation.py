import json
import pandas as pd
from collections import defaultdict, Counter
from sqlalchemy import create_engine, text
import re
import os
import numpy as np
import logging
import time
import matplotlib.pyplot as plt
import mysql.connector
import csv
# Load database credentials
with open('db_config.json') as f:
    db_config = json.load(f)

# Connect to the database
db_url = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
engine = create_engine(db_url)

# Regular expression to clean HTML tags, links, and non-alphabetical characters
clean_re = re.compile(r'<.*?>|https?://\S+|[^a-zA-Z\s]')

def clean_comment(text):
    return clean_re.sub(' ', text).lower()

# List of trivial words (stop words)
def load_stop_words(file_path):
    stop_words = set()
    try:
        with open(file_path, 'r') as file:
            for line in file:
                word = line.strip().lower()
                if word:
                    stop_words.add(word)
    except Exception as e:
        logging.error(f"Error loading stop words from file: {e}")
    return stop_words

# Path to the stop words file
stop_words_file_path = os.path.join(os.path.dirname(__file__), 'stop_words.txt')

# Load stop words from the file into a set
stop_words = load_stop_words(stop_words_file_path)

def filter_stop_words(words):
    return [word for word in words if word not in stop_words and len(word) <= 40]

# Create the data folder if it doesn't exist
data_folder = 'data'
if not os.path.exists(data_folder):
    os.makedirs(data_folder)

# Paths for saving files
matrix_save_path = os.path.join(data_folder, 'word_matrix.npy')
word_to_index_save_path = os.path.join(data_folder, 'word_to_index.json')
count_file = os.path.join(data_folder, 'count.txt')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_count(count_file):
    if os.path.exists(count_file):
        with open(count_file, 'r') as f:
            return int(f.read().strip())
    return 0

def save_count(count, count_file):
    with open(count_file, 'w') as f:
        f.write(str(count))

def load_last_seen_id(last_seen_id_file):
    try:
        with open(last_seen_id_file, 'r') as f:
            return json.load(f)
    except (EOFError, IOError, json.JSONDecodeError) as e:
        logging.error(f"Error loading last seen ID file: {e}. Starting with no last seen ID.")
        return None

def save_last_seen_id(last_seen_id, last_seen_id_file):
    try:
        with open(last_seen_id_file, 'w') as f:
            json.dump(last_seen_id, f)
    except Exception as e:
        logging.error(f"Error saving last seen ID file: {e}")

def save_plot(comments_processed, word_pairs, save_path='word_pairs_plot.png'):
    plt.figure(figsize=(10, 6))
    plt.plot(comments_processed, word_pairs, marker='o')
    plt.title('Number of Word Pairs vs. Number of Comments Processed')
    plt.xlabel('Number of Comments Processed')
    plt.ylabel('Number of Word Pairs')
    plt.grid(True)
    plt.savefig(save_path)
    plt.close()

def process_batches(batch_size=100000, save_interval=5):
    last_seen_id_file = 'last_seen_id.json'  # File to store the last seen ID
    last_seen_id = load_last_seen_id(last_seen_id_file)
    
    # Initialize variables
    word_matrix = defaultdict(Counter)
    total_processed = 0
    start_time = time.time()
    batch_counter = 0
    comments_processed = []
    word_pairs_list = []
    
    try:
        with engine.connect() as connection:
            last_seen_id = "0"

            while True:
                batch_start_time = time.time()
                
                # Query batch of comments using keyset pagination
                query = text("""
                    SELECT id, content
                    FROM comment
                    WHERE id > :last_seen_id AND content!='this post violated our policy'
                    ORDER BY id ASC
                    LIMIT :batch_size
                """)
                batch = connection.execute(query, {'last_seen_id': last_seen_id, 'batch_size': batch_size}).fetchall()

                if not batch:
                    break  # No more comments to process
                
                last_seen_id = batch[-1][0]  # Update last_seen_id for the next batch
                
                # Processing comments
                for row in batch:
                    comment = clean_comment(row[1])
                    words = comment.split()
                    filtered_words = filter_stop_words(words)
                    unique_words = set(filtered_words)
                    
                    for word1 in unique_words:
                        for word2 in unique_words:
                            word_matrix[word1][word2] += 1
                
                total_processed += len(batch)
                comments_processed.append(total_processed)
                word_pairs_list.append(sum(len(word_matrix[word1]) for word1 in word_matrix))
                
                batch_counter += 1
                #word_pairs = [(word1, word2, count) for word1 in word_matrix for word2, count in word_matrix[word1].items()]
                if batch_counter >= save_interval:
                    # Prepare data for CSV
                    insert_data = [{'word1': word1, 'word2': word2, 'count': count} for word1 in word_matrix for word2, count in word_matrix[word1].items()]
                    
                    # File path for the CSV
                    csv_file_path = '/var/lib/mysql-files/word_pairs.csv'
                    
                    # Save data to CSV
                    with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
                        writer = csv.writer(file)
                        writer.writerow(['word1', 'word2', 'count'])  # Write header
                        for row in insert_data:
                            writer.writerow([row['word1'], row['word2'], row['count']])
                    
                    # Load data from CSV into the database
                    # Define SQL queries
                    create_temp_table_query = """
                        CREATE TEMPORARY TABLE temp_word_pairs (
                            word1 VARCHAR(255),
                            word2 VARCHAR(255),
                            count INT
                        );
                    """

                    load_into_temp_query = f"""
                        LOAD DATA INFILE '{csv_file_path}'
                        INTO TABLE temp_word_pairs
                        FIELDS TERMINATED BY ','
                        LINES TERMINATED BY '\\n'
                        IGNORE 1 LINES
                        (word1, word2, count);
                    """

                    merge_data_query = """
                        INSERT INTO word_pairs (word1, word2, count)
                        SELECT word1, word2, SUM(count)
                        FROM temp_word_pairs
                        GROUP BY word1, word2
                        ON DUPLICATE KEY UPDATE
                            count = count + VALUES(count);
                    """

                    drop_temp_table_query = """
                        DROP TEMPORARY TABLE temp_word_pairs;
                    """

                    try:
                            connection.execute(text(create_temp_table_query))
                            connection.execute(text(load_into_temp_query))
                            connection.execute(text(merge_data_query))
                            connection.execute(text(drop_temp_table_query))
                            connection.commit()  
                            logging.info(f"Inserted {len(insert_data)} pairs")
                    except Exception as e:
                        print(f"An error occurred while loading data: {e}")
                        connection.rollback()  # Rollback on error
                    
                    save_last_seen_id(last_seen_id, last_seen_id_file)

                

                    # Reset for the next batch
                    word_matrix.clear()
                    batch_counter = 0

                    # Save plot
                    save_plot(comments_processed, word_pairs_list)
                    
                batch_end_time = time.time()
                logging.info(f"Processed {total_processed} comments so far.")
                logging.info(f"Batch processing time: {batch_end_time - batch_start_time:.2f} seconds.")

            # Final save if needed
            if word_matrix:
                save_plot(comments_processed, word_pairs_list)

    except KeyboardInterrupt:
        logging.warning("Process interrupted! Saving current state...")
        save_last_seen_id(last_seen_id, last_seen_id_file)
        save_plot(comments_processed, word_pairs_list)
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        end_time = time.time()
        logging.info(f"Total processing time: {end_time - start_time:.2f} seconds.")
process_batches(batch_size=1000, save_interval=4)