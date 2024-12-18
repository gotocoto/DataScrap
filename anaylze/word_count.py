import json
import re
import os
import logging
import time
from collections import Counter
from sqlalchemy import create_engine, text
import mysql.connector
import csv
start_time = time.time()
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
allowed_words_file_path = os.path.join(os.path.dirname(__file__), 'allowed_words.txt')
# Load stop words from the file into a set
stop_words = load_stop_words(stop_words_file_path)
allowed_words = load_stop_words(allowed_words_file_path)
def filter_stop_words(words):
    return [word for word in words if word not in stop_words and len(word) <= 40]

# Create the data folder if it doesn't exist
data_folder = 'data'
if not os.path.exists(data_folder):
    os.makedirs(data_folder)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

def process_batches(batch_size=100000, save_interval=5):
    last_seen_id_file = 'last_seen_id.json'
    last_seen_id = load_last_seen_id(last_seen_id_file)
    
    # Initialize variables
    word_counter = Counter()
    total_processed = 0
    batch_counter = 0
    csv_file_path = '/var/lib/mysql-files/word_pairs.csv'
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
                    word_counter.update(filtered_words)
                
                total_processed += len(batch)
                batch_counter += 1
                
                if batch_counter >= save_interval:
                    # Save word counts to the database
                    insert_data = [{'word': word, 'count': count} for word, count in word_counter.items()]
                    
                    # Define SQL queries
                    create_temp_table_query = """
                        CREATE TEMPORARY TABLE temp_word_count (
                            word VARCHAR(40),
                            count INT
                        );
                    """
                    
                    load_into_temp_query = f"""
                        LOAD DATA INFILE '{csv_file_path}'
                        INTO TABLE temp_word_count
                        FIELDS TERMINATED BY ','
                        LINES TERMINATED BY '\\n'
                        IGNORE 1 LINES
                        (word, count);
                    """

                    merge_data_query = """
                        INSERT INTO word_count (word, count)
                        SELECT word, SUM(count)
                        FROM temp_word_count
                        GROUP BY word
                        ON DUPLICATE KEY UPDATE
                            count = count + VALUES(count);
                    """

                    drop_temp_table_query = """
                        DROP TEMPORARY TABLE temp_word_count;
                    """

                    try:
                        # Save data to a CSV file
                        
                        with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
                            writer = csv.writer(file)
                            writer.writerow(['word', 'count'])  # Write header
                            for row in insert_data:
                                writer.writerow([row['word'], row['count']])
                        
                        connection.execute(text(create_temp_table_query))
                        connection.execute(text(load_into_temp_query))
                        connection.execute(text(merge_data_query))
                        connection.execute(text(drop_temp_table_query))
                        connection.commit()
                        logging.info(f"Inserted {len(insert_data)} word counts")
                    except Exception as e:
                        logging.error(f"An error occurred while loading data: {e}")
                        connection.rollback()
                    
                    save_last_seen_id(last_seen_id, last_seen_id_file)

                    # Reset for the next batch
                    word_counter.clear()
                    batch_counter = 0

                batch_end_time = time.time()
                logging.info(f"Processed {total_processed} comments so far.")
                logging.info(f"Batch processing time: {batch_end_time - batch_start_time:.2f} seconds.")

            # Final save if needed
            if word_counter:
                # Save remaining word counts to the database
                insert_data = [{'word': word, 'count': count} for word, count in word_counter.items()]
                
                csv_file_path = '/var/lib/mysql-files/word_pairs.csv'
                with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
                    writer = csv.writer(file)
                    writer.writerow(['word', 'count'])
                    for row in insert_data:
                        writer.writerow([row['word'], row['count']])
                
                try:
                    connection.execute(text(create_temp_table_query))
                    connection.execute(text(load_into_temp_query))
                    connection.execute(text(merge_data_query))
                    connection.execute(text(drop_temp_table_query))
                    connection.commit()
                    logging.info(f"Inserted {len(insert_data)} word counts")
                except Exception as e:
                    logging.error(f"An error occurred while loading data: {e}")
                    connection.rollback()
                
                save_last_seen_id(last_seen_id, last_seen_id_file)

    except KeyboardInterrupt:
        logging.warning("Process interrupted! Saving current state...")
        #save_last_seen_id(last_seen_id, last_seen_id_file)
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        
        end_time = time.time()
        logging.info(f"Total processing time: {end_time - start_time:.2f} seconds.")

process_batches(batch_size=100000, save_interval=20)
