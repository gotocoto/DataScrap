import json
import pandas as pd
from collections import defaultdict, Counter
from sqlalchemy import create_engine, text
import re
import os
import numpy as np
import logging
import time
import cProfile
import pstats
import io
from scipy.sparse import csr_matrix
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
stop_words = set([
    # (Stop words list)
])

def filter_stop_words(words):
    return [word for word in words if word not in stop_words and len(word) <= 40]

# Paths for saving files
data_folder = 'data'
if not os.path.exists(data_folder):
    os.makedirs(data_folder)

matrix_save_path = os.path.join(data_folder, 'word_matrix.npy')
word_to_index_save_path = os.path.join(data_folder, 'word_to_index.json')
count_file = os.path.join(data_folder, 'count.txt')
csv_file_path = "/var/lib/mysql-files/word_pairs.csv"
#os.path.join(data_folder, 'word_pairs.csv')

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

def save_data(word_matrix, word_to_index):
    # (Save sparse matrix and word_to_index)
    pass

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
#@profile
def process_batches(batch_size=100000, save_interval=5):
    last_seen_id_file = 'last_seen_id.json'
    last_seen_id = load_last_seen_id(last_seen_id_file)
    
    word_to_index = {}
    next_index = 0
    count = 0
    matrix_size = 1000000
    word_matrix = csr_matrix((matrix_size, matrix_size), dtype=np.int32)
    
    try:
        if not os.path.exists(matrix_save_path):
            raise ValueError("No file to run")
        word_matrix = load_npz(matrix_save_path).tocsr()
        with open(word_to_index_save_path, 'r') as f:
            word_to_index = json.load(f)
        next_index = max(word_to_index.values())
        count = load_count(count_file)
    except Exception as e:
        logging.error(f"Error loading matrix or word_to_index file: {e}. Starting with an empty matrix and index.")
        word_matrix = csr_matrix((matrix_size, matrix_size), dtype=np.int32)
        word_to_index = {}
        last_seen_id = None
        save_last_seen_id(last_seen_id, last_seen_id_file)
    
    total_processed = 0
    start_time = time.time()
    
    batch_counter = 0
    
    with engine.connect() as connection:
        start_time = time.time()
        last_seen_id = "0"

        while True:
            batch_start_time = time.time()
            
            query = text("""
                SELECT id, content
                FROM comment
                WHERE id > :last_seen_id
                ORDER BY id ASC
                LIMIT :batch_size
            """)
            batch = connection.execute(query, {'last_seen_id': last_seen_id, 'batch_size': batch_size}).fetchall()

            if not batch:
                break
            
            last_seen_id = batch[-1][0]

            word_matrix = defaultdict(Counter)
            for row in batch:
                comment = clean_comment(row[1])
                words = comment.split()
                filtered_words = filter_stop_words(words)
                unique_words = set(filtered_words)
                
                for word1 in unique_words:
                    for word2 in unique_words:
                        word_matrix[word1][word2] += 1

            word_pairs = [(word1, word2, count) for word1 in word_matrix for word2, count in word_matrix[word1].items() if count > 0]
            process_start_time = time.time()
            try:
                # Insert unique words into word_lookup table
                insert_word_query = text("""
                    INSERT IGNORE INTO word_lookup (word)
                    VALUES (:word);
                """)
                insert_word_data = [{'word': word} for word in word_matrix.keys()]
                connection.execute(insert_word_query, insert_word_data)
                
                # Export word pairs to CSV
                with open(csv_file_path, 'w', newline='') as csvfile:
                    csv_writer = csv.writer(csvfile)
                    for word1, word2, count in word_pairs:
                        csv_writer.writerow([word1, word2, count])
                
                # Use LOAD DATA INFILE to load the CSV into the temporary table
                connection.execute(text("""
                    CREATE TEMPORARY TABLE temp_word_pairs (
                        word1 VARCHAR(40),
                        word2 VARCHAR(40),
                        count INT,
                        INDEX idx_word1 (word1),  -- Index for the `word1` column to speed up JOIN with word_lookup
                        INDEX idx_word2 (word2)   -- Index for the `word2` column to speed up JOIN with word_lookup
                    );
                """))

                connection.execute(text(f"""
                    LOAD DATA INFILE '{csv_file_path}'
                    INTO TABLE temp_word_pairs
                    FIELDS TERMINATED BY ',' 
                    LINES TERMINATED BY '\n'
                    (word1, word2, count);
                """))

                # Upsert into the main table
                upsert_query = text("""
                    INSERT INTO word_matrix (id1, id2, count)
                    SELECT
                        w1.id AS id1,
                        w2.id AS id2,
                        t.count
                    FROM
                        temp_word_pairs t
                    JOIN
                        word_lookup w1 ON t.word1 = w1.word
                    JOIN
                        word_lookup w2 ON t.word2 = w2.word
                    ON DUPLICATE KEY UPDATE
                        word_matrix.count = word_matrix.count + VALUES(count);
                """)
                connection.execute(upsert_query)
                
                # Drop the temporary table
                connection.execute(text("DROP TEMPORARY TABLE temp_word_pairs;"))

                connection.commit()

            except Exception as e:
                logging.error(f"Error: {e}")
                connection.rollback()

            process_end_time = time.time()
            count += len(batch)
            batch_counter += 1
            
            if batch_counter >= save_interval:
                break
                save_data(word_matrix, word_to_index)
                save_count(count, count_file)
                save_last_seen_id(last_seen_id, last_seen_id_file)
                batch_counter = 0
                logging.info(f"Data saved after {save_interval} batches.")
            
            batch_end_time = time.time()
            logging.info(f"Processed {count} comments so far.")
            logging.info(f"Batch processing time: {batch_end_time - batch_start_time:.2f} seconds.")
            logging.info(f"Data processing time: {process_end_time - process_start_time:.2f} seconds.")
        
        save_data(word_matrix, word_to_index)
        save_count(count, count_file)
        save_last_seen_id(last_seen_id, last_seen_id_file)
        
        end_time = time.time()
        logging.info(f"Total processing time: {end_time - start_time:.2f} seconds.")
        logging.info("Processing completed.")

def profile_code():
    process_batches(batch_size=10070, save_interval=1)

profile_code()
# Set up profiling
pr = cProfile.Profile()
pr.enable()

pr.disable()

# Print profiling results
ps = io.StringIO()
ps = pstats.Stats(pr, stream=ps).sort_stats(pstats.SortKey.CUMULATIVE)
ps.print_stats()
print(ps.getvalue())
