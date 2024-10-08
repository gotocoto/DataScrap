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
from scipy.sparse import dok_matrix, save_npz, load_npz, csr_matrix, coo_matrix
from collections import defaultdict
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
    'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 'your',
    'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', 'her',
    'hers', 'herself', 'it', 'its', 'itself', 'they', 'them', 'their', 'theirs',
    'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', 'these', 'those',
    'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had',
    'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if',
    'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with',
    'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after',
    'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over',
    'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where',
    'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other',
    'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than',
    'too', 'very', 's', 't', 'can', 'will', 'just', 'don', 'should', 'now'
])

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

def save_data(word_matrix, word_to_index):
    # Save the sparse matrix
    try:
        temp_matrix_path = matrix_save_path + '.tmp'
        save_npz(temp_matrix_path, word_matrix.tocsr())
        if os.path.exists(matrix_save_path):
            os.remove(matrix_save_path)
        os.rename(temp_matrix_path+'.npz', matrix_save_path)
    except Exception as e:
        logging.error(f"Error saving matrix file: {e}")
    
    # Save the word_to_index
    try:
        temp_word_to_index_path = word_to_index_save_path + '.tmp'
        with open(temp_word_to_index_path, 'w') as f:
            json.dump(word_to_index, f)
        if os.path.exists(word_to_index_save_path):
            os.remove(word_to_index_save_path)
        os.rename(temp_word_to_index_path, word_to_index_save_path)
    except Exception as e:
        logging.error(f"Error saving word_to_index file: {e}")

index_cache = {}

def precompute_indices(max_size=40):
    """ Precompute index pairs for matrix sizes from 1 to max_size and cache them. """
    for size in range(1, max_size + 1):
        i_indices, j_indices = np.tril_indices(size,0)
        index_cache[size] = (i_indices, j_indices)

def get_cached_indices(num_words):
    """ Retrieve precomputed index pairs from the cache. """
    if num_words in index_cache:
        return index_cache[num_words]
    else:
        return np.tril_indices(num_words, 0)

# Precompute index pairs for sizes up to 40
precompute_indices(40)

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
    last_seen_id_file = 'last_seen_id.json'  # File to store the last seen ID
    last_seen_id = load_last_seen_id(last_seen_id_file)
    
    # Create the mappings
    word_to_index = {}
    next_index = 0
    count = 0
    # Initialize with a small matrix
    matrix_size = 1000000
    word_matrix = csr_matrix((matrix_size, matrix_size), dtype=np.int32)  # Initialize with CSR format
    
    # Load existing matrix and word_to_index if they exist
    try:
        if not os.path.exists(matrix_save_path) : raise ValueError("No file to run")
        word_matrix = load_npz(matrix_save_path).tocsr()  # Convert to CSR format for ease of use
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
        while True:
            batch_start_time = time.time()
            
            # Query batch of comments using keyset pagination
            if last_seen_id is None:
                query = text(f"SELECT id, content FROM comment ORDER BY id ASC LIMIT {batch_size}")
            else:
                query = text(f"SELECT id, content FROM comment WHERE id > :last_seen_id ORDER BY id ASC LIMIT {batch_size}")
            
            batch = connection.execute(query, {'last_seen_id': last_seen_id} if last_seen_id else {}).fetchall()
            
            if not batch:
                break  # No more comments to process
            last_seen_id = batch[-1][0]  # Update last_seen_id for the next batch
            word_matrix = defaultdict(Counter)
            # Processing comments
            process_start_time = time.time()
            for row in batch:
                comment = clean_comment(row[1])
                words = comment.split()
                filtered_words = filter_stop_words(words)
                
                # Map words to indices and handle new words
                unique_words = set(filtered_words)
                for i, word1 in enumerate(unique_words):
                    for j, word2 in enumerate(unique_words):
                        word_matrix[word1][word2] += 1
            
            word_pairs = []
            '''
            for word1 in word_matrix:
                for word2 in word_matrix[word1]:
                    count = word_matrix[word1][word2]
                    if count > 0:
                        word_pairs.append((word1, word2, count))'''
            
            word_pairs = [(word1,  word2,  count) for word1 in word_matrix for word2, count in word_matrix[word1].items() if count > 0]
            
            try:
                # Step 1: Insert unique words into word_lookup table
                
                # Insert words into word_lookup table
                insert_word_query = text("""
                    INSERT IGNORE INTO word_lookup (word)
                    VALUES (:word);
                """)
                #print(list(word_matrix.keys()))
                insert_word_data = [{'word': word} for word in list(word_matrix.keys())]
                connection.execute(insert_word_query, insert_word_data)
                
                # Create a temporary table
                connection.execute(text("""
                    CREATE TEMPORARY TABLE temp_word_pairs (
                        word1 VARCHAR(40),
                        word2 VARCHAR(40),
                        count INT
                    );
                """))
                
                # Insert data into the temporary table
                insert_query = text("""
                    INSERT INTO temp_word_pairs (word1, word2, count)
                    VALUES (:word1, :word2, :count)
                """)
                insert_data = [{'word1': word1, 'word2': word2, 'count': count} for word1, word2, count in word_pairs]
                connection.execute(insert_query, insert_data)
                
                # Insert or update in the main table
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

            except Exception as e:
                print(f"Error: {e}")





            process_end_time = time.time()
            count+=batch_size
            # Save data every `save_interval` batches
            batch_counter += 1
            if batch_counter >= save_interval:
                save_start_time = time.time()
                save_data(word_matrix, word_to_index)
                save_count(count,count_file)
                save_last_seen_id(last_seen_id, last_seen_id_file)
                save_end_time = time.time()
                batch_counter = 0
                logging.info(f"Data saved after {save_interval} batches.")
                logging.info(f"Data saving time: {save_end_time - save_start_time:.2f} seconds.")
            
            
            
            batch_end_time = time.time()
            logging.info(f"Processed {count} comments so far.")
            logging.info(f"Batch processing time: {batch_end_time - batch_start_time:.2f} seconds.")
            logging.info(f"Data processing time: {process_end_time - process_start_time:.2f} seconds.")
    
    # Final save if needed
    if batch_counter > 0:
        save_data(word_matrix, word_to_index)
        save_last_seen_id(last_seen_id, last_seen_id_file)
    
    end_time = time.time()
    logging.info(f"Total processing time: {end_time - start_time:.2f} seconds.")
    logging.info("Processing completed.")
    
    # Final save if needed
    if batch_counter > 0:
        save_data(word_matrix, word_to_index)
        save_count(count, count_file)
    
    end_time = time.time()
    logging.info(f"Total processing time: {end_time - start_time:.2f} seconds.")
    logging.info("Processing completed.")



def profile_code():
    # Your function call to process_batches
    process_batches(batch_size=100000, save_interval=30)
profile_code()
'''
# Set up profiling
pr = cProfile.Profile()
pr.enable()
profile_code()
pr.disable()

# Print profiling results
ps = io.StringIO()
ps = pstats.Stats(pr, stream=s).sort_stats(pstats.SortKey.CUMULATIVE)
ps.print_stats()
print(s.getvalue())'''
