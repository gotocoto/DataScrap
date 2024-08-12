import json
import pandas as pd
from collections import defaultdict, Counter
from sqlalchemy import create_engine, text
import re
import os
import numpy as np
import logging
import time
from tempfile import NamedTemporaryFile

# Load database credentials
with open('db_config.json') as f:
    db_config = json.load(f)

# Connect to the database
db_url = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"
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
    return [word for word in words if word not in stop_words]

# Create the data folder if it doesn't exist
data_folder = 'data'
if not os.path.exists(data_folder):
    os.makedirs(data_folder)

# Paths for saving files
matrix_save_path = os.path.join(data_folder, 'word_matrix.npy')
word_count_save_path = os.path.join(data_folder, 'word_count.json')
offset_file = os.path.join(data_folder, 'offset.txt')

# Load offset from file
def load_offset(offset_file):
    if os.path.exists(offset_file):
        with open(offset_file, 'r') as f:
            return int(f.read().strip())
    return 0

# Save offset to file
def save_offset(offset, offset_file):
    with open(offset_file, 'w') as f:
        f.write(str(offset))

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to save data with atomic operations
import os
import tempfile

def save_data(word_matrix, word_count):
    # Create unique temporary files
    matrix_temp_path = tempfile.mktemp(prefix='word_matrix_', suffix='.npy')
    word_count_temp_path = tempfile.mktemp(prefix='word_count_', suffix='.json')

    try:
        # Save matrix to temporary file
        np.save(matrix_temp_path, word_matrix)
        
        # Remove the existing matrix file if it exists
        if os.path.exists(matrix_save_path):
            os.remove(matrix_save_path)
        
        # Rename temporary file to target file
        os.rename(matrix_temp_path, matrix_save_path)
        
    except Exception as e:
        logging.error(f"Error saving matrix file: {e}")
    
    try:
        # Save word count to temporary file
        with open(word_count_temp_path, 'w') as f:
            json.dump(word_count, f)
        
        # Remove the existing word count file if it exists
        if os.path.exists(word_count_save_path):
            os.remove(word_count_save_path)
        
        # Rename temporary file to target file
        os.rename(word_count_temp_path, word_count_save_path)
        
    except Exception as e:
        logging.error(f"Error saving word count file: {e}")


# Function to process comments in batches
def process_batches(batch_size=100000, save_interval=5):
    offset = load_offset(offset_file)
    word_matrix = defaultdict(Counter)
    word_count = Counter()
    
    # Load existing matrix and word count if they exist
    try:
        if os.path.exists(matrix_save_path):
            word_matrix = np.load(matrix_save_path, allow_pickle=True).item()
    except (EOFError, IOError) as e:
        logging.error(f"Error loading matrix file: {e}. Starting with an empty matrix.")
        word_matrix = defaultdict(Counter)
    
    try:
        if os.path.exists(word_count_save_path):
            with open(word_count_save_path, 'r') as f:
                word_count = Counter(json.load(f))
    except (EOFError, IOError, json.JSONDecodeError) as e:
        logging.error(f"Error loading word count file: {e}. Starting with an empty word count.")
        word_count = Counter()
    
    total_processed = offset
    start_time = time.time()
    
    batch_counter = 0
    
    with engine.connect() as connection:
        while True:
            batch_start_time = time.time()
            
            # Query batch of comments
            query = text(f"SELECT content FROM comment LIMIT {batch_size} OFFSET {offset}")
            batch = connection.execute(query).fetchall()
            
            if not batch:
                break  # No more comments to process
            
            # Processing comments
            process_start_time = time.time()
            for row in batch:
                comment = clean_comment(row[0])  # Access 'content' using index 0
                words = comment.split()
                filtered_words = filter_stop_words(words)
                unique_words = set(filtered_words)
                
                # Update word count
                word_count.update(unique_words)
                
                # Update word co-occurrence matrix
                for i, word1 in enumerate(unique_words):
                    for j, word2 in enumerate(unique_words):
                        if word1 != word2:
                            word_matrix[word1][word2] += 1
            process_end_time = time.time()
            
            # Save data every `save_interval` batches
            batch_counter += 1
            if batch_counter >= save_interval:
                save_start_time = time.time()
                save_data(word_matrix, word_count)
                save_end_time = time.time()
                batch_counter = 0
                
                logging.info(f"Data saved after {save_interval} batches.")
                logging.info(f"Data saving time: {save_end_time - save_start_time:.2f} seconds.")
            
            offset += batch_size
            total_processed += len(batch)
            save_offset(offset, offset_file)
            
            batch_end_time = time.time()
            logging.info(f"Processed {total_processed} comments so far.")
            logging.info(f"Batch processing time: {batch_end_time - batch_start_time:.2f} seconds.")
            logging.info(f"Data processing time: {process_end_time - process_start_time:.2f} seconds.")
    
    # Final save if needed
    if batch_counter > 0:
        save_data(word_matrix, word_count)
    
    end_time = time.time()
    logging.info(f"Total processing time: {end_time - start_time:.2f} seconds.")
    logging.info("Processing completed.")

# Run the processing function
process_batches(batch_size=100000, save_interval=6)
