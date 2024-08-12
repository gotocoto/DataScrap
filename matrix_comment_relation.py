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
    return [word for word in words if word not in stop_words]

# Create the data folder if it doesn't exist
data_folder = 'data'
if not os.path.exists(data_folder):
    os.makedirs(data_folder)

# Paths for saving files
matrix_save_path = os.path.join(data_folder, 'word_matrix.npy')
word_count_save_path = os.path.join(data_folder, 'word_count.json')
offset_file = os.path.join(data_folder, 'offset.txt')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_offset(offset_file):
    if os.path.exists(offset_file):
        with open(offset_file, 'r') as f:
            return int(f.read().strip())
    return 0

def save_offset(offset, offset_file):
    with open(offset_file, 'w') as f:
        f.write(str(offset))

def save_data(word_matrix, word_count):
    # Save the sparse matrix
    try:
        temp_matrix_path = matrix_save_path + '.tmp'
        save_npz(temp_matrix_path, word_matrix.tocsr())
        if os.path.exists(matrix_save_path):
            os.remove(matrix_save_path)
        os.rename(temp_matrix_path+'.npz', matrix_save_path)
    except Exception as e:
        logging.error(f"Error saving matrix file: {e}")
    
    # Save the word count
    try:
        temp_word_count_path = word_count_save_path + '.tmp'
        with open(temp_word_count_path, 'w') as f:
            json.dump(word_count, f)
        if os.path.exists(word_count_save_path):
            os.remove(word_count_save_path)
        os.rename(temp_word_count_path, word_count_save_path)
    except Exception as e:
        logging.error(f"Error saving word count file: {e}")
index_cache = {}

def precompute_indices(max_size=40):
    """ Precompute index pairs for matrix sizes from 1 to max_size and cache them. """
    for size in range(1, max_size + 1):
        i_indices, j_indices = np.tril_indices(size, -1)
        index_cache[size] = (i_indices, j_indices)

def get_cached_indices(num_words):
    """ Retrieve precomputed index pairs from the cache. """
    if num_words in index_cache:
        return index_cache[num_words]
    else:
        return np.tril_indices(num_words, -1)

# Precompute index pairs for sizes up to 40
precompute_indices(40)
#@profile
def process_batches(batch_size=100000, save_interval=5):
    offset = load_offset(offset_file)
    
    # Create the mappings
    word_to_index = {}
    next_index = 0
    
    # Initialize with a small matrix
    matrix_size = 1000000
    word_matrix = csr_matrix((matrix_size, matrix_size), dtype=np.int32)  # Initialize with CSR format
    word_count = Counter()
    
    # Load existing matrix and word count if they exist
    try:
        if os.path.exists(matrix_save_path):
            word_matrix = load_npz(matrix_save_path).tocsr()  # Convert to CSR format for ease of use
        else:
            offset = 0
            save_offset(offset, offset_file)
    except Exception as e:
        logging.error(f"Error loading matrix file: {e}. Starting with an empty matrix.")
        word_matrix = csr_matrix((matrix_size, matrix_size), dtype=np.int32)
        
    
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
            rows_list = np.empty(batch_size,dtype=object)
            cols_list = np.empty(batch_size,dtype=object)
            #data = []
            
            for batch_num,row in enumerate(batch):
                comment = clean_comment(row[0])  # Access 'content' using index 0
                words = comment.split() 
                filtered_words = filter_stop_words(words)
                
                # Update the word count
                unique_words = set(filtered_words)
                if not unique_words:
                    next
                word_count.update(unique_words)
                
                # Map words to indices and handle new words
                indices = np.empty(len(unique_words),dtype=int)
                for i,word in enumerate(unique_words):
                    if word not in word_to_index:
                        word_to_index[word] = next_index
                        next_index += 1
                        # Expand the matrix if needed
                        if next_index >= word_matrix.shape[0]:
                            new_size = word_matrix.shape[0] * 2
                            new_matrix = csr_matrix((new_size, new_size), dtype=np.int32)
                            new_matrix[:word_matrix.shape[0], :word_matrix.shape[1]] = word_matrix
                            word_matrix = new_matrix
                            logging.info(f"Expanded matrix size to {new_size} x {new_size}.")
                    indices[i] = word_to_index[word]
                #num_words = indices.size
                #size = (num_words * (num_words - 1)) // 2
                sort_indices = np.sort(indices)
                # Generate index pairs for the lower triangular part of the matrix
                #i_indices, j_indices = np.tril_indices(num_words, -1)
                i_indices, j_indices = get_cached_indices(indices.size)

                # Preallocate arrays
                #rows_list[batch_num] = np.empty(size, dtype=int)
                #cols_list[batch_num] = np.empty(size, dtype=int)

                # Populate the rows and cols lists
                rows_list[batch_num] = sort_indices[i_indices]
                cols_list[batch_num] = sort_indices[j_indices]
                # Use the mask to append values to the lists
            # Apply updates to the matrix
            rows = np.concatenate(rows_list)
            cols = np.concatenate(cols_list)
            #update_matrix = csr_matrix((np.ones(len(rows)), (rows, cols)), shape=word_matrix.shape)
            # Add the update to the existing matrix
            word_matrix = word_matrix + csr_matrix((np.ones(len(rows), dtype=np.int32), (rows, cols)), shape=word_matrix.shape,dtype=np.int32) # Convert update to CSR and add to existing matrix
            
            process_end_time = time.time()
            
            # Save data every `save_interval` batches
            batch_counter += 1
            if batch_counter >= save_interval:
                break
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
        save_offset(offset, offset_file)
        pass
        
    end_time = time.time()
    logging.info(f"Total processing time: {end_time - start_time:.2f} seconds.")
    logging.info("Processing completed.")


def profile_code():
    # Your function call to process_batches
    process_batches(batch_size=100000, save_interval=10)
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
