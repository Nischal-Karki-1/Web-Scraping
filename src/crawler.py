import json
import time
import random
import requests
import logging
import os
import sys
import datetime
from http.client import RemoteDisconnected
from urllib.parse import quote_plus
from requests.exceptions import (
    ChunkedEncodingError, 
    ConnectionError, 
    Timeout, 
    RequestException,
    TooManyRedirects,
    ProxyError,
    SSLError
)
from urllib3.exceptions import (
    ProtocolError,
    ReadTimeoutError,
    IncompleteRead,
    ConnectTimeoutError,
    MaxRetryError,
    NewConnectionError
)
from socket import error as SocketError
import psycopg
from database import get_connection, return_connection, close_all_connections
from urllib.parse import urlparse, urljoin
import logging

# Define the path to your log file on your local machine
local_log_path = './local_crawler.log'  

# First try to clean up any existing handlers
root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
        root.removeHandler(handler)

# Configure logging
# Create a file handler that writes to your local path
file_handler = logging.FileHandler(local_log_path, mode='a')
console_handler = logging.StreamHandler()

# Configure formatters
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Set up the root logger
root.setLevel(logging.INFO)
root.addHandler(file_handler)
root.addHandler(console_handler)

# Create our named logger
logger = logging.getLogger(__name__)

# Test the logger
logger.info("Logging system initialized successfully")

SERVER = 'https://index.commoncrawl.org/'
user_agents = [
    
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/113.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:113.0) Gecko/20100101 Firefox/113.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.5615.121 Safari/537.36"
    
]
index = 0
direction = 1

# Files for state management
STATE_FILE = 'crawler_state.json'
TEMP_STATE_FILE = 'temp_crawler_state.json'  # For atomic state updates

# Constants for retry settings
MAX_RETRIES = 12
INITIAL_BACKOFF = 2
MAX_BACKOFF = 300  
SESSION_RESET_THRESHOLD = 4 

import logging
from urllib.parse import urlparse, urljoin

def filter_url_path_before_storing_into_database(domain, url_paths):
    filtered_url_paths = []

    if not url_paths:
        logging.info('No URLs found')
        return []

    if not domain.startswith('http'):
        domain = f'https://{domain}'

    for url in url_paths:
        try:
            if not url.startswith('http'):
                url = urljoin(domain, url)

            # Discard unnecessary patterns in URL
            if any(discard_pattern in url for discard_pattern in ["english.", "en.", "robots.txt", "robot.txt"]):
                continue

            url_path = urlparse(url).netloc
            domain_path = urlparse(domain).netloc

            # Discard if domain doesn't match
            if url_path.replace('www.', '') != domain_path.replace('www.', ''):
                continue

            filtered_url_paths.append(url)

        except Exception as e:
            logging.info(f'Error filtering the URL: {e}')

    return filtered_url_paths




def insert_into_url_registry_table(conn, domain_name, timestamp, index, url_paths):
    """
    Batch insert with detailed feedback about insertions vs conflicts
    """
    if not url_paths:
        logger.info("No URLs to insert")
        return {"inserted": 0, "duplicates": 0, "total": 0}
    
    data_to_insert = [
        (domain_name, timestamp, index, url_path, 'pending') 
        for url_path in url_paths
    ]
    
    with conn.transaction():
        with conn.cursor() as cur:
            
            cur.executemany(
                """
                INSERT INTO url_registry (domain, accessTimestamp, index, urlPath, status)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (urlPath) DO UPDATE SET
                    domain = EXCLUDED.domain  -- This ensures rowcount is accurate
                WHERE url_registry.urlPath IS NULL;  -- This condition will never be true for existing rows
                """,
                data_to_insert
            )
            
            # Now get accurate counts
            rows_affected = cur.rowcount
            
            # Count actual new insertions vs updates (conflicts)
            cur.execute(
                """
                SELECT COUNT(*) FROM url_registry 
                WHERE urlPath = ANY(%s) AND domain = %s AND index = %s
                """,
                (url_paths, domain_name, index)
            )
            total_existing = cur.fetchone()[0]
            
            inserted = rows_affected
            duplicates = len(url_paths) - inserted
            
            logger.info(f"Batch insert results: {inserted} new URLs inserted, {duplicates} duplicates skipped, {len(url_paths)} total processed for domain {domain_name}")
            
            return {
                "inserted": inserted,
                "duplicates": duplicates, 
                "total": len(url_paths)
            }

def get_next_agent():
    """Rotate through user agents to avoid being blocked"""
    global index, direction
    agent = user_agents[index]
    
    index += direction
    if index >= len(user_agents):
        direction = -1
        index = len(user_agents) - 2
    elif index < 0:
        direction = 1
        index = 1
    return agent

def create_robust_session():
    """Create a requests session with configured retries and timeouts"""
    session = requests.Session()
    # Configure longer timeouts
    session.timeout = (15, 45)  # (connect timeout, read timeout)
    return session

def make_request_with_retry(url, session=None, max_retries=MAX_RETRIES, backoff_factor=INITIAL_BACKOFF, timeout=(15, 45)):
    """Make HTTP request with exponential backoff retry logic and session management"""
    if session is None:
        session = create_robust_session()
        
    retries = 0
    consecutive_failures = 0
    
    while retries < max_retries:
        try:
            myagent = get_next_agent()
            print(f'Using agent: {myagent}')
            logger.info(f'Using agent: {myagent}')
            
            headers = {
                'User-Agent': myagent,
                'Accept': 'text/html,application/json,*/*',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
            }
            
            # Make the request
            response = session.get(
                url, 
                headers=headers,
                timeout=timeout,
                stream=True  # Important for handling large responses
            )
            
            # Check if we got a valid response
            response.raise_for_status()
            
            # Read the full content to detect potential chunking errors early
            content = response.text
            
            consecutive_failures = 0  # Reset failure counter on success
            return content
            
        except (
            ChunkedEncodingError, 
            ConnectionError, 
            Timeout, 
            ProtocolError, 
            IncompleteRead, 
            ReadTimeoutError,
            ConnectTimeoutError,
            NewConnectionError,
            MaxRetryError,
            RemoteDisconnected,
            SocketError
        ) as e:
            consecutive_failures += 1
            retries += 1
            
            # Calculate backoff with exponential increase but capped maximum
            current_backoff = min(backoff_factor * (2 ** (retries - 1)), MAX_BACKOFF)
            jitter = random.uniform(0.5, 1.5)  # Add randomness to avoid thundering herd
            wait_time = current_backoff * jitter
            
            logger.warning(f"Request failed ({e.__class__.__name__}): {e}. Retry {retries}/{max_retries} in {wait_time:.2f}s")
            
            # If we've had multiple consecutive failures, create a new session
            if consecutive_failures >= SESSION_RESET_THRESHOLD:
                logger.info("Multiple consecutive failures. Creating new session.")
                session = create_robust_session()
                consecutive_failures = 0
            
            # Wait before retrying
            time.sleep(wait_time)
            
        except (TooManyRedirects, ProxyError, SSLError) as e:
            logger.error(f"Fatal network error: {e.__class__.__name__}: {e}")
            raise
            
        except Exception as e:
            logger.error(f"Unexpected error: {e.__class__.__name__}: {e}")
            retries += 1
            wait_time = backoff_factor * (2 ** retries) + random.uniform(1, 5)
            time.sleep(wait_time)
    
    logger.error(f"Failed after {max_retries} retries for URL: {url}")
    return None

def search_single_cc_index(domain, index_name):
    """Search a single Common Crawl index for a specific domain."""
    # Create a wildcard search for the domain
    search_url = f"*.{domain}/*"
    session = create_robust_session()
    
    encoded_url = quote_plus(search_url)
    
    try:
        # Construct the index query URL
        index_url = f'{SERVER}{index_name}-index?url={encoded_url}&output=json'
        
        logger.info(f"Querying index: {index_name} for domain: {domain}")
        logger.info(f"Query URL: {index_url}")
        
        content = make_request_with_retry(index_url, session=session)
        
        if not content:
            logger.warning(f"No content returned for index {index_name}")
            return None, 0
        
        # Process the content
        lines = content.strip().split('\n')
        index_urls = []
        
        logger.info(f"Processing {len(lines)} lines from {index_name}")
        
        for line_idx, line in enumerate(lines):
            if line.strip():
                try:
                    record = json.loads(line)
                    if record.get("status") == '200':
                        index_urls.append(record.get("url"))
                except json.JSONDecodeError:
                    logger.warning(f"Could not parse line {line_idx + 1}: {line[:100]}...")
        
        # Return index data if we found any URLs
        if index_urls:
            index_data = {
                "index": index_name,
                "url_paths": index_urls
            }
            logger.info(f"Successfully processed {index_name}: found {len(index_urls)} valid URLs from {len(lines)} total lines")
            return index_data, len(lines)
        else:
            logger.info(f"No valid URLs found in {index_name}")
            return None, 0
            
    except Exception as e:
        logger.error(f"Error processing index {index_name}: {e}")
        return None, 0

def save_state(current_file_idx, current_index_position, processed_indices, current_domain_file=None, total_files=None):
    """Save current processing state to resume later"""
    state = {
        'last_processed_file_idx': current_file_idx,
        'last_processed_index_position': current_index_position,
        'processed_indices': processed_indices,  # List of completed index names for current file
        'last_domain_file': current_domain_file,
        'timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'total_files': total_files,
        'remaining_files': total_files - current_file_idx - 1 if total_files and current_file_idx < total_files else 0
    }
    
    # Atomic write - first write to temporary file then rename
    with open(TEMP_STATE_FILE, 'w') as f:
        json.dump(state, f, indent=4)
    
    # Atomic rename operation
    os.replace(TEMP_STATE_FILE, STATE_FILE)
    
    logger.info(f"Saved state at file {current_file_idx}, index position {current_index_position}, processed indices: {processed_indices}")
    
def load_state():
    """Load previous processing state"""
    if not os.path.exists(STATE_FILE):
        logger.info("No previous state found, starting fresh")
        return None
    
    try:
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
        
        logger.info(f"Loaded previous state: file {state.get('last_processed_file_idx')}, index position {state.get('last_processed_index_position')}, processed indices: {state.get('processed_indices', [])}")
        return state
    except Exception as e:
        logger.error(f"Error loading state: {e}")
        # Create backup of potentially corrupted file
        if os.path.exists(STATE_FILE):
            backup_file = f"{STATE_FILE}.bak.{int(time.time())}"
            os.rename(STATE_FILE, backup_file)
            logger.info(f"Created backup of corrupted state file: {backup_file}")
        return None

def save_domain_file(file_path, data_to_save, filename):
    """Save domain file with atomic write operation"""
    temp_file_path = f"{file_path}.temp"
    
    try:
        # Write to temporary file first
        with open(temp_file_path, 'w', encoding='utf-8') as file:
            json.dump(data_to_save, file, indent=4, ensure_ascii=False)
        
        # Atomic rename operation
        os.replace(temp_file_path, file_path)
        logger.info(f"Successfully saved domain file: {filename}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to save domain file {filename}: {e}")
        # Clean up temp file if it exists
        if os.path.exists(temp_file_path):
            try:
                os.remove(temp_file_path)
            except:
                pass
        return False
    
def update_domain_file_with_new_index_data(conn, domain_file_data, new_index_data):
    """
    Update domain file data with new index data using stack approach (LIFO)
    
    Args: 
        conn: Database connection object
        domain_file_data: Existing domain file data dictionary
        new_index_data: New index entry with structure {"index": "name", "url_paths": [...]}
    
    Returns:
        Updated domain file data dictionary, total_lines_added
    """    
        
    # Ensure URL_paths exists
    if 'URL_paths' not in domain_file_data:
        domain_file_data['URL_paths'] = []
        
    # Get the existing indices
    existing_indices = {item['index'] for item in domain_file_data.get('URL_paths', [])}
    
    index_name = new_index_data.get('index')
    url_paths = new_index_data.get('url_paths', [])
    total_lines_added = len(url_paths)
    
    if index_name and index_name not in existing_indices:
        # Get domain from domain_file_data
        domain = domain_file_data.get('domain', '')
        
        # Call filtered_url function
        filtered_url_paths = filter_url_path_before_storing_into_database(domain, url_paths)

        if filtered_url_paths:
            # Insert into database - now passing the list correctly
            try:
                insert_into_url_registry_table(  # Using batch version for better performance
                    conn, 
                    domain_name=domain,
                    timestamp=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
                    index=index_name, 
                    url_paths=filtered_url_paths  # This is now correctly a list
                )
            except Exception as e:
                logger.error(f"Failed to insert URLs into database: {e}")
             

        # Add new entry to the TOP of the list (stack behavior - LIFO)
        domain_file_data['URL_paths'].insert(0, new_index_data)
        logger.info(f"Added index to top of stack: {index_name} with {len(url_paths)} URLs")
        
        # Update total_lines
        current_total = domain_file_data.get('total_lines', 0)
        domain_file_data['total_lines'] = current_total + total_lines_added
        
        # Update timestamp to today
        domain_file_data['timestamp'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        return domain_file_data, total_lines_added
    else:
        logger.info(f"Index {index_name} already exists, skipping")
        return domain_file_data, 0

def main():
    
    INDICES = [
        "CC-MAIN-2025-18", "CC-MAIN-2025-21"
    ]

    input_url = './assets/newmediadomains'

    try:
        files = os.listdir(input_url)
        json_files = [f for f in files if f.endswith('.json')]
    except Exception as e:
        logger.error(f'Failed to load the newmediadomains folder: {e}')
        return

    # Load previous state if exists
    state = load_state()
    start_file_idx = state.get('last_processed_file_idx', 0) if state else 0
    start_index_position = state.get('last_processed_index_position', 0) if state else 0
    processed_indices = state.get('processed_indices', []) if state else []

    logger.info(f"Starting to process {len(json_files)} domain files from file index {start_file_idx}, index position {start_index_position}")

    try:
        count = 0
        
        # For each domain file in the folder (starting from the saved file index)
        for file_idx in range(start_file_idx, len(json_files)):
            filename = json_files[file_idx]
            file_path = os.path.join(input_url, filename)
            
            logger.info(f"Processing domain file {file_idx + 1}/{len(json_files)}: {filename}")
            
            try:
                with open(file_path, 'r', encoding='utf-8') as file:
                    loaded_data = json.load(file)
            except Exception as e:
                logger.error(f'Failed to load domain file {filename}: {e}')
                # Save state and continue to next file
                save_state(file_idx + 1, 0, [], None, len(json_files))
                continue
            
            # Handle both list and single object structures
            if isinstance(loaded_data, list):
                if len(loaded_data) == 0:
                    logger.error(f'Empty list in file {filename}')
                    save_state(file_idx + 1, 0, [], None, len(json_files))
                    continue
                # Take the first (and presumably only) domain from the list
                domain_file_data = loaded_data[0]
                logger.info(f'File {filename} contains a list with {len(loaded_data)} items, using first item')
            elif isinstance(loaded_data, dict):
                domain_file_data = loaded_data
                logger.info(f'File {filename} contains a single domain object')
            else:
                logger.error(f'Unexpected data structure in file {filename}: {type(loaded_data)}')
                save_state(file_idx + 1, 0, [], None, len(json_files))
                continue
            
            # Get domain name from the domain file
            domain = domain_file_data.get('domain', '')
            if not domain:
                logger.error(f'No domain found in file {filename}')
                save_state(file_idx + 1, 0, [], None, len(json_files))
                continue
            
            # Determine starting index position and processed indices (only for the resumed file)
            if file_idx == start_file_idx:
                # Resume from saved position
                current_index_position = start_index_position
                current_processed_indices = processed_indices.copy()
            else:
                # Fresh start for new file
                current_index_position = 0
                current_processed_indices = []
            
            logger.info(f'Starting index position: {current_index_position}, Already processed indices: {current_processed_indices}')
            
            try:
                # Process each index starting from the current position
                for index_position in range(current_index_position, len(INDICES)):
                    index_name = INDICES[index_position]
                    
                    # Skip if this index was already processed for this file
                    if index_name in current_processed_indices:
                        logger.info(f"Index {index_name} already processed for {domain}, skipping")
                        continue
                    
                    logger.info(f"Processing domain file {file_idx + 1}/{len(json_files)}, index {index_position + 1}/{len(INDICES)}: {index_name} for domain {domain}")
                    
                    # Add delay before processing index
                    delay = random.uniform(20, 30)
                    logger.info(f'Waiting {delay:.2f}s before processing index: {index_name}')
                    
                    # Check for interruption during delay
                    start_time = time.time()
                    while time.time() - start_time < delay:
                        time.sleep(1)  # Sleep in smaller chunks to allow interruption
                    
                    index_processing_successful = False
                    
                    try:
                        logger.info(f"Starting processing of index: {index_name} for domain: {domain}")
                        
                        # Search single Common Crawl index for this domain
                        index_data, total_lines = search_single_cc_index(domain, index_name)
                        
                        if index_data and total_lines > 0:
                            # Update the domain file data
                            domain_file_data, lines_added = update_domain_file_with_new_index_data(
                                domain_file_data, index_data
                            )
                            
                            if lines_added > 0:
                                # SAVE DOMAIN FILE IMMEDIATELY AFTER EACH SUCCESSFUL INDEX
                                if isinstance(loaded_data, list):
                                    # Update the first item in the list
                                    loaded_data[0] = domain_file_data
                                    data_to_save = loaded_data
                                else:
                                    # Save as single object
                                    data_to_save = domain_file_data
                                
                                # Save the domain file immediately
                                if save_domain_file(file_path, data_to_save, filename):
                                    logger.info(f"Successfully saved domain file after processing index {index_name} - Added {lines_added} URLs")
                                else:
                                    logger.error(f"Failed to save domain file after processing index {index_name}")
                                    # Don't mark as successful if we couldn't save
                                    continue
                            
                        else:
                            logger.info(f"No new data found for domain {domain} in index {index_name}")
                        
                        # Mark as successful only if we reach this point
                        index_processing_successful = True
                        logger.info(f"Completed processing index: {index_name} for domain: {domain}")
                        
                    except Exception as e:
                        logger.error(f"Error processing index {index_name} for domain {domain}: {e}", exc_info=True)
                        # Don't save state - we want to retry this index
                        logger.warning(f"Index {index_name} processing failed - will retry on resume")
                        continue
                    
                    # CRITICAL: Only save state after successful completion of this index
                    if index_processing_successful:
                        # Add this index to processed list
                        current_processed_indices.append(index_name)
                        
                        # Save state with current position
                        print(f'Domain: {domain}')
                        save_state(file_idx, index_position, current_processed_indices, domain, len(json_files))
                        logger.info(f"Saved state after successful completion of index: {index_name}")
                        
                        # Add delay between indices (only after successful processing)
                        if index_position < len(INDICES) - 1:
                            delay = random.uniform(50, 60)
                            logger.info(f"Waiting {delay:.2f} seconds before next index...")
                            time.sleep(delay)
                
                # All indices processed for this file
                # Move to next file - reset index position and processed indices
                save_state(file_idx + 1, 0, [], None, len(json_files))
                
                count += 1
               
            except Exception as e:
                logger.error(f'Failed to process domain file {filename}: {e}')
                # Don't advance - let it retry the same file
                continue
                
        logger.info(f"Processing completed. Total domain files processed: {count}")
                
    except KeyboardInterrupt:
        logger.warning("Process interrupted by user")
        logger.info("State saved at last successfully completed index. Incomplete index will be retried on resume.")
    except Exception as e:
        logger.error(f"Fatal error in main process: {e}", exc_info=True)
    finally:
        logger.info(f"Process completed. Total domain files processed: {count if 'count' in locals() else 0}")
        
        
def resume_from_crash():
    """Function to resume processing after a crash"""
    logger.info("Attempting to resume from previous crash...")
    
    # Check if we have a valid state file
    state = load_state()
    if not state:
        logger.error("No previous state found. Cannot resume.")
        return False
    
    logger.info(f"Found previous state. Last processed domain file: {state.get('last_domain_file')} (file index: {state.get('last_processed_file_idx')}, index position: {state.get('last_processed_index_position')})")
    
    # Re-run the main process
    main()
    return True

if __name__ == "__main__":
    # Check for resume argument
    if len(sys.argv) > 1 and sys.argv[1] == '--resume':
        resume_from_crash()
    else:
        main()