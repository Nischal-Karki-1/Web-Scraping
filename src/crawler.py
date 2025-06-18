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

# Set up logging
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
    "Mozilla/5.0 (iPhone; CPU iPhone OS 8_3 like Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Version/8.0 Mobile/12F70 Safari/600.1.4 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 8_3 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Version/8.0 Mobile/12F70 Safari/600.1.4 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko); compatible; OAI-SearchBot/1.0; +https://openai.com/searchbot)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/600.2.5 (KHTML, like Gecko) Version/8.0.2 Safari/600.2.5 (Applebot/0.1)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/600.2.5 (KHTML, like Gecko) Version/8.0.2 Safari/600.2.5 (Applebot/0.1; +http://www.apple.com/go/applebot)",
    "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko); compatible; ClaudeBot/1.0; +claudebot@anthropic.com)",
    
]
index = 0
direction = 1

# Files for state management
STATE_FILE = 'crawler_state.json'
TEMP_STATE_FILE = 'temp_crawler_state.json'  # For atomic state updates

# Constants for retry settings
MAX_RETRIES = 8
INITIAL_BACKOFF = 2
MAX_BACKOFF = 240  # 4 minutes max backoff
SESSION_RESET_THRESHOLD = 3  # Reset session after this many failed requests

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

def search_cc_index(domain, INDICES):
    """Search the Common Crawl index for a specific domain with robust error handling and interruption safety."""
    # Create a wildcard search for the domain
    search_url = f"*.{domain}/*"
    URL_paths = []
    total_lines = 0
    session = create_robust_session()
    
    encoded_url = quote_plus(search_url)
    
    logger.info(f"Starting search for domain: {domain} across {len(INDICES)} indices")
    
    # Track progress for better logging
    completed_indices = 0
    
    for idx, INDEX_NAME in enumerate(INDICES):
        try:
            # Construct the index query URL
            delay = random.uniform(20, 30)
            logger.info(f'Processing index {idx + 1}/{len(INDICES)}: {INDEX_NAME} for domain {domain}')
            logger.info(f'Waiting {delay:.2f}s before processing: {INDEX_NAME}')
            
            # Check for interruption during delay
            start_time = time.time()
            while time.time() - start_time < delay:
                time.sleep(1)  # Sleep in smaller chunks to allow interruption
                # This allows KeyboardInterrupt to be caught more quickly
            
            index_url = f'{SERVER}{INDEX_NAME}-index?url={encoded_url}&output=json'
            
            logger.info(f"Querying index at: {index_url}")
            
            content = None
            try:
                print(f"Querying index {idx + 1}/{len(INDICES)}: {INDEX_NAME}")
                content = make_request_with_retry(index_url, session=session)
            except KeyboardInterrupt:
                logger.warning(f"Keyboard interrupt during request for {INDEX_NAME}")
                # Don't return partial results - let the interruption bubble up
                raise  # Re-raise to be caught by outer handler
            except Exception as e:
                logger.error(f"Fatal error querying {INDEX_NAME}: {e}")
                # Try to create a new session for next index
                session = create_robust_session()
                continue
                
            if not content:
                logger.warning(f"Skipping index {INDEX_NAME} due to failed requests")
                continue
            
            # Process the content and organize by index
            try:
                lines = content.strip().split('\n')
                index_urls = []
                
                logger.info(f"Processing {len(lines)} lines from {INDEX_NAME}")
                
                for line_idx, line in enumerate(lines):
                    if line.strip():
                        try:
                            record = json.loads(line)
                            if record.get("status") == '200':
                                index_urls.append(record.get("url"))
                        except json.JSONDecodeError:
                            logger.warning(f"Could not parse line {line_idx + 1}: {line[:100]}...")
                    
                    # Periodic interruption check for large responses
                    if line_idx > 0 and line_idx % 1000 == 0:
                        logger.debug(f"Processed {line_idx}/{len(lines)} lines from {INDEX_NAME}")
                
                # Add this index's data to URL_paths if we found any URLs
                if index_urls:
                    URL_paths.append({
                        "index": INDEX_NAME,
                        "url_paths": index_urls
                    })
                    total_lines += len(lines)
                    logger.info(f"Successfully processed {INDEX_NAME}: found {len(index_urls)} valid URLs from {len(lines)} total lines")
                else:
                    logger.info(f"No valid URLs found in {INDEX_NAME}")
                
                # Mark this index as completed
                completed_indices += 1
                logger.info(f"Completed {completed_indices}/{len(INDICES)} indices for domain {domain}")
                    
            except KeyboardInterrupt:
                logger.warning(f"Keyboard interrupt during content processing for {INDEX_NAME}")
                logger.info(f"Completed {completed_indices}/{len(INDICES)} indices before interruption")
                # Don't return partial results for domain processing
                raise  # Re-raise to be caught by outer handler
            except Exception as e:
                logger.error(f"Error processing response for {INDEX_NAME}: {e}")
        
        except KeyboardInterrupt:
            logger.warning(f"Search interrupted at index {INDEX_NAME} for domain {domain}")
            logger.info(f"Completed {completed_indices}/{len(INDICES)} indices before interruption")
            # For domain-level processing, we want all-or-nothing
            # Don't return partial results - let the interruption bubble up to main()
            raise
        except Exception as e:
            logger.error(f"Unexpected error processing index {INDEX_NAME}: {e}")
            continue
    
    logger.info(f"Successfully completed search for domain {domain}: {len(URL_paths)} indices processed, {total_lines} total lines")
    return URL_paths, total_lines


def save_state(current_file_idx, current_domain_idx, current_domain=None, total_files=None):
    """Save current processing state to resume later"""
    state = {
        'last_processed_file_idx': current_file_idx,
        'last_processed_domain_idx': current_domain_idx,
        'last_domain': current_domain,
        'timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'total_files': total_files,
        'remaining_files': total_files - current_file_idx - 1 if total_files and current_file_idx < total_files else 0
    }
    
    # Atomic write - first write to temporary file then rename
    with open(TEMP_STATE_FILE, 'w') as f:
        json.dump(state, f, indent=4)
    
    # Atomic rename operation
    os.replace(TEMP_STATE_FILE, STATE_FILE)
    
    logger.info(f"Saved state at file {current_file_idx}, domain {current_domain_idx}: {current_domain}")
    
def load_state():
    """Load previous processing state"""
    if not os.path.exists(STATE_FILE):
        logger.info("No previous state found, starting fresh")
        return None
    
    try:
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
        
        logger.info(f"Loaded previous state: file {state.get('last_processed_file_idx')}, domain {state.get('last_processed_domain_idx')}, last domain: {state.get('last_domain')}")
        return state
    except Exception as e:
        logger.error(f"Error loading state: {e}")
        # Create backup of potentially corrupted file
        if os.path.exists(STATE_FILE):
            backup_file = f"{STATE_FILE}.bak.{int(time.time())}"
            os.rename(STATE_FILE, backup_file)
            logger.info(f"Created backup of corrupted state file: {backup_file}")
        return None
    
    
def update_domain_json_file_with_new_commoncrawl_indices(domain_data, new_indices_data, new_total_lines):
    """
    Update domain data with new indices using stack approach (LIFO)
    
    Args: 
        domain_data: Existing domain data dictionary
        new_indices_data: List of new index entries with structure [{"index": "name", "url_paths": [...]}]
        new_total_lines: Total lines from new indices to add
    
    Returns:
        Updated domain data dictionary
    """    
        
    # Ensure URL_paths exists
    if 'URL_paths' not in domain_data:
        domain_data['URL_paths'] = []
        
    # Get the existing indices
    existing_indices = {item['index'] for item in domain_data.get('URL_paths', [])}
    
    # Collect new indices to add (in reverse order for stack behavior)
    new_entries_to_add = []
    
    for index_entry in new_indices_data:
        index_name = index_entry.get('index')
        url_paths = index_entry.get('url_paths', [])
        
        if index_name and index_name not in existing_indices:
            new_index_entry = {
                "index": index_name,
                "url_paths": url_paths
            }
            new_entries_to_add.append(new_index_entry)
            logger.info(f"Prepared new index for stack addition: {index_name} with {len(url_paths)} URLs")
        else:
            logger.info(f"Index {index_name} already exists, skipping")
    
    # Add new entries to the TOP of the list (stack behavior - LIFO)
    # Insert in original order so that the last item in the list goes to the top
    for new_entry in new_entries_to_add:
        domain_data['URL_paths'].insert(0, new_entry)
        logger.info(f"Added index to top of stack: {new_entry['index']}")
    
    # Update total_lines
    current_total = domain_data.get('total_lines', 0)
    domain_data['total_lines'] = current_total + new_total_lines
    
    # Update timestamp to today
    domain_data['timestamp'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    return domain_data


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
    # FIXED: Resume from the same domain (don't add +1)
    start_domain_idx = state.get('last_processed_domain_idx', -1) if state else 0
    # Handle the initial case
    if start_domain_idx < 0:
        start_domain_idx = 0

    logger.info(f"Starting to process {len(json_files)} JSON files from file index {start_file_idx}, domain index {start_domain_idx}")

    try:
        count = 0
        
        # For each file in the folder newmediadomains (starting from the saved file index)
        for file_idx in range(start_file_idx, len(json_files)):
            filename = json_files[file_idx]
            file_path = os.path.join(input_url, filename)
            
            logger.info(f"Processing file {file_idx + 1}/{len(json_files)}: {filename}")
            
            try:
                with open(file_path, 'r', encoding='utf-8') as file:
                    data = json.load(file)
            except Exception as e:
                logger.error(f'Failed to load file {filename}: {e}')
                # Save state and continue to next file - no domain was processed
                save_state(file_idx, -1, None, len(json_files))
                continue
                
            # Track if the file was modified
            file_modified = False
            
            # Determine starting domain index (only for the resumed file)
            domain_start_idx = start_domain_idx if file_idx == start_file_idx else 0
            
            try:
                # Process each domain in the current file
                for domain_idx in range(domain_start_idx, len(data)):
                    if domain_idx < len(data) and data[domain_idx] and isinstance(data[domain_idx], dict):
                        domain_data = data[domain_idx]
                        domain = domain_data.get('domain', '')
                      
                        logger.info(f"Processing file {file_idx + 1}/{len(json_files)}, domain {domain_idx + 1}/{len(data)}: {domain}")
                        
                        # CRITICAL CHANGE: Don't save state here - only save after complete domain processing
                        domain_processing_successful = False
                        
                        try:
                            logger.info(f"Starting processing of all indices for domain: {domain}")
                            
                            # Search Common Crawl index for this domain (processes ALL indices)
                            URL_paths, total_lines = search_cc_index(domain, INDICES)
                            
                            if URL_paths and total_lines > 0:
                                # Update the domain data in the current file data
                                data[domain_idx] = update_domain_json_file_with_new_commoncrawl_indices(
                                    domain_data, URL_paths, total_lines
                                )
                                file_modified = True
                                
                                logger.info(f"Successfully updated domain: {domain} - Added {len(URL_paths)} new indices with {total_lines} total lines")
                            else:
                                logger.info(f"No new data found for domain: {domain}")
                            
                            # Mark as successful only if we reach this point
                            domain_processing_successful = True
                            logger.info(f"Completed processing all indices for domain: {domain}")
                            
                        except Exception as e:
                            logger.error(f"Error processing domain {domain}: {e}", exc_info=True)
                            # Don't save state - we want to retry this domain
                            logger.warning(f"Domain {domain} processing failed - will retry on resume")
                            continue
                        
                        # CRITICAL: Only save state after successful completion of ALL indices for this domain
                        if domain_processing_successful:
                            save_state(file_idx, domain_idx, domain, len(json_files))
                            logger.info(f"Saved state after successful completion of domain: {domain}")
                            
                            # Add delay between domains (only after successful processing)
                            if domain_idx < len(data) - 1:
                                delay = random.uniform(50, 60)
                                logger.info(f"Waiting {delay:.2f} seconds before next domain...")
                                time.sleep(delay)
                        
                    else:
                        logger.warning(f'Domain {domain_idx}: Not found or invalid structure in {filename}')
                        # For invalid domains, we can save state to skip them
                        save_state(file_idx, domain_idx, f"INVALID_DOMAIN_{domain_idx}", len(json_files))
                        
                    count += 1
                
                # Save the updated file if it was modified
                if file_modified:
                    try:
                        with open(file_path, 'w', encoding='utf-8') as file:
                            json.dump(data, file, indent=4, ensure_ascii=False)
                        logger.info(f"Saved updated file: {filename}")
                    except Exception as e:
                        logger.error(f"Failed to save file {filename}: {e}")
            
                # Reset domain start index for subsequent files
                start_domain_idx = 0
               
            except Exception as e:
                logger.error(f'Failed to process file {filename}: {e}')
                # Don't advance - let it retry the same file
                continue
                
        logger.info(f"Processing completed. Total domains processed: {count}")
                
    except KeyboardInterrupt:
        logger.warning("Process interrupted by user")
        logger.info("State saved at last successfully completed domain. Incomplete domain will be retried on resume.")
    except Exception as e:
        logger.error(f"Fatal error in main process: {e}", exc_info=True)
    finally:
        logger.info(f"Process completed. Total domains processed: {count if 'count' in locals() else 0}")
        
        
def resume_from_crash():
    """Function to resume processing after a crash"""
    logger.info("Attempting to resume from previous crash...")
    
    # Check if we have a valid state file
    state = load_state()
    if not state:
        logger.error("No previous state found. Cannot resume.")
        return False
    
    logger.info(f"Found previous state. Last processed domain: {state.get('last_domain')} (index: {state.get('last_processed_index')})")
    
    # Re-run the main process
    main()
    return True

if __name__ == "__main__":
    # Check for resume argument
    if len(sys.argv) > 1 and sys.argv[1] == '--resume':
        resume_from_crash()
    else:
        main()