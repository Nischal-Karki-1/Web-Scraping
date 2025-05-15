import json
import time
import random
import requests
import logging
import os
import sys
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
import errno


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
    "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko); compatible; GPTBot/1.1; +https://openai.com/gptbot)",
    "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko); compatible; ChatGPT-User/1.0; +https://openai.com/bot)",
    "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko); compatible; OAI-SearchBot/1.0; +https://openai.com/searchbot)",
    "Mozilla/5.0 (compatible; anthropic-ai/1.0; +http://www.anthropic.com/bot.html)",
    "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko); compatible; ClaudeBot/1.0; +claudebot@anthropic.com)",
    "Mozilla/5.0 (compatible; claude-web/1.0; +http://www.anthropic.com/bot.html)",
]
index = 0
direction = 1

# Files for state management
STATE_FILE = 'crawler_state.json'
RESULTS_FILE = 'all_domain_records.json'
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
    """Search the Common Crawl index for a specific domain with robust error handling."""
    # Create a wildcard search for the domain
    search_url = f"*.{domain}/*"
    URL_paths = []
    total_lines = 0
    session = create_robust_session()
    
    encoded_url = quote_plus(search_url)
    for INDEX_NAME in INDICES:
        # Construct the index query URL
        delay = random.uniform(20, 30)
        print(f'Wating processing: {INDEX_NAME}')
        logger.info(f'Waiting {delay:.2f}s before processing: {INDEX_NAME}')
        time.sleep(delay)
        index_url = f'{SERVER}{INDEX_NAME}-index?url={encoded_url}&output=json'
        
        logger.info(f"Querying index at: {index_url}")
        
        content = None
        try:
            print(f"Querying index at: {index_url}")
            content = make_request_with_retry(index_url, session=session)
        except Exception as e:
            logger.error(f"Fatal error querying {INDEX_NAME}: {e}")
            # Try to create a new session for next index
            session = create_robust_session()
            continue
            
        if not content:
            logger.warning(f"Skipping index {INDEX_NAME} due to failed requests")
            continue
            
        try:
            lines = content.strip().split('\n')
            total_lines += len(lines)
            
            for line in lines:
                if line.strip():
                    try:
                        record = json.loads(line)
                        if record.get("status") == '200':
                            URL_paths.append(record.get("url"))
                    except json.JSONDecodeError:
                        logger.warning(f"Could not parse line: {line[:100]}...")
        except Exception as e:
            logger.error(f"Error processing response: {e}")
    
    return URL_paths, total_lines

def save_state(data, current_index, current_domain_index=None, current_domain=None):
    """Save current processing state to resume later"""
    state = {
        'last_processed_index': current_index,
        'last_domain_index': current_domain_index,
        'last_domain': current_domain,
        'timestamp': time.time(),
        'remaining_domains': len(data) - current_index - 1 if current_index < len(data) else 0
    }
    
    # Atomic write - first write to temporary file then rename
    with open(TEMP_STATE_FILE, 'w') as f:
        json.dump(state, f, indent=4)
    
    # Atomic rename operation
    os.replace(TEMP_STATE_FILE, STATE_FILE)
    
    logger.info(f"Saved state at index {current_index}, domain: {current_domain}")

def load_state():
    """Load previous processing state"""
    if not os.path.exists(STATE_FILE):
        logger.info("No previous state found, starting fresh")
        return None
    
    try:
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
        
        logger.info(f"Loaded previous state: last processed index {state.get('last_processed_index')}, domain: {state.get('last_domain')}")
        return state
    except Exception as e:
        logger.error(f"Error loading state: {e}")
        # Create backup of potentially corrupted file
        if os.path.exists(STATE_FILE):
            backup_file = f"{STATE_FILE}.bak.{int(time.time())}"
            os.rename(STATE_FILE, backup_file)
            logger.info(f"Created backup of corrupted state file: {backup_file}")
        return None

def load_existing_results():
    """Load existing results to continue appending"""
    if not os.path.exists(RESULTS_FILE):
        return []
    
    try:
        with open(RESULTS_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading existing results: {e}")
        # Create backup of potentially corrupted file
        if os.path.exists(RESULTS_FILE):
            backup_file = f"{RESULTS_FILE}.bak.{int(time.time())}"
            os.rename(RESULTS_FILE, backup_file)
            logger.info(f"Created backup of results file: {backup_file}")
        return []

def save_results(records):
    """Save results with atomic write to prevent corruption"""
    # Write to temporary file first
    temp_file = f"{RESULTS_FILE}.tmp"
    with open(temp_file, 'w') as f:
        json.dump(records, f, indent=4)
    
    # Then do an atomic replace
    os.replace(temp_file, RESULTS_FILE)
    logger.info(f"Saved {len(records)} records to {RESULTS_FILE}")

def main():
    input_url = './URL1.json'
    INDICES = [
        "CC-MAIN-2025-13", "CC-MAIN-2025-08", "CC-MAIN-2025-05", "CC-MAIN-2024-51", 
        "CC-MAIN-2024-46", "CC-MAIN-2024-42", "CC-MAIN-2024-38", "CC-MAIN-2024-33", 
        "CC-MAIN-2024-30", "CC-MAIN-2024-26", "CC-MAIN-2024-22", "CC-MAIN-2024-18", 
        "CC-MAIN-2024-10", "CC-MAIN-2023-50", "CC-MAIN-2023-40"
    ]
    
    try:
        with open(input_url, 'r') as file:
            data = json.load(file)
    except Exception as e:
        logger.error(f"Failed to load input data: {e}")
        return
    
    # Load previous state if exists
    state = load_state()
    start_idx = state['last_processed_index'] + 1 if state and 'last_processed_index' in state else 0
    
    # Load existing results
    all_records = load_existing_results()
    
    logger.info(f"Starting to process {len(data)} domains from index {start_idx}")
    
    try:
        # Process each domain
        for i in range(start_idx, len(data)):
           
            domain_data = data[i]
            domain = domain_data.get('URL')
            print(f"Waiting for {domain}")
            logger.info(f"Processing {i+1}/{len(data)}: {domain}")
            
            try:
                # Search Common Crawl index for this domain
                URL_paths, total_lines = search_cc_index(domain, INDICES)
                
                # Create record with domain info
                record = {
                    "id": i,
                    "domain": domain,
                    "URL_paths": URL_paths,
                    "total_response_lines": total_lines
                }
                
                # Add to our collection
                all_records.append(record)
                
                # Save results incrementally with atomic write
                save_results(all_records)
                
                # Save state after each successful processing
                save_state(data, i, None, domain)
                
                logger.info(f"Successfully processed domain: {domain} - Found {len(URL_paths)} URLs from {total_lines} response lines")
                
                # Add delay between domains
                if i < len(data) - 1:  # No need to delay after the last domain
                    delay = random.uniform(50,60 )
                    logger.info(f"Waiting {delay:.2f} seconds before next domain...")
                    time.sleep(delay)
                    
            except Exception as e:
                logger.error(f"Error processing domain {domain}: {e}", exc_info=True)
                save_state(data, i-1, None, domain)  # Save state before the error
                # Continue with next domain
                
    except KeyboardInterrupt:
        logger.warning("Process interrupted by user")
        current_i = i if 'i' in locals() else start_idx
        save_state(data, current_i-1 if current_i > 0 else 0, None, domain if 'domain' in locals() else None)
        logger.info("State saved, you can resume later")
    except Exception as e:
        logger.error(f"Fatal error in main process: {e}", exc_info=True)
        if 'i' in locals() and 'domain' in locals():
            save_state(data, i-1, None, domain)
    finally:
        # Final save
        if 'all_records' in locals():
            logger.info(f"Process completed or interrupted. Processed {len(all_records)} domains.")

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