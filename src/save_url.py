import json
import psycopg
import asyncio
import os
import logging
import re
from datetime import datetime
from urllib.parse import urlparse
from pathlib import Path
from database import get_connection, return_connection, close_all_connections




# Configure logging
logging.basicConfig(
    filename='dadef insert_into_url_registry_table(conn, ):

    do_insert_into_url_registry_table_block = f"""
            DO $$
            BEGIN
                INSERT INTO url_registry (domain, accessTimestamp, index, urlPath, status)
                VALUES (%s, %s, %s, %s, 'pending')
                ON CONFLICT (urlPath) DO NOTHING;
            END
            $$;
    """

    with psycopg.connect(conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute(do_insert_into_url_registry_table_block, (domain_from_url, timestamp, index_value, url_path))
tabase.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


async def load_json_file(file_path):
    """Load and parse a JSON file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        logger.info(f"JSON data loaded successfully from {file_path}")
        return data
    except Exception as e:
        logger.error(f"Error loading JSON file {file_path}: {e}")
        return None

async def create_table(conn):
    """Create table with url_id as primary key and url_path as unique constraint"""
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("""
                CREATE SEQUENCE IF NOT EXISTS url_id_seq START 1;
                                 
                CREATE TABLE IF NOT EXISTS url_registry (
                    urlID TEXT PRIMARY KEY DEFAULT 'url' || nextval('url_id_seq'),
                    domain VARCHAR(255) NOT NULL,
                    accessTimestamp TIMESTAMP NOT NULL,
                    index TEXT NOT NULL,
                    urlPath TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    CONSTRAINT unique_url_path UNIQUE (urlPath)
                );
                
                
            """)
        
        await conn.commit()
        logger.info("Table created successfully with primary key and unique constraints")
    except Exception as e:
        await conn.rollback()
        logger.error(f"Error creating table: {e}")
        
def should_skip_url(url):
    """Check if the URL should be skipped based on filtering criteria"""
    # Skip robots.txt URLs
    if "robots.txt" in url.lower():
        logger.debug(f"Skipping robots.txt URL: {url}")
        return True
    
    # Parse URL to check for english subdomain
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    
    # Check if domain starts with "english." subdomain pattern
    parts = domain.split('.')
    if len(parts) >= 3 and parts[0] == "english":
        logger.debug(f"Skipping english subdomain URL: {url}")
        return True
    
    return False

async def process_general_crawler_file(conn, file_path):
    """Process a single general crawler JSON file"""
    domain = Path(file_path).stem  # Extract domain name from filename
    json_data = await load_json_file(file_path)
    
    if not json_data:
        return 0, 0, 0  # Return zeros if file couldn't be loaded
    
    inserted_count = 0
    duplicate_count = 0
    filtered_count = 0
    
    try:
        async with conn.cursor() as cursor:
            for data in json_data:
                full_path = data.get('url')
                
                # Skip URLs that match filtering criteria
                if should_skip_url(full_path):
                    filtered_count += 1
                    continue
                
                parsed = urlparse(full_path)
                domain_from_url = parsed.netloc
                
                # Handle the different timestamp format
                timestamp_str = data.get('discovery_time', '')
                if 'T' in timestamp_str:
                    # Convert from format like '2025-04-26T01:50:37.054463'
                    timestamp_str = timestamp_str.replace('T', ' ').split('.')[0]
                
                try:
                    timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    timestamp = datetime.now()
                    logger.warning(f"Could not parse timestamp '{timestamp_str}', using current time")
                
                index = "General Crawler"
                url_path = data.get('url')
                
                try:
                    await cursor.execute("""
                        INSERT INTO url_registry (domain, accessTimestamp, index, urlPath, status) 
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (urlPath) DO NOTHING
                    """, (domain_from_url, timestamp, index, url_path, 'pending'))
                    
                    if cursor.rowcount > 0:
                        inserted_count += 1
                    else:
                        duplicate_count += 1
                except Exception as e:
                    logger.error(f"Error inserting record: {e}")
        
        await conn.commit()
        logger.info(f"Domain {domain} from General Crawler: {inserted_count} unique URLs inserted, {duplicate_count} duplicates skipped, {filtered_count} URLs filtered out")
        return inserted_count, duplicate_count, filtered_count
    except Exception as e:
        await conn.rollback()
        logger.error(f"Error processing file {file_path}: {e}")
        return 0, 0, 0

async def process_media_domains_file(conn, file_path):
    """Process a single media domains JSON file"""
    domain = Path(file_path).stem  # Extract domain name from filename
    json_data = await load_json_file(file_path)
    
    if not json_data:
        return 0, 0, 0  # Return zeros if file couldn't be loaded
    
    inserted_count = 0
    duplicate_count = 0
    filtered_count = 0
    
    try:
        async with conn.cursor() as cursor:
            for data in json_data:
                domain = data.get('domain')
                timestamp_str = data.get('timestamp')
                
                try:
                    timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    timestamp = datetime.now()
                    logger.warning(f"Could not parse timestamp '{timestamp_str}', using current time")
                
                url_paths = data.get('URL_paths', [])
                for url_data in url_paths:
                    index = url_data.get('index')
                    url_paths_list = url_data.get('url_paths', [])
                    
                    for url_path in url_paths_list:
                        # Skip URLs that match filtering criteria
                        if should_skip_url(url_path):
                            filtered_count += 1
                            continue
                            
                        try:
                            await cursor.execute("""
                                INSERT INTO url_registry (domain, accessTimestamp, index, urlPath, status) 
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT (urlPath) DO NOTHING
                            """, (domain, timestamp, index, url_path, 'pending'))
                            
                            if cursor.rowcount > 0:
                                inserted_count += 1
                            else:
                                duplicate_count += 1
                        except Exception as e:
                            logger.error(f"Error inserting record: {e}")
        
        await conn.commit()
        logger.info(f"Domain {domain} from CommonCrawl: {inserted_count} unique URLs inserted, {duplicate_count} duplicates skipped, {filtered_count} URLs filtered out")
        return inserted_count, duplicate_count, filtered_count
    except Exception as e:
        await conn.rollback()
        logger.error(f"Error processing file {file_path}: {e}")
        return 0, 0, 0

async def process_folder(conn, folder_path, process_func):
    """Process all JSON files in the specified folder"""
    if not os.path.exists(folder_path):
        logger.error(f"Folder not found: {folder_path}")
        return
    
    total_inserted = 0
    total_duplicates = 0
    total_filtered = 0
    file_count = 0
    
    for filename in os.listdir(folder_path):
        if filename.endswith('.json'):
            file_path = os.path.join(folder_path, filename)
            logger.info(f"Processing file: {filename}")
            
            inserted, duplicates, filtered = await process_func(conn, file_path)
            total_inserted += inserted
            total_duplicates += duplicates
            total_filtered += filtered
            file_count += 1
    
    logger.info(f"Processed {file_count} files: {total_inserted} total unique URLs inserted, {total_duplicates} total duplicates skipped, {total_filtered} total URLs filtered out")

async def main():
    logger.info("Script execution started")
    
    # Connect to database
    conn = await get_connection()
    if not conn:
        logger.error("Failed to connect to database. Exiting.")
        return
    
    try:
        # Create table structure
        await create_table(conn)
        
        # Process media domains files (CommonCrawl)
        media_domains_folder = r"C:\Users\Dell\OneDrive\Desktop\WebScraping\assests\newmediadomains"
        logger.info(f"Processing CommonCrawl data from folder: {media_domains_folder}")
        await process_folder(conn, media_domains_folder, process_media_domains_file)
        
        # Process general crawler files
        general_crawler_folder = r"C:\Users\Dell\OneDrive\Desktop\WebScraping\assests\generalcralwerurl"
        logger.info(f"Processing General Crawler data from folder: {general_crawler_folder}")
        await process_folder(conn, general_crawler_folder, process_general_crawler_file)
        
        logger.info("Data processing complete")
    except Exception as e:
        logger.error(f"Unexpected error in main execution: {e}")
    finally:
        if conn:
            await return_connection(conn)
            logger.info("Database connection closed")

if __name__ == "__main__":
    asyncio.run(main())