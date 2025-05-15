import os
import logging
import asyncio
from dotenv import load_dotenv
from psycopg_pool import AsyncConnectionPool

# Set correct event loop policy for Windows
if os.name == 'nt':  # Check if running on Windows
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
# Configure logging
logging.basicConfig(
    filename='database.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Check required environment variables
required_vars = ["DB_NAME", "DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT"]
missing_vars = [var for var in required_vars if not os.getenv(var)]
if missing_vars:
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

# Database configuration
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}

# Initialize connection pool variable
connection_pool = None

async def initialize_pool():
    """Initialize the async connection pool."""
    global connection_pool
    try:
        # Create the pool but don't connect yet
        connection_pool = AsyncConnectionPool(
            conninfo=" ".join(f"{k}={v}" for k, v in DB_CONFIG.items()),
            min_size=3,
            max_size=10,
            open=False  # Don't open connections in constructor
        )
        
        # Now explicitly open the pool using the recommended approach
        await connection_pool.open()
        logger.info("Async database connection pool initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize async database connection pool: {e}")
        raise

async def get_connection():
    """Get a connection from the async pool."""
    if connection_pool is None:
        await initialize_pool()
    # The correct method is getconn() not acquire()
    return await connection_pool.getconn()

async def return_connection(conn):
    """Return the connection to the async pool."""
    # The correct method is putconn() not release()
    await connection_pool.putconn(conn)

async def close_all_connections():
    """Close all connections in the async pool (call this at app exit)."""
    if connection_pool:
        await connection_pool.close()
        logger.info("All database connections closed")



