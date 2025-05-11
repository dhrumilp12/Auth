import os
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import logging

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PostgresRDSClient:
    _connection = None

    @staticmethod
    def get_connection():
        if PostgresRDSClient._connection is None:
            try:
                # Connect to Google Cloud SQL PostgreSQL
                connection = psycopg2.connect(
                    dbname=os.getenv("CLOUDSQL_DB_NAME"),
                    user=os.getenv("CLOUDSQL_USERNAME"),
                    password=os.getenv("CLOUDSQL_PASSWORD"),
                    host=os.getenv("CLOUDSQL_HOST"),
                    port=os.getenv("CLOUDSQL_PORT")
                )
                # Set autocommit immediately after connection creation
                connection.autocommit = True
                PostgresRDSClient._connection = connection
                
                logger.info("Connected to Google Cloud SQL PostgreSQL")
                
                # Check if required tables exist and create them if they don't
                if not PostgresRDSClient._check_table_exists("users"):
                    logger.info("Required tables don't exist. Initializing schema...")
                    PostgresRDSClient._initialize_schema()
                    
            except psycopg2.OperationalError as e:
                # Check if the error is because the database doesn't exist
                if "does not exist" in str(e):
                    logger.info(f"Database {os.getenv('CLOUDSQL_DB_NAME')} does not exist. Attempting to create it.")
                    PostgresRDSClient._create_database()
                    # Try to connect again after creating the database
                    connection = psycopg2.connect(
                        dbname=os.getenv("CLOUDSQL_DB_NAME"),
                        user=os.getenv("CLOUDSQL_USERNAME"),
                        password=os.getenv("CLOUDSQL_PASSWORD"),
                        host=os.getenv("CLOUDSQL_HOST"),
                        port=os.getenv("CLOUDSQL_PORT")
                    )
                    # Set autocommit immediately
                    connection.autocommit = True
                    PostgresRDSClient._connection = connection
                    logger.info(f"Connected to newly created database {os.getenv('CLOUDSQL_DB_NAME')}")
                    
                    # Initialize schema in the new database
                    PostgresRDSClient._initialize_schema()
                else:
                    logger.error(f"Error connecting to PostgreSQL: {str(e)}")
                    raise
            except Exception as e:
                logger.error(f"Error connecting to PostgreSQL: {str(e)}")
                raise
        return PostgresRDSClient._connection

    @staticmethod
    def _check_table_exists(table_name):
        """Check if a specific table exists in the current database."""
        try:
            # Create a separate connection just for checking table existence
            # This avoids transaction conflicts with the main connection
            conn = psycopg2.connect(
                dbname=os.getenv("CLOUDSQL_DB_NAME"),
                user=os.getenv("CLOUDSQL_USERNAME"),
                password=os.getenv("CLOUDSQL_PASSWORD"),
                host=os.getenv("CLOUDSQL_HOST"),
                port=os.getenv("CLOUDSQL_PORT")
            )
            conn.autocommit = True
                
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = %s
                    )
                """, (table_name,))
                
                exists = cursor.fetchone()[0]
            
            # Close the temporary connection
            conn.close()
            return exists
        except Exception as e:
            logger.error(f"Error checking if table exists: {str(e)}")
            return False

    @staticmethod
    def _create_database():
        """Creates the database if it doesn't exist."""
        conn = None
        try:
            # Connect to default 'postgres' database to create our database
            conn = psycopg2.connect(
                dbname="postgres",  # Connect to default postgres database
                user=os.getenv("CLOUDSQL_USERNAME"),
                password=os.getenv("CLOUDSQL_PASSWORD"),
                host=os.getenv("CLOUDSQL_HOST"),
                port=os.getenv("CLOUDSQL_PORT")
            )
            # Must set autocommit for CREATE DATABASE
            conn.autocommit = True
            
            with conn.cursor() as cursor:
                db_name = os.getenv("CLOUDSQL_DB_NAME")
                # Use sql.Identifier to safely quote the database name
                cursor.execute(
                    sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name))
                )
                logger.info(f"Database {db_name} created successfully")
        except Exception as e:
            logger.error(f"Error creating database: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()

    @staticmethod
    def _initialize_schema():
        """Initialize database schema with required tables."""
        conn = None
        try:
            # Create a fresh connection for schema initialization
            # to avoid transaction conflicts
            conn = psycopg2.connect(
                dbname=os.getenv("CLOUDSQL_DB_NAME"),
                user=os.getenv("CLOUDSQL_USERNAME"),
                password=os.getenv("CLOUDSQL_PASSWORD"),
                host=os.getenv("CLOUDSQL_HOST"),
                port=os.getenv("CLOUDSQL_PORT")
            )
            conn.autocommit = True
            
            with conn.cursor() as cursor:
                # Create users table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id SERIAL PRIMARY KEY,
                        username VARCHAR(20) UNIQUE NOT NULL,
                        email VARCHAR(255) UNIQUE NOT NULL,
                        password VARCHAR(255),
                        name VARCHAR(255),
                        age INTEGER,
                        gender VARCHAR(10),
                        preferred_language VARCHAR(2),
                        profile_picture VARCHAR(255),
                        google_id VARCHAR(255)
                    )
                """)
                logger.info("Database schema initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing schema: {str(e)}")
            raise
        finally:
            # Always close the connection
            if conn and conn != PostgresRDSClient._connection:
                conn.close()

    @staticmethod
    def check_database_exists():
        """Check if the database exists."""
        conn = None
        try:
            conn = psycopg2.connect(
                dbname="postgres",
                user=os.getenv("CLOUDSQL_USERNAME"),
                password=os.getenv("CLOUDSQL_PASSWORD"),
                host=os.getenv("CLOUDSQL_HOST"),
                port=os.getenv("CLOUDSQL_PORT")
            )
            conn.autocommit = True
            
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", 
                             (os.getenv("CLOUDSQL_DB_NAME"),))
                exists = cursor.fetchone() is not None
            
            return exists
        except Exception as e:
            logger.error(f"Error checking if database exists: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()

    @staticmethod
    def execute_query(query, params=None, fetch_one=False, fetch_all=False):
        conn = PostgresRDSClient.get_connection()
        with conn.cursor() as cursor:
            try:
                cursor.execute(query, params)
                if fetch_one:
                    result = cursor.fetchone()
                    if result:
                        # Return both the result and column descriptions
                        columns = [desc[0] for desc in cursor.description]
                        return {"data": result, "columns": columns}
                    return None
                if fetch_all:
                    results = cursor.fetchall()
                    if results:
                        # Return both results and column descriptions
                        columns = [desc[0] for desc in cursor.description]
                        return {"data": results, "columns": columns}
                    return None
                conn.commit()
            except Exception as e:
                logger.error(f"Error executing query: {str(e)}")
                conn.rollback()
                raise
