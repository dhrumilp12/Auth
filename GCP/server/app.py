from flask import Flask
from flask_jwt_extended import JWTManager
from flask_mail import Mail
from utils.extensions import oauth, mail
from routes import register_blueprints
from dotenv import load_dotenv
import logging
import os
from services.postgres_rds import PostgresRDSClient

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_app():
    """Create and configure the Flask application."""
    app = Flask(__name__)

    # Load configuration from environment variables
    app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'default_secret_key')
    app.config['JWT_SECRET_KEY'] = os.getenv('JWT_SECRET_KEY', 'default_jwt_secret_key')
    app.config['MAIL_SERVER'] = os.getenv('MAIL_SERVER', 'smtp.gmail.com')
    app.config['MAIL_PORT'] = int(os.getenv('MAIL_PORT', 587))
    app.config['MAIL_USE_TLS'] = os.getenv('MAIL_USE_TLS', 'true').lower() == 'true'
    app.config['MAIL_USERNAME'] = os.getenv('MAIL_USERNAME')
    app.config['MAIL_PASSWORD'] = os.getenv('MAIL_PASSWORD')
    app.config['MAIL_DEFAULT_SENDER'] = os.getenv('MAIL_DEFAULT_SENDER')
    app.config['SECURITY_PASSWORD_SALT'] = os.getenv('SECRET_KEY', 'default_password_salt')

    # Initialize extensions
    JWTManager(app)
    mail.init_app(app)
    oauth.init_app(app)

    # Initialize database connection and schema
    with app.app_context():
        try:
            # Use a separate function to initialize the database
            # to avoid transaction conflicts with other app startup operations
            _initialize_database()
            logger.info("Database connection and schema initialization successful")
        except Exception as e:
            logger.error(f"Database initialization error: {str(e)}")

    # Register blueprints
    register_blueprints(app)

    return app

def _initialize_database():
    """Initialize the database connection and schema."""
    # Explicitly close any existing connection first
    if PostgresRDSClient._connection is not None:
        try:
            PostgresRDSClient._connection.close()
        except:
            pass
        PostgresRDSClient._connection = None
    
    # Get a fresh connection - this will create tables if needed
    PostgresRDSClient.get_connection()

if __name__ == '__main__':
    app = create_app()
    host = os.getenv('FLASK_RUN_HOST', '0.0.0.0')
    port = int(os.getenv('FLASK_RUN_PORT', 5000))
    debug = os.getenv('FLASK_DEBUG', 'false').lower() == 'true'

    logger.info(f"Starting Flask app on {host}:{port} (debug={debug})")
    app.run(host=host, port=port, debug=debug)
