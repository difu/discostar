"""
Database initialization and connection management for DiscoStar.
"""

import logging
from pathlib import Path
from typing import Optional

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session

from .models import Base
from ..utils.config import get_data_directory


logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages database connections and initialization."""
    
    def __init__(self, database_url: Optional[str] = None):
        """Initialize database manager.
        
        Args:
            database_url: SQLite database URL. If None, uses default location.
        """
        if database_url is None:
            db_path = get_data_directory() / "discostar.db"
            database_url = f"sqlite:///{db_path}"
        
        self.database_url = database_url
        self.engine = create_engine(
            database_url,
            echo=False,  # Set to True for SQL debugging
            pool_pre_ping=True
        )
        
        # Enable SQLite foreign key constraints
        if database_url.startswith('sqlite'):
            @event.listens_for(Engine, "connect")
            def set_sqlite_pragma(dbapi_connection, connection_record):
                cursor = dbapi_connection.cursor()
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.close()
        
        self.SessionLocal = sessionmaker(
            autocommit=False, 
            autoflush=False, 
            bind=self.engine
        )
    
    def create_tables(self) -> None:
        """Create all database tables."""
        logger.info("Creating database tables...")
        Base.metadata.create_all(bind=self.engine)
        logger.info("Database tables created successfully")
    
    def drop_tables(self) -> None:
        """Drop all database tables. Use with caution!"""
        logger.warning("Dropping all database tables...")
        Base.metadata.drop_all(bind=self.engine)
        logger.info("Database tables dropped")
    
    def get_session(self) -> Session:
        """Get a new database session."""
        return self.SessionLocal()
    
    def init_database(self) -> None:
        """Initialize the database with tables."""
        # Ensure data directory exists
        db_path = Path(self.database_url.replace('sqlite:///', ''))
        db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Create tables
        self.create_tables()
        
        logger.info(f"Database initialized at: {db_path}")
    
    def execute_sql_file(self, sql_file_path: Path) -> None:
        """Execute SQL commands from a file.
        
        Args:
            sql_file_path: Path to SQL file to execute
        """
        if not sql_file_path.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_file_path}")
        
        with open(sql_file_path, 'r') as f:
            sql_content = f.read()
        
        with self.engine.connect() as connection:
            # Split by semicolon and execute each statement
            statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
            
            for statement in statements:
                if statement:
                    logger.debug(f"Executing SQL: {statement[:100]}...")
                    connection.execute(statement)
                    connection.commit()
        
        logger.info(f"Executed SQL file: {sql_file_path}")


# Global database manager instance
_db_manager: Optional[DatabaseManager] = None


def get_database_manager(database_url: Optional[str] = None) -> DatabaseManager:
    """Get the global database manager instance.
    
    Args:
        database_url: Database URL. Only used on first call.
        
    Returns:
        DatabaseManager instance
    """
    global _db_manager
    
    if _db_manager is None:
        _db_manager = DatabaseManager(database_url)
    
    return _db_manager


def get_db_session() -> Session:
    """Get a new database session.
    
    Returns:
        SQLAlchemy Session instance
    """
    return get_database_manager().get_session()


def init_database(database_url: Optional[str] = None) -> None:
    """Initialize the database.
    
    Args:
        database_url: Database URL. If None, uses default location.
    """
    db_manager = get_database_manager(database_url)
    db_manager.init_database()


def reset_database(database_url: Optional[str] = None) -> None:
    """Reset the database by dropping and recreating all tables.
    
    Args:
        database_url: Database URL. If None, uses default location.
    """
    db_manager = get_database_manager(database_url)
    db_manager.drop_tables()
    db_manager.create_tables()
    logger.info("Database reset completed")


def get_database_url(config: dict) -> str:
    """Get database URL from configuration.
    
    Args:
        config: Application configuration dictionary
        
    Returns:
        Database URL string
    """
    db_config = config.get('database', {})
    
    # Check for SQLite configuration
    sqlite_config = db_config.get('sqlite', {})
    if sqlite_config:
        db_path = sqlite_config.get('path', 'data/discostar.db')
        # Ensure absolute path
        if not Path(db_path).is_absolute():
            db_path = Path.cwd() / db_path
        return f"sqlite:///{db_path}"
    
    # Default to SQLite if no configuration found
    db_path = get_data_directory() / "discostar.db"
    return f"sqlite:///{db_path}"