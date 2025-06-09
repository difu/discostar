"""
Test database utilities for DiscoStar.

Provides isolated test database management for unit testing.
"""

import tempfile
from pathlib import Path
from typing import Optional, Generator
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from .models import Base
from .database import DatabaseManager


class DiscogsTestDatabaseManager(DatabaseManager):
    """Test-specific database manager with isolation."""
    
    def __init__(self, database_url: Optional[str] = None):
        """Initialize test database manager.
        
        Args:
            database_url: Test database URL. If None, uses in-memory SQLite.
        """
        if database_url is None:
            # Use in-memory SQLite for fast tests
            database_url = "sqlite:///:memory:"
        
        super().__init__(database_url)
        
        # Disable echo for tests by default
        self.engine.echo = False
    
    def setup_test_database(self) -> None:
        """Set up test database with tables."""
        self.create_tables()
    
    def teardown_test_database(self) -> None:
        """Clean up test database."""
        self.drop_tables()
    
    @contextmanager
    def test_session(self) -> Generator[Session, None, None]:
        """Context manager for test database sessions with automatic rollback.
        
        Yields:
            Database session that will be rolled back after use
        """
        session = self.get_session()
        try:
            yield session
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


@contextmanager
def temporary_database() -> Generator[DiscogsTestDatabaseManager, None, None]:
    """Context manager for temporary test database.
    
    Creates a temporary SQLite file database that is automatically cleaned up.
    
    Yields:
        DiscogsTestDatabaseManager instance with temporary database
    """
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_file:
        tmp_path = Path(tmp_file.name)
    
    try:
        database_url = f"sqlite:///{tmp_path}"
        db_manager = DiscogsTestDatabaseManager(database_url)
        db_manager.setup_test_database()
        yield db_manager
    finally:
        # Clean up temporary file
        if tmp_path.exists():
            tmp_path.unlink()


@contextmanager
def memory_database() -> Generator[DiscogsTestDatabaseManager, None, None]:
    """Context manager for in-memory test database.
    
    Yields:
        DiscogsTestDatabaseManager instance with in-memory database
    """
    db_manager = DiscogsTestDatabaseManager()
    db_manager.setup_test_database()
    try:
        yield db_manager
    finally:
        db_manager.teardown_test_database()


def create_test_database_manager(use_memory: bool = True) -> DiscogsTestDatabaseManager:
    """Create a test database manager.
    
    Args:
        use_memory: If True, use in-memory SQLite. If False, use temporary file.
        
    Returns:
        DiscogsTestDatabaseManager instance
    """
    if use_memory:
        return DiscogsTestDatabaseManager()
    else:
        tmp_file = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        tmp_file.close()
        database_url = f"sqlite:///{tmp_file.name}"
        return DiscogsTestDatabaseManager(database_url)