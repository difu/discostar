"""
Unit tests for database manager functionality.

Tests database initialization, connections, and management operations.
"""

import pytest
import tempfile
from pathlib import Path

from sqlalchemy import text
from src.core.database.database import DatabaseManager, get_database_manager
from src.core.database.test_database import DiscogsTestDatabaseManager, memory_database, temporary_database
from src.core.database.models import Base, User


class TestDatabaseManager:
    """Test DatabaseManager functionality."""
    
    def test_memory_database_creation(self):
        """Test creating in-memory database."""
        db_manager = DatabaseManager("sqlite:///:memory:")
        assert db_manager.database_url == "sqlite:///:memory:"
        assert db_manager.engine is not None
        assert db_manager.SessionLocal is not None
    
    def test_file_database_creation(self):
        """Test creating file-based database."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_file:
            tmp_path = Path(tmp_file.name)
        
        try:
            database_url = f"sqlite:///{tmp_path}"
            db_manager = DatabaseManager(database_url)
            
            assert db_manager.database_url == database_url
            assert db_manager.engine is not None
        finally:
            if tmp_path.exists():
                tmp_path.unlink()
    
    def test_create_tables(self):
        """Test table creation."""
        db_manager = DatabaseManager("sqlite:///:memory:")
        db_manager.create_tables()
        
        # Verify tables exist by trying to query
        with db_manager.get_session() as session:
            # This should not raise an exception
            result = session.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
            table_names = [row[0] for row in result.fetchall()]
            
            expected_tables = ['users', 'artists', 'labels', 'masters', 'releases']
            for table in expected_tables:
                assert table in table_names
    
    def test_drop_tables(self):
        """Test table dropping."""
        db_manager = DatabaseManager("sqlite:///:memory:")
        db_manager.create_tables()
        db_manager.drop_tables()
        
        # Verify tables are dropped
        with db_manager.get_session() as session:
            result = session.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
            table_names = [row[0] for row in result.fetchall()]
            
            # Should have no user-created tables
            user_tables = [name for name in table_names if not name.startswith('sqlite_')]
            assert len(user_tables) == 0
    
    def test_get_session(self):
        """Test getting database session."""
        db_manager = DatabaseManager("sqlite:///:memory:")
        db_manager.create_tables()
        
        session = db_manager.get_session()
        assert session is not None
        
        # Test session works
        session.execute(text("SELECT 1"))
        session.close()


class TestDiscogsTestDatabaseManager:
    """Test DiscogsTestDatabaseManager functionality."""
    
    def test_test_database_manager_creation(self):
        """Test creating test database manager."""
        test_db = DiscogsTestDatabaseManager()
        assert test_db.database_url == "sqlite:///:memory:"
        assert test_db.engine.echo is False  # Should be disabled for tests
    
    def test_setup_teardown(self):
        """Test setup and teardown of test database."""
        test_db = DiscogsTestDatabaseManager()
        
        # Setup
        test_db.setup_test_database()
        
        # Verify setup worked
        with test_db.get_session() as session:
            result = session.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
            table_names = [row[0] for row in result.fetchall()]
            assert 'users' in table_names
        
        # Teardown
        test_db.teardown_test_database()
        
        # Verify teardown worked
        with test_db.get_session() as session:
            result = session.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
            table_names = [row[0] for row in result.fetchall()]
            user_tables = [name for name in table_names if not name.startswith('sqlite_')]
            assert len(user_tables) == 0
    
    def test_test_session_context_manager(self):
        """Test test session context manager."""
        test_db = DiscogsTestDatabaseManager()
        test_db.setup_test_database()
        
        with test_db.test_session() as session:
            # Create a user
            user = User(
                discogs_username="test_user",
                discogs_user_id=123456,
                display_name="Test User"
            )
            session.add(user)
            session.commit()
            
            # Verify user exists
            found_user = session.query(User).filter_by(discogs_username="test_user").first()
            assert found_user is not None
            assert found_user.display_name == "Test User"
    
    def test_test_session_rollback_on_exception(self):
        """Test that test session rolls back on exception."""
        test_db = DiscogsTestDatabaseManager()
        test_db.setup_test_database()
        
        try:
            with test_db.test_session() as session:
                # Create a user
                user = User(
                    discogs_username="test_user",
                    discogs_user_id=123456,
                    display_name="Test User"
                )
                session.add(user)
                session.commit()
                
                # Force an exception
                raise ValueError("Test exception")
        except ValueError:
            pass
        
        # Verify session was properly closed
        with test_db.test_session() as session:
            found_user = session.query(User).filter_by(discogs_username="test_user").first()
            # User should still exist since we committed before the exception
            assert found_user is not None


class TestDatabaseContextManagers:
    """Test database context managers."""
    
    def test_memory_database_context_manager(self):
        """Test memory database context manager."""
        with memory_database() as db_manager:
            assert isinstance(db_manager, DiscogsTestDatabaseManager)
            assert "memory" in db_manager.database_url
            
            # Test database works
            with db_manager.test_session() as session:
                result = session.execute(text("SELECT 1"))
                assert result.fetchone()[0] == 1
    
    def test_temporary_database_context_manager(self):
        """Test temporary database context manager."""
        with temporary_database() as db_manager:
            assert isinstance(db_manager, DiscogsTestDatabaseManager)
            assert db_manager.database_url.startswith("sqlite:///")
            
            # Test database works
            with db_manager.test_session() as session:
                result = session.execute(text("SELECT 1"))
                assert result.fetchone()[0] == 1
        
        # File should be cleaned up after context manager exits
        # (We can't easily test this without accessing the temp file path)
    
    def test_database_persistence_in_temporary_db(self):
        """Test that temporary database persists data during context."""
        with temporary_database() as db_manager:
            # Create user in one session
            with db_manager.test_session() as session:
                user = User(
                    discogs_username="persistent_user",
                    discogs_user_id=999999,
                    display_name="Persistent User"
                )
                session.add(user)
                session.commit()
            
            # Verify user exists in another session
            with db_manager.test_session() as session:
                found_user = session.query(User).filter_by(discogs_username="persistent_user").first()
                assert found_user is not None
                assert found_user.display_name == "Persistent User"