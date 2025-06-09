"""
Database test fixtures for DiscoStar.

Provides pytest fixtures for database testing with isolated test data.
"""

import pytest
from pathlib import Path
from typing import Generator

from src.core.database.test_database import DiscogsTestDatabaseManager, memory_database, temporary_database
from src.core.database.test_data_extractor import extract_test_data
from src.core.database.models import (
    User, UserCollection, Artist, Label, Master, Release,
    ReleaseArtist, ReleaseLabel, Track, CollectionFolder
)


@pytest.fixture
def memory_db() -> Generator[DiscogsTestDatabaseManager, None, None]:
    """Provide in-memory test database."""
    with memory_database() as db:
        yield db


@pytest.fixture
def temp_db() -> Generator[DiscogsTestDatabaseManager, None, None]:
    """Provide temporary file test database."""
    with temporary_database() as db:
        yield db


@pytest.fixture(scope="session")
def test_data_db() -> Generator[str, None, None]:
    """Provide test database with sample data (session scope for performance).
    
    This fixture creates a test database with sample data extracted from
    the production database. It's session-scoped to avoid recreating the
    test data for every test.
    """
    test_db_path = "tests/data/test_database.db"
    
    # Only extract if test database doesn't exist
    if not Path(test_db_path).exists():
        extract_test_data(target_db_path=test_db_path, collection_sample_size=200)
    
    yield test_db_path


@pytest.fixture
def test_db_manager(test_data_db: str) -> Generator[DiscogsTestDatabaseManager, None, None]:
    """Provide test database manager with sample data."""
    db_manager = DiscogsTestDatabaseManager(f"sqlite:///{test_data_db}")
    yield db_manager


@pytest.fixture
def test_session(test_db_manager: DiscogsTestDatabaseManager):
    """Provide test database session with sample data."""
    with test_db_manager.test_session() as session:
        yield session


@pytest.fixture
def clean_db_session(memory_db: DiscogsTestDatabaseManager):
    """Provide clean test database session (no sample data)."""
    with memory_db.test_session() as session:
        yield session