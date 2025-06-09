"""
Pytest configuration for DiscoStar tests.

Global test configuration, fixtures, and setup for the entire test suite.
"""

import pytest
import os
import tempfile
from pathlib import Path

# Import all fixtures to make them available to tests
from tests.fixtures.database_fixtures import *
from tests.fixtures.api_fixtures import *


def pytest_configure(config):
    """Configure pytest with custom markers and settings."""
    config.addinivalue_line(
        "markers", "database: marks tests as requiring database access"
    )
    config.addinivalue_line(
        "markers", "api: marks tests as API-related"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "slow: marks tests as slow running"
    )


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Set up test environment (session scope)."""
    # Set test environment variables
    os.environ["TESTING"] = "1"
    os.environ["LOG_LEVEL"] = "WARNING"  # Reduce log noise in tests
    
    # Ensure test data directory exists
    test_data_dir = Path("tests/data")
    test_data_dir.mkdir(parents=True, exist_ok=True)
    
    yield
    
    # Cleanup after all tests
    pass


@pytest.fixture(scope="function")
def temp_dir():
    """Provide a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def mock_config():
    """Provide mock configuration for tests."""
    return {
        "database": {
            "sqlite": {
                "path": ":memory:"
            }
        },
        "discogs": {
            "api": {
                "base_url": "https://api.discogs.com",
                "rate_limit": {
                    "requests_per_minute": 60,
                    "min_interval": 1.0
                }
            }
        },
        "logging": {
            "level": "WARNING",
            "file": None
        }
    }


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Set up mock environment variables for tests."""
    monkeypatch.setenv("DISCOGS_API_TOKEN", "test_token_123")
    monkeypatch.setenv("DISCOGS_USERNAME", "test_user")
    monkeypatch.setenv("TESTING", "1")


# Pytest collection hooks for better test organization
def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers based on file location."""
    for item in items:
        # Add markers based on test file location
        if "test_database" in str(item.fspath):
            item.add_marker(pytest.mark.database)
        if "test_api" in str(item.fspath) or "test_discogs" in str(item.fspath):
            item.add_marker(pytest.mark.api)
        if "test_integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
        
        # Mark slow tests
        if any(keyword in item.name.lower() for keyword in ["large", "full", "integration"]):
            item.add_marker(pytest.mark.slow)