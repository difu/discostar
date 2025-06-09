# DiscoStar Test Suite

This directory contains the comprehensive test suite for DiscoStar, including unit tests, integration tests, and test data for faster development.

## Test Data Strategy

### Generated Test Database
- **Location**: `tests/data/test_database.db` (4.2MB, git-friendly)
- **Strategy**: Collection-driven sampling with 200 random collection items + dependencies
- **Contents**:
  - **2,355 artists** (dependencies + extras for variety)
  - **250 releases** (200 from collection + 50 extras)
  - **216 masters** (dependencies + extras)
  - **200 user collection items** (complete functional collection)
  - **185 labels** (dependencies + extras)
  - **All relationship data** (release_artists, release_labels, tracks)

### Test Data Extraction
```bash
# Regenerate test data (if needed)
python -c "from src.core.database.test_data_extractor import extract_test_data; extract_test_data(collection_sample_size=200)"
```

## Test Structure

### 1. Database Tests (`tests/test_database/`)
- **`test_models.py`**: Model creation, relationships, JSON fields
- **`test_database_manager.py`**: Database connections, setup/teardown
- Test coverage: Models, relationships, database operations

### 2. API Tests (`tests/test_discogs/`)
- **`test_api_client.py`**: Discogs API client with mocked responses
- **`test_collection_sync.py`**: Collection synchronization with database integration
- Test coverage: API calls, rate limiting, error handling, data sync

### 3. Test Fixtures (`tests/fixtures/`)
- **`database_fixtures.py`**: Database test fixtures and session management
- **`model_factories.py`**: Factory functions for creating test data
- **`api_fixtures.py`**: Mock API responses and test data

## Running Tests

### Basic Test Execution
```bash
# Run all tests
pytest

# Run specific test categories
pytest -m database          # Database tests only
pytest -m api              # API tests only
pytest -m integration      # Integration tests only

# Run tests with coverage
pytest --cov=src --cov-report=html

# Run specific test files
pytest tests/test_database/test_models.py
pytest tests/test_discogs/test_api_client.py
```

### Test Filtering
```bash
# Skip slow tests
pytest -m "not slow"

# Run only database tests
pytest tests/test_database/

# Verbose output
pytest -v

# Stop on first failure
pytest -x
```

## Test Fixtures Usage

### Database Testing
```python
def test_my_function(test_session):
    """Test with real sample data."""
    # test_session provides access to test database with 200 collection items
    users = test_session.query(User).all()
    assert len(users) == 1

def test_clean_database(clean_db_session):
    """Test with clean in-memory database."""
    # clean_db_session provides empty database for isolated tests
    user = create_test_user()
    clean_db_session.add(user)
    clean_db_session.commit()
```

### Creating Test Data
```python
from tests.fixtures.model_factories import create_test_user, create_test_release

def test_with_factory_data(clean_db_session):
    """Test using factory-generated data."""
    user = create_test_user(discogs_username="test_user")
    release = create_test_release(title="Test Album")
    
    clean_db_session.add_all([user, release])
    clean_db_session.commit()
```

### API Testing
```python
from tests.fixtures.api_fixtures import SAMPLE_COLLECTION_RESPONSE

@pytest.mark.asyncio
async def test_api_call(mock_api_client):
    """Test API with mocked responses."""
    mock_api_client.get_collection.return_value = SAMPLE_COLLECTION_RESPONSE
    result = await api_client.get_collection("test_user")
    assert len(result['releases']) == 2
```

## Suggested Unit Tests for Development

### High Priority Tests (Implement First)
1. **Model Validation Tests**
   - JSON field serialization/deserialization
   - Relationship integrity
   - Model creation with required fields

2. **Database Operation Tests**
   - Session management
   - Transaction rollbacks
   - Connection pooling

3. **API Client Tests**
   - Rate limiting behavior
   - Error response handling
   - Authentication headers

4. **Collection Sync Tests**
   - Data transformation accuracy
   - Duplicate handling
   - Progress tracking

### Medium Priority Tests
1. **Data Ingestion Tests**
   - XML parsing accuracy
   - Batch processing
   - Error recovery

2. **Configuration Tests**
   - Environment variable loading
   - YAML configuration parsing
   - Default value handling

3. **CLI Command Tests**
   - Argument parsing
   - Command execution
   - Error messaging

### Integration Tests
1. **End-to-End Collection Sync**
   - API → Database workflow
   - Real data transformation
   - Performance benchmarks

2. **Data Pipeline Tests**
   - XML dump → Database ingestion
   - Collection sync → Analytics
   - Multi-user scenarios

## Performance Testing

### Database Performance
```bash
# Test with larger dataset
python -c "from src.core.database.test_data_extractor import extract_test_data; extract_test_data(collection_sample_size=1000)"

# Benchmark database operations
pytest tests/test_database/ --benchmark-only
```

### Memory Usage Testing
```bash
# Run tests with memory profiling
pytest --memory-profiler tests/
```

## Test Data Refresh

The test database should be refreshed when:
- Production schema changes significantly
- Collection data structure changes
- Test coverage needs different data patterns

```bash
# Force regeneration of test data
rm tests/data/test_database.db
python -c "from src.core.database.test_data_extractor import extract_test_data; extract_test_data()"
```

## Test Environment

### Environment Variables for Testing
```bash
export TESTING=1
export LOG_LEVEL=WARNING
export DISCOGS_API_TOKEN=test_token_123
export DISCOGS_USERNAME=test_user
```

### Mock Configuration
Tests use mock configuration to avoid dependencies on external services:
- In-memory SQLite databases
- Mocked HTTP responses
- Disabled external API calls

## Debugging Tests

### Common Issues
1. **Database Connection Errors**: Check test database exists in `tests/data/`
2. **Import Errors**: Ensure `src/` is in Python path
3. **Fixture Scope Issues**: Use appropriate fixture scope (function/session)

### Debug Mode
```bash
# Run with debug output
pytest -s -vv --tb=long

# Drop into debugger on failure
pytest --pdb

# Show local variables in tracebacks
pytest --tb=short --showlocals
```

This comprehensive test suite enables fast, reliable development with realistic data while maintaining git-friendly file sizes.