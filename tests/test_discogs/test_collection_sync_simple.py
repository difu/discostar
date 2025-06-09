"""
Basic tests for collection synchronization.

Simple tests that work with the actual CollectionSync API.
"""

import pytest
from unittest.mock import patch, AsyncMock

from src.core.discogs.collection_sync import CollectionSync


class TestCollectionSyncBasic:
    """Basic tests for CollectionSync functionality."""
    
    @pytest.fixture
    def mock_config(self):
        """Create mock configuration."""
        return {
            "discogs": {
                "api": {
                    "username": "test_user",
                    "token": "test_token_123",
                    "base_url": "https://api.discogs.com",
                    "rate_limit": {
                        "requests_per_minute": 60,
                        "min_interval": 1.0
                    }
                }
            }
        }
    
    def test_collection_sync_initialization(self, mock_config):
        """Test CollectionSync initialization."""
        sync_client = CollectionSync(config=mock_config)
        assert sync_client.username == "test_user"
        assert sync_client.config == mock_config
    
    @pytest.mark.asyncio
    async def test_sync_collection_basic(self, mock_config):
        """Test basic collection sync workflow."""
        sync_client = CollectionSync(config=mock_config)
        
        # Mock the DiscogsAPIClient
        with patch('src.core.discogs.collection_sync.DiscogsAPIClient') as mock_api_class:
            mock_api_client = AsyncMock()
            mock_api_client.get_all_collection_items.return_value = []
            mock_api_class.return_value.__aenter__.return_value = mock_api_client
            
            # Mock database session
            with patch('src.core.discogs.collection_sync.get_db_session') as mock_get_session:
                mock_session = AsyncMock()
                mock_get_session.return_value.__enter__.return_value = mock_session
                
                # Run sync
                result = await sync_client.sync_collection()
                
                # Verify result structure
                assert isinstance(result, dict)
                assert 'collection_items_added' in result
                assert 'collection_items_updated' in result
                assert 'releases_fetched' in result
    
    @pytest.mark.asyncio 
    async def test_sync_collection_with_items(self, mock_config):
        """Test collection sync with mock collection items."""
        sync_client = CollectionSync(config=mock_config)
        
        # Mock collection items
        mock_items = [
            {
                "id": 123456,
                "basic_information": {
                    "id": 123456,
                    "title": "Test Album",
                    "artists": [{"name": "Test Artist"}]
                }
            }
        ]
        
        with patch('src.core.discogs.collection_sync.DiscogsAPIClient') as mock_api_class:
            mock_api_client = AsyncMock()
            mock_api_client.get_all_collection_items.return_value = mock_items
            mock_api_class.return_value.__aenter__.return_value = mock_api_client
            
            with patch('src.core.discogs.collection_sync.get_db_session') as mock_get_session:
                mock_session = AsyncMock()
                mock_get_session.return_value.__enter__.return_value = mock_session
                
                # Mock the _process_collection_items method
                with patch.object(sync_client, '_process_collection_items') as mock_process:
                    with patch.object(sync_client, '_update_data_source') as mock_update:
                        result = await sync_client.sync_collection()
                        
                        # Verify methods were called
                        mock_process.assert_called_once()
                        mock_update.assert_called_once()
                        
                        # Verify result
                        assert isinstance(result, dict)