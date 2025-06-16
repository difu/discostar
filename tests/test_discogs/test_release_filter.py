"""
Unit tests for the ReleaseFilter class and related functionality.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from src.core.discogs.release_filter import (
    ReleaseFilter, 
    create_release_filter, 
    get_master_release_ids, 
    get_collection_master_ids
)
from src.core.database.models import UserCollection, Release, Master


class TestReleaseFilter:
    """Test cases for the ReleaseFilter class."""
    
    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return Mock(spec=Session)
    
    @pytest.fixture
    def base_config(self):
        """Base configuration for tests."""
        return {
            'ingestion': {
                'releases': {
                    'strategy': 'all',
                    'include_master_releases': False,
                    'include_artist_releases': False,
                    'include_label_releases': False,
                    'max_releases_per_artist': 50,
                    'max_releases_per_label': 100
                }
            }
        }
    
    @pytest.fixture
    def collection_config(self, base_config):
        """Configuration for collection_only strategy."""
        config = base_config.copy()
        config['ingestion']['releases']['strategy'] = 'collection_only'
        return config
    
    @pytest.fixture
    def selective_config(self, base_config):
        """Configuration for selective strategy."""
        config = base_config.copy()
        config['ingestion']['releases']['strategy'] = 'selective'
        config['ingestion']['releases']['include_artist_releases'] = True
        config['ingestion']['releases']['include_label_releases'] = True
        return config
    
    def test_init_with_default_strategy(self, mock_session, base_config):
        """Test initialization with default strategy."""
        filter_obj = ReleaseFilter(base_config, mock_session)
        
        assert filter_obj.strategy == 'all'
        assert filter_obj.config == base_config
        assert filter_obj.session == mock_session
        assert filter_obj._collection_release_ids is None
        assert filter_obj._collection_master_ids is None
    
    def test_init_with_collection_strategy(self, mock_session, collection_config):
        """Test initialization with collection_only strategy."""
        filter_obj = ReleaseFilter(collection_config, mock_session)
        
        assert filter_obj.strategy == 'collection_only'
        assert filter_obj.release_config['include_master_releases'] is False
    
    def test_init_with_empty_config(self, mock_session):
        """Test initialization with empty config."""
        empty_config = {}
        filter_obj = ReleaseFilter(empty_config, mock_session)
        
        assert filter_obj.strategy == 'all'  # Default value
        assert filter_obj.release_config == {}
    
    def test_should_include_release_all_strategy(self, mock_session, base_config):
        """Test that 'all' strategy includes all releases."""
        filter_obj = ReleaseFilter(base_config, mock_session)
        
        assert filter_obj.should_include_release(123) is True
        assert filter_obj.should_include_release(456, master_id=789) is True
        assert filter_obj.should_include_release(999, master_id=None, 
                                                artist_ids={1, 2}, label_ids={3, 4}) is True
    
    @patch('src.core.discogs.release_filter.logger')
    def test_should_include_release_unknown_strategy(self, mock_logger, mock_session, base_config):
        """Test handling of unknown strategy."""
        base_config['ingestion']['releases']['strategy'] = 'unknown_strategy'
        filter_obj = ReleaseFilter(base_config, mock_session)
        
        result = filter_obj.should_include_release(123)
        
        assert result is True  # Default to including
        mock_logger.warning.assert_called_with(
            "Unknown strategy 'unknown_strategy', including release 123"
        )
    
    def test_should_include_release_collection_only_direct_match(self, mock_session, collection_config):
        """Test collection_only strategy with direct collection match."""
        filter_obj = ReleaseFilter(collection_config, mock_session)
        
        # Mock the collection release IDs query
        mock_session.query.return_value.distinct.return_value.all.return_value = [(123,), (456,)]
        
        assert filter_obj.should_include_release(123) is True
        assert filter_obj.should_include_release(456) is True
        assert filter_obj.should_include_release(789) is False
    
    def test_should_include_release_collection_only_with_master_expansion(self, mock_session, collection_config):
        """Test collection_only strategy with master release expansion."""
        collection_config['ingestion']['releases']['include_master_releases'] = True
        filter_obj = ReleaseFilter(collection_config, mock_session)
        
        # Mock collection release IDs (direct matches)
        mock_session.query.return_value.distinct.return_value.all.return_value = [(123,)]
        
        # Mock collection master IDs query
        mock_query = Mock()
        mock_session.query.return_value = mock_query
        mock_query.join.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.distinct.return_value = mock_query
        
        # First call for release IDs, second call for master IDs
        mock_query.all.side_effect = [[(123,)], [(456,)]]
        
        # Test direct collection release
        assert filter_obj.should_include_release(123) is True
        
        # Test release via master expansion
        assert filter_obj.should_include_release(999, master_id=456) is True
        
        # Test release not in collection and master not in collection
        assert filter_obj.should_include_release(888, master_id=777) is False
    
    def test_should_include_release_selective_strategy(self, mock_session, selective_config):
        """Test selective strategy logic."""
        filter_obj = ReleaseFilter(selective_config, mock_session)
        
        # Mock collection release IDs
        mock_session.query.return_value.distinct.return_value.all.return_value = [(123,)]
        
        # Test direct collection release (should always be included)
        assert filter_obj.should_include_release(123) is True
        
        # Test non-collection release
        assert filter_obj.should_include_release(456) is False
    
    def test_load_collection_release_ids_success(self, mock_session, collection_config):
        """Test successful loading of collection release IDs."""
        filter_obj = ReleaseFilter(collection_config, mock_session)
        
        # Mock successful query
        mock_session.query.return_value.distinct.return_value.all.return_value = [
            (123,), (456,), (789,)
        ]
        
        filter_obj._load_collection_release_ids()
        
        assert filter_obj._collection_release_ids == {123, 456, 789}
    
    @patch('src.core.discogs.release_filter.logger')
    def test_load_collection_release_ids_error(self, mock_logger, mock_session, collection_config):
        """Test error handling when loading collection release IDs."""
        filter_obj = ReleaseFilter(collection_config, mock_session)
        
        # Mock query error
        mock_session.query.side_effect = SQLAlchemyError("Database error")
        
        filter_obj._load_collection_release_ids()
        
        assert filter_obj._collection_release_ids == set()
        mock_logger.warning.assert_called_with("Error loading collection release IDs: Database error")
    
    def test_is_collection_master_success(self, mock_session, collection_config):
        """Test successful master ID checking."""
        filter_obj = ReleaseFilter(collection_config, mock_session)
        
        # Mock the master IDs query
        mock_query = Mock()
        mock_session.query.return_value = mock_query
        mock_query.join.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.distinct.return_value = mock_query
        mock_query.all.return_value = [(123,), (456,), (None,)]  # Include None to test filtering
        
        assert filter_obj._is_collection_master(123) is True
        assert filter_obj._is_collection_master(456) is True
        assert filter_obj._is_collection_master(789) is False
        
        # Verify None values are filtered out
        assert None not in filter_obj._collection_master_ids
    
    @patch('src.core.discogs.release_filter.logger')
    def test_is_collection_master_error(self, mock_logger, mock_session, collection_config):
        """Test error handling when loading collection master IDs."""
        filter_obj = ReleaseFilter(collection_config, mock_session)
        
        # Mock query error
        mock_session.query.side_effect = SQLAlchemyError("Database error")
        
        result = filter_obj._is_collection_master(123)
        
        assert result is False
        assert filter_obj._collection_master_ids == set()
        mock_logger.warning.assert_called_with("Error loading collection master IDs: Database error")
    
    def test_has_collection_artist_not_implemented(self, mock_session, selective_config):
        """Test that artist-based filtering is not yet implemented."""
        filter_obj = ReleaseFilter(selective_config, mock_session)
        
        result = filter_obj._has_collection_artist({1, 2, 3})
        
        assert result is False
        assert filter_obj._collection_artist_ids == set()
    
    def test_has_collection_label_not_implemented(self, mock_session, selective_config):
        """Test that label-based filtering is not yet implemented."""
        filter_obj = ReleaseFilter(selective_config, mock_session)
        
        result = filter_obj._has_collection_label({1, 2, 3})
        
        assert result is False
        assert filter_obj._collection_label_ids == set()
    
    def test_check_artist_release_limit_always_true(self, mock_session, selective_config):
        """Test that artist release limit checking currently allows all releases."""
        filter_obj = ReleaseFilter(selective_config, mock_session)
        
        result = filter_obj._check_artist_release_limit({1, 2, 3})
        
        assert result is True
    
    def test_check_label_release_limit_always_true(self, mock_session, selective_config):
        """Test that label release limit checking currently allows all releases."""
        filter_obj = ReleaseFilter(selective_config, mock_session)
        
        result = filter_obj._check_label_release_limit({1, 2, 3})
        
        assert result is True
    
    def test_get_strategy_info_all_strategy(self, mock_session, base_config):
        """Test strategy info for 'all' strategy."""
        filter_obj = ReleaseFilter(base_config, mock_session)
        
        info = filter_obj.get_strategy_info()
        
        expected = {
            'strategy': 'all',
            'collection_releases': 0,
            'collection_masters': 0,
            'include_master_releases': False,
            'include_artist_releases': False,
            'include_label_releases': False,
            'max_releases_per_artist': 50,
            'max_releases_per_label': 100
        }
        assert info == expected
    
    def test_get_strategy_info_with_loaded_data(self, mock_session, collection_config):
        """Test strategy info with loaded collection data."""
        filter_obj = ReleaseFilter(collection_config, mock_session)
        
        # Pre-populate the cached data
        filter_obj._collection_release_ids = {1, 2, 3, 4, 5}
        filter_obj._collection_master_ids = {10, 20, 30}
        
        info = filter_obj.get_strategy_info()
        
        assert info['strategy'] == 'collection_only'
        assert info['collection_releases'] == 5
        assert info['collection_masters'] == 3
    
    def test_caching_behavior(self, mock_session, collection_config):
        """Test that IDs are cached after first load."""
        filter_obj = ReleaseFilter(collection_config, mock_session)
        
        # Mock successful query
        mock_session.query.return_value.distinct.return_value.all.return_value = [(123,), (456,)]
        
        # First call should trigger database query
        result1 = filter_obj._is_collection_release(123)
        assert result1 is True
        
        # Reset mock to verify no additional queries
        mock_session.reset_mock()
        
        # Second call should use cached data
        result2 = filter_obj._is_collection_release(456)
        assert result2 is True
        
        # Verify no additional database queries were made
        mock_session.query.assert_not_called()


class TestReleaseFilterFactoryFunction:
    """Test cases for the create_release_filter factory function."""
    
    def test_create_release_filter(self):
        """Test the factory function creates a ReleaseFilter instance."""
        config = {'ingestion': {'releases': {'strategy': 'all'}}}
        session = Mock(spec=Session)
        
        filter_obj = create_release_filter(config, session)
        
        assert isinstance(filter_obj, ReleaseFilter)
        assert filter_obj.config == config
        assert filter_obj.session == session


class TestUtilityFunctions:
    """Test cases for utility functions."""
    
    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return Mock(spec=Session)
    
    def test_get_master_release_ids_success(self, mock_session):
        """Test successful retrieval of master release IDs."""
        master_ids = {123, 456, 789}
        
        # Mock the query
        mock_query = Mock()
        mock_session.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.all.return_value = [(1001,), (1002,), (1003,)]
        
        result = get_master_release_ids(mock_session, master_ids)
        
        assert result == {1001, 1002, 1003}
        mock_query.filter.assert_called_once()
    
    def test_get_master_release_ids_empty_input(self, mock_session):
        """Test get_master_release_ids with empty master_ids set."""
        result = get_master_release_ids(mock_session, set())
        
        assert result == set()
        mock_session.query.assert_not_called()
    
    @patch('src.core.discogs.release_filter.logger')
    def test_get_master_release_ids_error(self, mock_logger, mock_session):
        """Test error handling in get_master_release_ids."""
        master_ids = {123, 456}
        
        # Mock query error
        mock_session.query.side_effect = SQLAlchemyError("Database error")
        
        result = get_master_release_ids(mock_session, master_ids)
        
        assert result == set()
        mock_logger.error.assert_called_with("Error getting master release IDs: Database error")
    
    def test_get_collection_master_ids_success(self, mock_session):
        """Test successful retrieval of collection master IDs."""
        # Mock the query chain
        mock_query = Mock()
        mock_session.query.return_value = mock_query
        mock_query.join.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.distinct.return_value = mock_query
        mock_query.all.return_value = [(123,), (456,), (None,), (789,)]
        
        result = get_collection_master_ids(mock_session)
        
        # Should filter out None values
        assert result == {123, 456, 789}
        mock_query.join.assert_called_once()
        mock_query.filter.assert_called_once()
    
    @patch('src.core.discogs.release_filter.logger')
    def test_get_collection_master_ids_error(self, mock_logger, mock_session):
        """Test error handling in get_collection_master_ids."""
        # Mock query error
        mock_session.query.side_effect = SQLAlchemyError("Database error")
        
        result = get_collection_master_ids(mock_session)
        
        assert result == set()
        mock_logger.error.assert_called_with("Error getting collection master IDs: Database error")


class TestReleaseFilterEdgeCases:
    """Test edge cases and boundary conditions."""
    
    @pytest.fixture
    def mock_session(self):
        """Create a mock database session."""
        return Mock(spec=Session)
    
    def test_filter_with_none_master_id(self, mock_session):
        """Test filtering behavior when master_id is None."""
        config = {
            'ingestion': {
                'releases': {
                    'strategy': 'collection_only',
                    'include_master_releases': True
                }
            }
        }
        filter_obj = ReleaseFilter(config, mock_session)
        
        # Mock collection release IDs (empty)
        mock_session.query.return_value.distinct.return_value.all.return_value = []
        
        # Should not include release with None master_id
        result = filter_obj.should_include_release(123, master_id=None)
        assert result is False
    
    def test_filter_with_empty_artist_and_label_sets(self, mock_session):
        """Test filtering with empty artist and label sets."""
        config = {
            'ingestion': {
                'releases': {
                    'strategy': 'selective',
                    'include_artist_releases': True,
                    'include_label_releases': True
                }
            }
        }
        filter_obj = ReleaseFilter(config, mock_session)
        
        # Mock empty collection
        mock_session.query.return_value.distinct.return_value.all.return_value = []
        
        # Should not include release with empty sets
        result = filter_obj.should_include_release(123, artist_ids=set(), label_ids=set())
        assert result is False
    
    def test_filter_with_none_artist_and_label_sets(self, mock_session):
        """Test filtering with None artist and label sets."""
        config = {
            'ingestion': {
                'releases': {
                    'strategy': 'selective',
                    'include_artist_releases': True,
                    'include_label_releases': True
                }
            }
        }
        filter_obj = ReleaseFilter(config, mock_session)
        
        # Mock empty collection
        mock_session.query.return_value.distinct.return_value.all.return_value = []
        
        # Should not include release with None sets
        result = filter_obj.should_include_release(123, artist_ids=None, label_ids=None)
        assert result is False
    
    def test_multiple_database_queries_caching(self, mock_session):
        """Test that multiple queries use cached results."""
        config = {
            'ingestion': {
                'releases': {
                    'strategy': 'collection_only',
                    'include_master_releases': True
                }
            }
        }
        filter_obj = ReleaseFilter(config, mock_session)
        
        # Mock queries - should only be called once each due to caching
        mock_session.query.return_value.distinct.return_value.all.return_value = [(123,)]
        
        mock_query = Mock()
        mock_session.query.return_value = mock_query
        mock_query.join.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.distinct.return_value = mock_query
        mock_query.all.side_effect = [[(123,)], [(456,)]]  # For releases, then masters
        
        # Multiple calls should use cached data
        filter_obj.should_include_release(123)
        filter_obj.should_include_release(456)
        filter_obj.should_include_release(789, master_id=456)
        
        # Verify caching worked
        assert filter_obj._collection_release_ids is not None
        assert filter_obj._collection_master_ids is not None