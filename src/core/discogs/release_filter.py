"""
Release filtering logic for selective ingestion strategies.
"""

import logging
from typing import Dict, Any, Set, Optional
from sqlalchemy.orm import Session

from ..database.models import UserCollection, Master, Artist, Label, Release

logger = logging.getLogger(__name__)


class ReleaseFilter:
    """Filters releases based on ingestion strategy."""
    
    def __init__(self, config: Dict[str, Any], session: Session):
        """Initialize the release filter.
        
        Args:
            config: Application configuration
            session: Database session
        """
        self.config = config
        self.session = session
        self.release_config = config.get('ingestion', {}).get('releases', {})
        self.strategy = self.release_config.get('strategy', 'all')
        
        # Cached sets for filtering
        self._collection_release_ids: Optional[Set[int]] = None
        self._collection_master_ids: Optional[Set[int]] = None
        self._collection_artist_ids: Optional[Set[int]] = None
        self._collection_label_ids: Optional[Set[int]] = None
        self._master_release_ids: Optional[Set[int]] = None
        
        logger.info(f"Initialized release filter with strategy: {self.strategy}")
    
    def should_include_release(self, release_id: int, master_id: Optional[int] = None, 
                             artist_ids: Optional[Set[int]] = None, 
                             label_ids: Optional[Set[int]] = None) -> bool:
        """Determine if a release should be included based on the current strategy.
        
        Args:
            release_id: Discogs release ID
            master_id: Associated master release ID
            artist_ids: Set of artist IDs for this release
            label_ids: Set of label IDs for this release
            
        Returns:
            True if the release should be included, False otherwise
        """
        if self.strategy == 'all':
            return True
        
        if self.strategy == 'collection_only':
            return self._is_collection_release_or_master(release_id, master_id)
        
        if self.strategy == 'selective':
            return self._is_selective_release(release_id, master_id, artist_ids, label_ids)
        
        # Default to including the release
        logger.warning(f"Unknown strategy '{self.strategy}', including release {release_id}")
        return True
    
    def _is_collection_release(self, release_id: int) -> bool:
        """Check if a release is in any user's collection."""
        if self._collection_release_ids is None:
            self._load_collection_release_ids()
        
        return release_id in self._collection_release_ids
    
    def _is_collection_release_or_master(self, release_id: int, master_id: Optional[int] = None) -> bool:
        """Check if a release should be included in collection_only strategy.
        
        This includes:
        1. Releases directly in the collection
        2. If include_master_releases is True, releases linked to masters in collection
        """
        # Always include direct collection releases
        if self._is_collection_release(release_id):
            return True
        
        # Check master release expansion if enabled
        if (master_id and 
            self.release_config.get('include_master_releases', False) and
            self._is_collection_master(master_id)):
            logger.debug(f"Including release {release_id} via master {master_id}")
            return True
        
        return False
    
    def _is_selective_release(self, release_id: int, master_id: Optional[int] = None,
                            artist_ids: Optional[Set[int]] = None,
                            label_ids: Optional[Set[int]] = None) -> bool:
        """Check if a release should be included under selective strategy."""
        # Always include collection releases
        if self._is_collection_release(release_id):
            return True
        
        # Include releases from collection masters
        if (master_id and 
            self.release_config.get('include_masters_releases', True) and
            self._is_collection_master(master_id)):
            return True
        
        # Include releases from collection artists (with limits)
        if (artist_ids and 
            self.release_config.get('include_artist_releases', False) and
            self._has_collection_artist(artist_ids)):
            return self._check_artist_release_limit(artist_ids)
        
        # Include releases from collection labels (with limits)
        if (label_ids and 
            self.release_config.get('include_label_releases', False) and
            self._has_collection_label(label_ids)):
            return self._check_label_release_limit(label_ids)
        
        return False
    
    def _load_collection_release_ids(self) -> None:
        """Load all release IDs that are in user collections."""
        try:
            release_ids = self.session.query(UserCollection.release_id).distinct().all()
            self._collection_release_ids = {rid[0] for rid in release_ids}
            logger.info(f"Loaded {len(self._collection_release_ids)} collection release IDs")
        except Exception as e:
            logger.warning(f"Error loading collection release IDs: {e}")
            self._collection_release_ids = set()
    
    def _is_collection_master(self, master_id: int) -> bool:
        """Check if a master is referenced by collection releases."""
        if self._collection_master_ids is None:
            try:
                master_ids = (self.session.query(Release.master_id)
                            .join(UserCollection, Release.id == UserCollection.release_id)
                            .filter(Release.master_id.isnot(None))
                            .distinct().all())
                self._collection_master_ids = {mid[0] for mid in master_ids if mid[0]}
                logger.info(f"Loaded {len(self._collection_master_ids)} collection master IDs")
            except Exception as e:
                logger.warning(f"Error loading collection master IDs: {e}")
                self._collection_master_ids = set()
        
        return master_id in self._collection_master_ids
    
    def _has_collection_artist(self, artist_ids: Set[int]) -> bool:
        """Check if any of the artists are in collections."""
        if self._collection_artist_ids is None:
            try:
                # This would require the ReleaseArtist junction table to be populated
                # For now, return False until we implement artist tracking
                self._collection_artist_ids = set()
                logger.info("Artist-based filtering not yet implemented")
            except Exception as e:
                logger.warning(f"Error loading collection artist IDs: {e}")
                self._collection_artist_ids = set()
        
        return bool(artist_ids.intersection(self._collection_artist_ids))
    
    def _has_collection_label(self, label_ids: Set[int]) -> bool:
        """Check if any of the labels are in collections."""
        if self._collection_label_ids is None:
            try:
                # This would require the ReleaseLabel junction table to be populated
                # For now, return False until we implement label tracking
                self._collection_label_ids = set()
                logger.info("Label-based filtering not yet implemented")
            except Exception as e:
                logger.warning(f"Error loading collection label IDs: {e}")
                self._collection_label_ids = set()
        
        return bool(label_ids.intersection(self._collection_label_ids))
    
    def _check_artist_release_limit(self, artist_ids: Set[int]) -> bool:
        """Check if we've exceeded the release limit for these artists."""
        # TODO: Implement release counting per artist
        max_releases = self.release_config.get('max_releases_per_artist', 50)
        # For now, allow all releases (implement counting later)
        return True
    
    def _check_label_release_limit(self, label_ids: Set[int]) -> bool:
        """Check if we've exceeded the release limit for these labels."""
        # TODO: Implement release counting per label
        max_releases = self.release_config.get('max_releases_per_label', 100)
        # For now, allow all releases (implement counting later)
        return True
    
    def get_strategy_info(self) -> Dict[str, Any]:
        """Get information about the current filtering strategy."""
        return {
            'strategy': self.strategy,
            'collection_releases': len(self._collection_release_ids) if self._collection_release_ids else 0,
            'collection_masters': len(self._collection_master_ids) if self._collection_master_ids else 0,
            'include_master_releases': self.release_config.get('include_master_releases', False),
            'include_artist_releases': self.release_config.get('include_artist_releases', False),
            'include_label_releases': self.release_config.get('include_label_releases', False),
            'max_releases_per_artist': self.release_config.get('max_releases_per_artist', 50),
            'max_releases_per_label': self.release_config.get('max_releases_per_label', 100)
        }


def create_release_filter(config: Dict[str, Any], session: Session) -> ReleaseFilter:
    """Factory function to create a release filter.
    
    Args:
        config: Application configuration
        session: Database session
        
    Returns:
        ReleaseFilter instance
    """
    return ReleaseFilter(config, session)


def get_master_release_ids(session: Session, master_ids: Set[int]) -> Set[int]:
    """Get all release IDs that belong to the given master IDs.
    
    Args:
        session: Database session
        master_ids: Set of master IDs
        
    Returns:
        Set of release IDs linked to these masters
    """
    if not master_ids:
        return set()
    
    try:
        release_ids = (session.query(Release.id)
                      .filter(Release.master_id.in_(master_ids))
                      .all())
        result = {rid[0] for rid in release_ids}
        logger.info(f"Found {len(result)} releases for {len(master_ids)} masters")
        return result
    except Exception as e:
        logger.error(f"Error getting master release IDs: {e}")
        return set()


def get_collection_master_ids(session: Session) -> Set[int]:
    """Get all master IDs that have releases in user collections.
    
    Args:
        session: Database session
        
    Returns:
        Set of master IDs with releases in collections
    """
    try:
        master_ids = (session.query(Release.master_id)
                     .join(UserCollection, Release.id == UserCollection.release_id)
                     .filter(Release.master_id.isnot(None))
                     .distinct().all())
        result = {mid[0] for mid in master_ids if mid[0]}
        logger.info(f"Found {len(result)} masters with collection releases")
        return result
    except Exception as e:
        logger.error(f"Error getting collection master IDs: {e}")
        return set()