import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

from sqlalchemy.orm import Session

from .api_client import DiscogsAPIClient, DiscogsAPIError
from ..database.database import get_db_session
from ..database.models import (
    Artist, Release, Master, Label, UserCollection,
    DataSource, CollectionFolder
)
from ..utils.config import load_config


logger = logging.getLogger(__name__)


class CollectionSyncError(Exception):
    """Exception raised during collection synchronization."""
    pass


class CollectionSync:
    """Handles synchronization of user collection and wantlist with local database."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize collection sync.
        
        Args:
            config: Configuration dictionary. If None, loads from default config.
        """
        self.config = config or load_config()
        self.username = self.config['discogs']['api']['username']
    
    async def sync_collection(self, force_refresh: bool = False) -> Dict[str, int]:
        """Sync user's collection with local database.
        
        Args:
            force_refresh: If True, re-fetch all collection data
            
        Returns:
            Dictionary with sync statistics
        """
        stats = {
            'collection_items_added': 0,
            'collection_items_updated': 0,
            'releases_fetched': 0,
            'artists_fetched': 0,
            'labels_fetched': 0,
            'errors': 0
        }
        
        try:
            async with DiscogsAPIClient(self.config) as client:
                # Fetch collection data
                logger.info("Fetching collection data from Discogs API")
                collection_items = await client.get_all_collection_items(self.username)
                
                if not collection_items:
                    logger.warning("No collection items found")
                    return stats
                
                logger.info(f"Found {len(collection_items)} collection items")
                
                # Process collection items
                with get_db_session() as db:
                    await self._process_collection_items(
                        db, client, collection_items, force_refresh, stats
                    )
                
                # Mark data source as updated
                self._update_data_source(db, 'collection')
                
        except Exception as e:
            logger.error(f"Collection sync failed: {e}")
            raise CollectionSyncError(f"Failed to sync collection: {e}")
        
        logger.info(f"Collection sync completed: {stats}")
        return stats
    
    async def sync_wantlist(self, force_refresh: bool = False) -> Dict[str, int]:
        """Sync user's wantlist with local database.
        
        Args:
            force_refresh: If True, re-fetch all wantlist data
            
        Returns:
            Dictionary with sync statistics
        """
        stats = {
            'wantlist_items_added': 0,
            'wantlist_items_updated': 0,
            'releases_fetched': 0,
            'errors': 0
        }
        
        # Note: Wantlist support requires additional database tables
        # For now, just log and return empty stats
        logger.info("Wantlist sync not yet implemented - requires additional database schema")
        return stats
    
    async def _process_collection_items(self, db: Session, client: DiscogsAPIClient,
                                       collection_items: List[Dict[str, Any]],
                                       force_refresh: bool, stats: Dict[str, int]):
        """Process collection items and store in database."""
        existing_items = self._get_existing_collection_items(db)
        release_ids_to_fetch = set()
        
        for item in collection_items:
            try:
                basic_release = item.get('basic_information', {})
                release_id = basic_release.get('id')
                
                if not release_id:
                    logger.warning("Collection item missing release ID")
                    stats['errors'] += 1
                    continue
                
                # Check if collection item exists
                instance_id = item.get('instance_id')
                existing_item = existing_items.get(instance_id)
                
                if existing_item and not force_refresh:
                    stats['collection_items_updated'] += 1
                    continue
                
                # Store/update collection item
                collection_item = self._create_or_update_collection_item(
                    db, item, existing_item
                )
                
                if existing_item:
                    stats['collection_items_updated'] += 1
                else:
                    stats['collection_items_added'] += 1
                
                # Check if we need to fetch full release data
                if not self._release_exists(db, release_id) or force_refresh:
                    release_ids_to_fetch.add(release_id)
                
            except Exception as e:
                logger.error(f"Error processing collection item {item.get('instance_id')}: {e}")
                stats['errors'] += 1
        
        # First commit the collection items (without releases)
        db.commit()
        
        # Fetch missing release data
        if release_ids_to_fetch:
            await self._fetch_releases(db, client, list(release_ids_to_fetch), stats)
            db.commit()
    
    async def _process_wantlist_items(self, db: Session, client: DiscogsAPIClient,
                                     wantlist_items: List[Dict[str, Any]],
                                     force_refresh: bool, stats: Dict[str, int]):
        """Process wantlist items and store in database."""
        existing_items = self._get_existing_wantlist_items(db)
        release_ids_to_fetch = set()
        
        for item in wantlist_items:
            try:
                basic_release = item.get('basic_information', {})
                release_id = basic_release.get('id')
                
                if not release_id:
                    logger.warning("Wantlist item missing release ID")
                    stats['errors'] += 1
                    continue
                
                # Check if wantlist item exists
                want_id = item.get('id')
                existing_item = existing_items.get(want_id)
                
                if existing_item and not force_refresh:
                    stats['wantlist_items_updated'] += 1
                    continue
                
                # Store/update wantlist item
                wantlist_item = self._create_or_update_wantlist_item(
                    db, item, existing_item
                )
                
                if existing_item:
                    stats['wantlist_items_updated'] += 1
                else:
                    stats['wantlist_items_added'] += 1
                
                # Check if we need to fetch full release data
                if not self._release_exists(db, release_id) or force_refresh:
                    release_ids_to_fetch.add(release_id)
                
            except Exception as e:
                logger.error(f"Error processing wantlist item {item.get('id')}: {e}")
                stats['errors'] += 1
        
        # Fetch missing release data
        if release_ids_to_fetch:
            await self._fetch_releases(db, client, release_ids_to_fetch, stats)
        
        db.commit()
    
    async def _fetch_releases(self, db: Session, client: DiscogsAPIClient,
                             release_ids: List[int], stats: Dict[str, int]):
        """Fetch and store release data for given release IDs."""
        logger.info(f"Fetching {len(release_ids)} releases from API")
        
        # Keep track of entities we've already processed in this batch
        processed_artists = set()
        processed_labels = set()
        processed_masters = set()
        
        for i, release_id in enumerate(release_ids, 1):
            try:
                # Fetch release data
                release_data = await client.get_release(release_id)
                
                # Store release and related entities
                await self._store_release_data(db, client, release_data, stats, 
                                             processed_artists, processed_labels, processed_masters)
                
                if i % 5 == 0:
                    logger.info(f"Processed {i}/{len(release_ids)} releases")
                    db.commit()  # Periodic commit
                
            except DiscogsAPIError as e:
                logger.error(f"Failed to fetch release {release_id}: {e}")
                stats['errors'] += 1
            except Exception as e:
                logger.error(f"Unexpected error fetching release {release_id}: {e}")
                stats['errors'] += 1
    
    async def _store_release_data(self, db: Session, client: DiscogsAPIClient,
                                 release_data: Dict[str, Any], stats: Dict[str, int],
                                 processed_artists: set, processed_labels: set, processed_masters: set):
        """Store release data and related entities in database."""
        release_id = release_data.get('id')
        
        # Store artists (avoid duplicates in this batch)
        for artist_data in release_data.get('artists', []):
            artist_id = artist_data.get('id')
            if artist_id and artist_id not in processed_artists and not self._artist_exists(db, artist_id):
                try:
                    full_artist = await client.get_artist(artist_id)
                    self._store_artist(db, full_artist)
                    processed_artists.add(artist_id)
                    stats['artists_fetched'] += 1
                except Exception as e:
                    logger.error(f"Failed to fetch artist {artist_id}: {e}")
        
        # Store labels (avoid duplicates in this batch)
        for label_data in release_data.get('labels', []):
            label_id = label_data.get('id')
            if label_id and label_id not in processed_labels and not self._label_exists(db, label_id):
                try:
                    full_label = await client.get_label(label_id)
                    self._store_label(db, full_label)
                    processed_labels.add(label_id)
                    stats['labels_fetched'] += 1
                except Exception as e:
                    logger.error(f"Failed to fetch label {label_id}: {e}")
        
        # Store master if present (avoid duplicates in this batch)
        master_id = release_data.get('master_id')
        if master_id and master_id not in processed_masters and not self._master_exists(db, master_id):
            try:
                master_data = await client.get_master(master_id)
                self._store_master(db, master_data)
                processed_masters.add(master_id)
            except Exception as e:
                logger.error(f"Failed to fetch master {master_id}: {e}")
        
        # Store release
        self._store_release(db, release_data)
        stats['releases_fetched'] += 1
    
    def _get_existing_collection_items(self, db: Session) -> Dict[int, UserCollection]:
        """Get existing collection items mapped by instance_id."""
        # First get or create user
        user = self._get_or_create_user(db)
        items = db.query(UserCollection).filter_by(user_id=user.id).all()
        return {item.instance_id: item for item in items}
    
    def _get_existing_wantlist_items(self, db: Session) -> Dict[int, Any]:
        """Get existing wantlist items mapped by want_id."""
        # Wantlist not yet implemented
        return {}
    
    def _create_or_update_collection_item(self, db: Session, item_data: Dict[str, Any],
                                         existing_item: Optional[UserCollection]) -> UserCollection:
        """Create or update a collection item."""
        basic_release = item_data.get('basic_information', {})
        
        user = self._get_or_create_user(db)
        
        if existing_item:
            # Update existing item
            existing_item.date_added = datetime.fromisoformat(
                item_data.get('date_added', '').replace('Z', '+00:00')
            ) if item_data.get('date_added') else None
            existing_item.folder_id = item_data.get('folder_id')
            existing_item.rating = item_data.get('rating')
            # Handle notes field (sometimes it's a list, sometimes a string)
            notes = item_data.get('notes')
            existing_item.notes = str(notes) if notes else None
            existing_item.basic_information = basic_release
            return existing_item
        else:
            # Create new item
            collection_item = UserCollection(
                user_id=user.id,
                instance_id=item_data.get('instance_id'),
                release_id=basic_release.get('id'),
                date_added=datetime.fromisoformat(
                    item_data.get('date_added', '').replace('Z', '+00:00')
                ) if item_data.get('date_added') else None,
                folder_id=item_data.get('folder_id'),
                rating=item_data.get('rating'),
                notes=str(item_data.get('notes')) if item_data.get('notes') else None,
                basic_information=basic_release,
                created_at=datetime.utcnow()
            )
            db.add(collection_item)
            return collection_item
    
    def _create_or_update_wantlist_item(self, db: Session, item_data: Dict[str, Any],
                                       existing_item: Optional[Any]) -> Any:
        """Create or update a wantlist item."""
        # Wantlist not yet implemented
        pass
    
    def _release_exists(self, db: Session, release_id: int) -> bool:
        """Check if release exists in database."""
        return db.query(Release).filter_by(id=release_id).first() is not None
    
    def _artist_exists(self, db: Session, artist_id: int) -> bool:
        """Check if artist exists in database."""
        return db.query(Artist).filter_by(id=artist_id).first() is not None
    
    def _label_exists(self, db: Session, label_id: int) -> bool:
        """Check if label exists in database."""
        return db.query(Label).filter_by(id=label_id).first() is not None
    
    def _master_exists(self, db: Session, master_id: int) -> bool:
        """Check if master exists in database."""
        return db.query(Master).filter_by(id=master_id).first() is not None
    
    def _store_artist(self, db: Session, artist_data: Dict[str, Any]):
        """Store artist data in database."""
        artist_id = artist_data.get('id')
        if not artist_id:
            return
            
        # Check if artist already exists
        existing = db.query(Artist).filter_by(id=artist_id).first()
        
        if existing:
            # Update existing artist
            existing.name = artist_data.get('name')
            existing.real_name = artist_data.get('realname')
            existing.profile = artist_data.get('profile')
            existing.data_quality = artist_data.get('data_quality')
            existing.name_variations = artist_data.get('namevariations', [])
            existing.aliases = artist_data.get('aliases', [])
            existing.urls = artist_data.get('urls', [])
            existing.members = artist_data.get('members', [])
            existing.groups = artist_data.get('groups', [])
            existing.images = artist_data.get('images', [])
            existing.updated_at = datetime.utcnow()
        else:
            # Create new artist (handle potential race condition)
            try:
                artist = Artist(
                    id=artist_id,
                    name=artist_data.get('name'),
                    real_name=artist_data.get('realname'),
                    profile=artist_data.get('profile'),
                    data_quality=artist_data.get('data_quality'),
                    name_variations=artist_data.get('namevariations', []),
                    aliases=artist_data.get('aliases', []),
                    urls=artist_data.get('urls', []),
                    members=artist_data.get('members', []),
                    groups=artist_data.get('groups', []),
                    images=artist_data.get('images', []),
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow()
                )
                db.add(artist)
                db.flush()  # Try to detect duplicates early
            except Exception as e:
                # If there's a constraint error, roll back this add
                db.rollback()
                # Re-check if artist exists (might have been created by another process)
                existing = db.query(Artist).filter_by(id=artist_id).first()
                if not existing:
                    # If still doesn't exist, re-raise the error
                    raise e
    
    def _store_label(self, db: Session, label_data: Dict[str, Any]):
        """Store label data in database."""
        label_id = label_data.get('id')
        if not label_id:
            return
            
        # Check if label already exists
        existing = db.query(Label).filter_by(id=label_id).first()
        
        if existing:
            # Update existing label
            existing.name = label_data.get('name')
            existing.profile = label_data.get('profile')
            existing.data_quality = label_data.get('data_quality')
            existing.contact_info = label_data.get('contactinfo')
            # Don't set parent_label_id yet to avoid foreign key constraint issues
            existing.parent_label_id = None
            existing.subsidiaries = label_data.get('sublabels', [])
            existing.urls = label_data.get('urls', [])
            existing.images = label_data.get('images', [])
            existing.updated_at = datetime.utcnow()
        else:
            # Create new label (handle potential race condition)
            try:
                label = Label(
                    id=label_id,
                    name=label_data.get('name'),
                    profile=label_data.get('profile'),
                    data_quality=label_data.get('data_quality'),
                    contact_info=label_data.get('contactinfo'),
                    parent_label_id=None,  # Skip parent relationships for now
                    subsidiaries=label_data.get('sublabels', []),
                    urls=label_data.get('urls', []),
                    images=label_data.get('images', []),
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow()
                )
                db.add(label)
                db.flush()  # Try to detect duplicates early
            except Exception as e:
                # If there's a constraint error, roll back this add
                db.rollback()
                # Re-check if label exists (might have been created by another process)
                existing = db.query(Label).filter_by(id=label_id).first()
                if not existing:
                    # If still doesn't exist, re-raise the error
                    raise e
    
    def _store_master(self, db: Session, master_data: Dict[str, Any]):
        """Store master release data in database."""
        # Check if master already exists
        existing = db.query(Master).filter_by(id=master_data.get('id')).first()
        
        if existing:
            # Update existing master
            existing.title = master_data.get('title')
            existing.year = master_data.get('year')
            existing.main_release_id = master_data.get('main_release')
            existing.data_quality = master_data.get('data_quality')
            existing.artists = master_data.get('artists', [])
            existing.genres = master_data.get('genres', [])
            existing.styles = master_data.get('styles', [])
            existing.images = master_data.get('images', [])
            existing.videos = master_data.get('videos', [])
            existing.updated_at = datetime.utcnow()
        else:
            # Create new master
            master = Master(
                id=master_data.get('id'),
                title=master_data.get('title'),
                year=master_data.get('year'),
                main_release_id=master_data.get('main_release'),
                data_quality=master_data.get('data_quality'),
                artists=master_data.get('artists', []),
                genres=master_data.get('genres', []),
                styles=master_data.get('styles', []),
                images=master_data.get('images', []),
                videos=master_data.get('videos', []),
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            db.add(master)
    
    def _store_release(self, db: Session, release_data: Dict[str, Any]):
        """Store release data in database."""
        # Check if release already exists
        existing = db.query(Release).filter_by(id=release_data.get('id')).first()
        
        if existing:
            # Update existing release
            existing.title = release_data.get('title')
            existing.year = release_data.get('year')
            existing.released = release_data.get('released')
            existing.master_id = release_data.get('master_id')
            existing.country = release_data.get('country')
            existing.status = release_data.get('status')
            existing.data_quality = release_data.get('data_quality')
            existing.artists = release_data.get('artists', [])
            existing.extraartists = release_data.get('extraartists', [])
            existing.labels = release_data.get('labels', [])
            existing.companies = release_data.get('companies', [])
            existing.formats = release_data.get('formats', [])
            existing.genres = release_data.get('genres', [])
            existing.styles = release_data.get('styles', [])
            existing.tracklist = release_data.get('tracklist', [])
            existing.identifiers = release_data.get('identifiers', [])
            existing.images = release_data.get('images', [])
            existing.videos = release_data.get('videos', [])
            existing.notes = release_data.get('notes')
            existing.estimated_weight = release_data.get('estimated_weight')
            existing.updated_at = datetime.utcnow()
        else:
            # Create new release
            release = Release(
                id=release_data.get('id'),
                title=release_data.get('title'),
                year=release_data.get('year'),
                released=release_data.get('released'),
                master_id=release_data.get('master_id'),
                country=release_data.get('country'),
                status=release_data.get('status'),
                data_quality=release_data.get('data_quality'),
                artists=release_data.get('artists', []),
                extraartists=release_data.get('extraartists', []),
                labels=release_data.get('labels', []),
                companies=release_data.get('companies', []),
                formats=release_data.get('formats', []),
                genres=release_data.get('genres', []),
                styles=release_data.get('styles', []),
                tracklist=release_data.get('tracklist', []),
                identifiers=release_data.get('identifiers', []),
                images=release_data.get('images', []),
                videos=release_data.get('videos', []),
                notes=release_data.get('notes'),
                estimated_weight=release_data.get('estimated_weight'),
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            db.add(release)
    
    def _update_data_source(self, db: Session, source_type: str):
        """Update data source timestamp."""
        source = db.query(DataSource).filter_by(
            source_type=source_type,
            source_name='discogs_api'
        ).first()
        
        if source:
            source.last_updated = datetime.utcnow()
        else:
            source = DataSource(
                source_type=source_type,
                source_name='discogs_api',
                last_updated=datetime.utcnow(),
                source_metadata={'username': self.username}
            )
            db.add(source)
        
        db.commit()
    
    def _get_or_create_user(self, db: Session):
        """Get or create user record."""
        from ..database.models import User
        
        user = db.query(User).filter_by(discogs_username=self.username).first()
        if not user:
            user = User(
                discogs_username=self.username,
                created_at=datetime.utcnow()
            )
            db.add(user)
            db.commit()
            db.refresh(user)
        
        return user