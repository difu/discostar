"""
Relationship processor for populating join tables from release JSON data.
"""

import logging
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from ..database.models import Release, ReleaseArtist, ReleaseLabel, Track

logger = logging.getLogger(__name__)


class RelationshipProcessor:
    """Processes release relationships and populates join tables."""
    
    def __init__(self):
        """Initialize the relationship processor."""
        pass
    
    def process_release_relationships(self, session: Session, release: Release) -> Dict[str, int]:
        """Process all relationships for a release and populate join tables.
        
        Args:
            session: Database session
            release: Release object with JSON relationship data
            
        Returns:
            Dictionary with counts of created relationships
        """
        stats = {
            'artists_created': 0,
            'labels_created': 0,
            'tracks_created': 0,
            'errors': 0
        }
        
        try:
            # Process artists (both main artists and extra artists)
            stats['artists_created'] += self._process_release_artists(session, release)
            
            # Process labels
            stats['labels_created'] += self._process_release_labels(session, release)
            
            # Process tracks
            stats['tracks_created'] += self._process_release_tracks(session, release)
            
        except Exception as e:
            logger.error(f"Error processing relationships for release {release.id}: {e}")
            stats['errors'] += 1
        
        return stats
    
    def _process_release_artists(self, session: Session, release: Release) -> int:
        """Process artist relationships for a release.
        
        Args:
            session: Database session
            release: Release object
            
        Returns:
            Number of artist relationships created
        """
        created_count = 0
        
        # Process main artists
        if release.artists:
            for artist_data in release.artists:
                try:
                    release_artist = self._create_release_artist(
                        release.id, artist_data, role=""
                    )
                    if release_artist:
                        session.merge(release_artist)
                        created_count += 1
                except Exception as e:
                    logger.debug(f"Error creating release artist: {e}")
        
        # Process extra artists (producers, engineers, etc.)
        if release.extraartists:
            for artist_data in release.extraartists:
                try:
                    role = artist_data.get('role', '')
                    release_artist = self._create_release_artist(
                        release.id, artist_data, role=role
                    )
                    if release_artist:
                        session.merge(release_artist)
                        created_count += 1
                except Exception as e:
                    logger.debug(f"Error creating release extra artist: {e}")
        
        return created_count
    
    def _create_release_artist(self, release_id: int, artist_data: Dict[str, Any], 
                              role: str) -> Optional[ReleaseArtist]:
        """Create a ReleaseArtist object from artist data.
        
        Args:
            release_id: Release ID
            artist_data: Artist data dictionary
            role: Artist role (empty string for main artists)
            
        Returns:
            ReleaseArtist object or None if creation failed
        """
        try:
            artist_id = artist_data.get('id')
            if not artist_id:
                return None
            
            # Handle nested id structure from XML
            if isinstance(artist_id, dict):
                artist_id = artist_id.get('text') or artist_id.get('value')
            
            if not artist_id:
                return None
            
            try:
                artist_id = int(artist_id)
            except (ValueError, TypeError):
                return None
            
            release_artist = ReleaseArtist(
                release_id=release_id,
                artist_id=artist_id,
                role=role or '',
                name=artist_data.get('name', ''),
                anv=artist_data.get('anv'),
                join_relation=artist_data.get('join'),
                tracks=artist_data.get('tracks')
            )
            
            return release_artist
            
        except Exception as e:
            logger.debug(f"Error creating release artist: {e}")
            return None
    
    def _process_release_labels(self, session: Session, release: Release) -> int:
        """Process label relationships for a release.
        
        Args:
            session: Database session
            release: Release object
            
        Returns:
            Number of label relationships created
        """
        created_count = 0
        
        if not release.labels:
            return 0
        
        for label_data in release.labels:
            try:
                release_label = self._create_release_label(release.id, label_data)
                if release_label:
                    session.merge(release_label)
                    created_count += 1
            except Exception as e:
                logger.debug(f"Error creating release label: {e}")
        
        return created_count
    
    def _create_release_label(self, release_id: int, label_data: Dict[str, Any]) -> Optional[ReleaseLabel]:
        """Create a ReleaseLabel object from label data.
        
        Args:
            release_id: Release ID
            label_data: Label data dictionary
            
        Returns:
            ReleaseLabel object or None if creation failed
        """
        try:
            label_id = label_data.get('id')
            if not label_id:
                return None
            
            # Handle nested id structure from XML
            if isinstance(label_id, dict):
                label_id = label_id.get('text') or label_id.get('value')
            
            if not label_id:
                return None
            
            try:
                label_id = int(label_id)
            except (ValueError, TypeError):
                return None
            
            catalog_number = label_data.get('catno', '') or label_data.get('catalog_number', '')
            
            release_label = ReleaseLabel(
                release_id=release_id,
                label_id=label_id,
                catalog_number=catalog_number or ''
            )
            
            return release_label
            
        except Exception as e:
            logger.debug(f"Error creating release label: {e}")
            return None
    
    def _process_release_tracks(self, session: Session, release: Release) -> int:
        """Process track data for a release.
        
        Args:
            session: Database session
            release: Release object
            
        Returns:
            Number of tracks created
        """
        created_count = 0
        
        if not release.tracklist:
            return 0
        
        # Clear existing tracks for this release to handle updates
        try:
            session.query(Track).filter_by(release_id=release.id).delete()
        except Exception as e:
            logger.debug(f"Error clearing existing tracks for release {release.id}: {e}")
        
        for track_data in release.tracklist:
            try:
                track = self._create_track(release.id, track_data)
                if track:
                    session.add(track)
                    created_count += 1
            except Exception as e:
                logger.debug(f"Error creating track: {e}")
        
        return created_count
    
    def _create_track(self, release_id: int, track_data: Dict[str, Any]) -> Optional[Track]:
        """Create a Track object from track data.
        
        Args:
            release_id: Release ID
            track_data: Track data dictionary
            
        Returns:
            Track object or None if creation failed
        """
        try:
            title = track_data.get('title')
            if not title:
                return None
            
            track = Track(
                release_id=release_id,
                position=track_data.get('position', ''),
                title=title,
                duration=track_data.get('duration'),
                type=track_data.get('type_')  # 'type_' to avoid Python keyword conflict
            )
            
            return track
            
        except Exception as e:
            logger.debug(f"Error creating track: {e}")
            return None
    
    def process_existing_releases(self, session: Session, batch_size: int = 100) -> Dict[str, int]:
        """Process relationships for all existing releases in the database.
        
        Args:
            session: Database session
            batch_size: Number of releases to process in each batch
            
        Returns:
            Dictionary with processing statistics
        """
        stats = {
            'releases_processed': 0,
            'artists_created': 0,
            'labels_created': 0,
            'tracks_created': 0,
            'errors': 0
        }
        
        logger.info("Starting relationship processing for existing releases")
        
        try:
            # Get total count for progress tracking
            total_releases = session.query(Release).count()
            logger.info(f"Processing relationships for {total_releases:,} releases")
            
            # Process releases in batches
            offset = 0
            while True:
                releases = (session.query(Release)
                           .offset(offset)
                           .limit(batch_size)
                           .all())
                
                if not releases:
                    break
                
                for release in releases:
                    try:
                        rel_stats = self.process_release_relationships(session, release)
                        stats['artists_created'] += rel_stats['artists_created']
                        stats['labels_created'] += rel_stats['labels_created']
                        stats['tracks_created'] += rel_stats['tracks_created']
                        stats['errors'] += rel_stats['errors']
                        stats['releases_processed'] += 1
                        
                    except Exception as e:
                        logger.error(f"Error processing release {release.id}: {e}")
                        stats['errors'] += 1
                
                # Commit batch
                session.commit()
                
                # Log progress
                if stats['releases_processed'] % 1000 == 0:
                    logger.info(f"Processed {stats['releases_processed']:,}/{total_releases:,} releases")
                
                offset += batch_size
        
        except Exception as e:
            logger.error(f"Error during batch processing: {e}")
            session.rollback()
            raise
        
        logger.info(f"Relationship processing complete: {stats}")
        return stats


def get_relationship_processor() -> RelationshipProcessor:
    """Factory function to create relationship processor.
    
    Returns:
        RelationshipProcessor instance
    """
    return RelationshipProcessor()