"""
Relationship processor for populating join tables from release JSON data.
"""

import logging
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy import text

from ..database.models import Release, ReleaseArtist, ReleaseLabel, Track
from ..utils.duration import parse_duration_to_seconds

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
            # Prepare bulk data lists
            artists_data = []
            labels_data = []
            tracks_data = []
            
            # Collect artist relationships
            artists_data.extend(self._collect_release_artists(release))
            
            # Collect label relationships
            labels_data.extend(self._collect_release_labels(release))
            
            # Collect tracks
            tracks_data.extend(self._collect_release_tracks(release))
            
            # Bulk insert data
            if artists_data:
                stats['artists_created'] += self._bulk_insert_artists(session, artists_data)
            
            if labels_data:
                stats['labels_created'] += self._bulk_insert_labels(session, labels_data)
            
            if tracks_data:
                # Clear existing tracks first
                session.query(Track).filter_by(release_id=release.id).delete()
                stats['tracks_created'] += self._bulk_insert_tracks(session, tracks_data)
            
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
        
        # Keep track of already processed combinations to avoid duplicates
        processed_combinations = set()
        
        # Process main artists
        if release.artists:
            for artist_data in release.artists:
                try:
                    release_artist = self._create_release_artist(
                        release.id, artist_data, role=""
                    )
                    if release_artist:
                        # Create unique key for deduplication
                        key = (release_artist.release_id, release_artist.artist_id, release_artist.role)
                        if key not in processed_combinations:
                            # Check if already exists in database
                            existing = session.query(ReleaseArtist).filter(
                                ReleaseArtist.release_id == release_artist.release_id,
                                ReleaseArtist.artist_id == release_artist.artist_id,
                                ReleaseArtist.role == release_artist.role
                            ).first()
                            if not existing:
                                session.add(release_artist)
                                created_count += 1
                            processed_combinations.add(key)
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
                        # Create unique key for deduplication
                        key = (release_artist.release_id, release_artist.artist_id, release_artist.role)
                        if key not in processed_combinations:
                            # Check if already exists in database
                            existing = session.query(ReleaseArtist).filter(
                                ReleaseArtist.release_id == release_artist.release_id,
                                ReleaseArtist.artist_id == release_artist.artist_id,
                                ReleaseArtist.role == release_artist.role
                            ).first()
                            if not existing:
                                session.add(release_artist)
                                created_count += 1
                            processed_combinations.add(key)
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
        
        # Keep track of already processed combinations to avoid duplicates
        processed_combinations = set()
        
        for label_data in release.labels:
            try:
                release_label = self._create_release_label(release.id, label_data)
                if release_label:
                    # Create unique key for deduplication
                    key = (release_label.release_id, release_label.label_id, release_label.catalog_number)
                    if key not in processed_combinations:
                        # Check if already exists in database
                        existing = session.query(ReleaseLabel).filter(
                            ReleaseLabel.release_id == release_label.release_id,
                            ReleaseLabel.label_id == release_label.label_id,
                            ReleaseLabel.catalog_number == release_label.catalog_number
                        ).first()
                        if not existing:
                            session.add(release_label)
                            created_count += 1
                        processed_combinations.add(key)
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
            
            duration_str = track_data.get('duration')
            duration_seconds = parse_duration_to_seconds(duration_str)
            
            track = Track(
                release_id=release_id,
                position=track_data.get('position', ''),
                title=title,
                duration=duration_str,
                duration_seconds=duration_seconds,
                type=track_data.get('type_')  # 'type_' to avoid Python keyword conflict
            )
            
            return track
            
        except Exception as e:
            logger.debug(f"Error creating track: {e}")
            return None
    
    def process_existing_releases(self, session: Session, batch_size: int = 100, commit_interval: int = 1000) -> Dict[str, int]:
        """Process relationships for all existing releases in the database.
        
        Args:
            session: Database session
            batch_size: Number of releases to process in each batch
            commit_interval: Number of releases to process before committing
            
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
            batch_operations = 0
            
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
                        batch_operations += 1
                        
                        # Commit in batches instead of after each release
                        if batch_operations >= commit_interval:
                            session.commit()
                            batch_operations = 0
                            logger.info(f"Progress: {stats['releases_processed']:,}/{total_releases:,} releases "
                                      f"({stats['releases_processed']/total_releases*100:.1f}%) - "
                                      f"Artists: {stats['artists_created']:,}, Labels: {stats['labels_created']:,}, "
                                      f"Tracks: {stats['tracks_created']:,}")
                        
                    except Exception as e:
                        logger.error(f"Error processing release {release.id}: {e}")
                        session.rollback()
                        stats['errors'] += 1
                        batch_operations = 0  # Reset batch counter after rollback
                
                offset += batch_size
            
            # Final commit for any remaining operations
            if batch_operations > 0:
                session.commit()
        
        except Exception as e:
            logger.error(f"Error during batch processing: {e}")
            session.rollback()
            raise
        
        logger.info(f"Relationship processing complete: {stats}")
        return stats
    
    def process_releases_by_ids(self, session: Session, release_ids: List[int], 
                               commit_interval: int = 1000) -> Dict[str, int]:
        """Process relationships for specific release IDs.
        
        Args:
            session: Database session
            release_ids: List of release IDs to process
            commit_interval: Number of releases to process before committing
            
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
        
        if not release_ids:
            return stats
        
        logger.info(f"Processing relationships for {len(release_ids):,} releases")
        
        try:
            batch_operations = 0
            total_releases = len(release_ids)
            
            for i, release_id in enumerate(release_ids):
                try:
                    release = session.query(Release).filter_by(id=release_id).first()
                    if not release:
                        logger.warning(f"Release {release_id} not found")
                        continue
                    
                    rel_stats = self.process_release_relationships(session, release)
                    stats['artists_created'] += rel_stats['artists_created']
                    stats['labels_created'] += rel_stats['labels_created']
                    stats['tracks_created'] += rel_stats['tracks_created']
                    stats['errors'] += rel_stats['errors']
                    stats['releases_processed'] += 1
                    batch_operations += 1
                    
                    # Commit in batches
                    if batch_operations >= commit_interval:
                        session.commit()
                        batch_operations = 0
                        logger.info(f"Progress: {stats['releases_processed']:,}/{total_releases:,} releases "
                                  f"({stats['releases_processed']/total_releases*100:.1f}%) - "
                                  f"Artists: {stats['artists_created']:,}, Labels: {stats['labels_created']:,}, "
                                  f"Tracks: {stats['tracks_created']:,}")
                    
                except Exception as e:
                    logger.error(f"Error processing release {release_id}: {e}")
                    session.rollback()
                    stats['errors'] += 1
                    batch_operations = 0  # Reset batch counter after rollback
            
            # Final commit for any remaining operations
            if batch_operations > 0:
                session.commit()
        
        except Exception as e:
            logger.error(f"Error during release processing: {e}")
            session.rollback()
            raise
        
        logger.info(f"Release relationship processing complete: {stats}")
        return stats
    
    def _collect_release_artists(self, release: Release) -> List[Dict[str, Any]]:
        """Collect artist relationship data for bulk insert.
        
        Args:
            release: Release object
            
        Returns:
            List of artist relationship dictionaries
        """
        artists_data = []
        processed_combinations = set()
        
        # Process main artists
        if release.artists:
            for artist_data in release.artists:
                artist_dict = self._create_artist_dict(release.id, artist_data, "")
                if artist_dict:
                    key = (artist_dict['release_id'], artist_dict['artist_id'], artist_dict['role'])
                    if key not in processed_combinations:
                        artists_data.append(artist_dict)
                        processed_combinations.add(key)
        
        # Process extra artists
        if release.extraartists:
            for artist_data in release.extraartists:
                role = artist_data.get('role', '')
                artist_dict = self._create_artist_dict(release.id, artist_data, role)
                if artist_dict:
                    key = (artist_dict['release_id'], artist_dict['artist_id'], artist_dict['role'])
                    if key not in processed_combinations:
                        artists_data.append(artist_dict)
                        processed_combinations.add(key)
        
        return artists_data
    
    def _create_artist_dict(self, release_id: int, artist_data: Dict[str, Any], role: str) -> Optional[Dict[str, Any]]:
        """Create artist dictionary for bulk insert.
        
        Args:
            release_id: Release ID
            artist_data: Artist data dictionary
            role: Artist role
            
        Returns:
            Artist dictionary or None if creation failed
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
            
            return {
                'release_id': release_id,
                'artist_id': artist_id,
                'role': role or '',
                'name': artist_data.get('name', ''),
                'anv': artist_data.get('anv'),
                'join_relation': artist_data.get('join'),
                'tracks': artist_data.get('tracks')
            }
            
        except Exception as e:
            logger.debug(f"Error creating artist dict: {e}")
            return None
    
    def _collect_release_labels(self, release: Release) -> List[Dict[str, Any]]:
        """Collect label relationship data for bulk insert.
        
        Args:
            release: Release object
            
        Returns:
            List of label relationship dictionaries
        """
        labels_data = []
        processed_combinations = set()
        
        if not release.labels:
            return labels_data
        
        for label_data in release.labels:
            label_dict = self._create_label_dict(release.id, label_data)
            if label_dict:
                key = (label_dict['release_id'], label_dict['label_id'], label_dict['catalog_number'])
                if key not in processed_combinations:
                    labels_data.append(label_dict)
                    processed_combinations.add(key)
        
        return labels_data
    
    def _create_label_dict(self, release_id: int, label_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create label dictionary for bulk insert.
        
        Args:
            release_id: Release ID
            label_data: Label data dictionary
            
        Returns:
            Label dictionary or None if creation failed
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
            
            return {
                'release_id': release_id,
                'label_id': label_id,
                'catalog_number': catalog_number or ''
            }
            
        except Exception as e:
            logger.debug(f"Error creating label dict: {e}")
            return None
    
    def _collect_release_tracks(self, release: Release) -> List[Dict[str, Any]]:
        """Collect track data for bulk insert.
        
        Args:
            release: Release object
            
        Returns:
            List of track dictionaries
        """
        tracks_data = []
        
        if not release.tracklist:
            return tracks_data
        
        for track_data in release.tracklist:
            track_dict = self._create_track_dict(release.id, track_data)
            if track_dict:
                tracks_data.append(track_dict)
        
        return tracks_data
    
    def _create_track_dict(self, release_id: int, track_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create track dictionary for bulk insert.
        
        Args:
            release_id: Release ID
            track_data: Track data dictionary
            
        Returns:
            Track dictionary or None if creation failed
        """
        try:
            title = track_data.get('title')
            if not title:
                return None
            
            duration_str = track_data.get('duration')
            duration_seconds = parse_duration_to_seconds(duration_str)
            
            return {
                'release_id': release_id,
                'position': track_data.get('position', ''),
                'title': title,
                'duration': duration_str,
                'duration_seconds': duration_seconds,
                'type': track_data.get('type_')  # 'type_' to avoid Python keyword conflict
            }
            
        except Exception as e:
            logger.debug(f"Error creating track dict: {e}")
            return None
    
    def _bulk_insert_artists(self, session: Session, artists_data: List[Dict[str, Any]]) -> int:
        """Bulk insert artist relationships.
        
        Args:
            session: Database session
            artists_data: List of artist dictionaries
            
        Returns:
            Number of artists inserted
        """
        if not artists_data:
            return 0
        
        try:
            # Use SQLAlchemy bulk insert with ignore duplicates
            session.execute(
                text("""
                    INSERT OR IGNORE INTO release_artists 
                    (release_id, artist_id, role, name, anv, join_relation, tracks)
                    VALUES (:release_id, :artist_id, :role, :name, :anv, :join_relation, :tracks)
                """),
                artists_data
            )
            return len(artists_data)
        except Exception as e:
            logger.error(f"Error bulk inserting artists: {e}")
            return 0
    
    def _bulk_insert_labels(self, session: Session, labels_data: List[Dict[str, Any]]) -> int:
        """Bulk insert label relationships.
        
        Args:
            session: Database session
            labels_data: List of label dictionaries
            
        Returns:
            Number of labels inserted
        """
        if not labels_data:
            return 0
        
        try:
            # Use SQLAlchemy bulk insert with ignore duplicates
            session.execute(
                text("""
                    INSERT OR IGNORE INTO release_labels 
                    (release_id, label_id, catalog_number)
                    VALUES (:release_id, :label_id, :catalog_number)
                """),
                labels_data
            )
            return len(labels_data)
        except Exception as e:
            logger.error(f"Error bulk inserting labels: {e}")
            return 0
    
    def _bulk_insert_tracks(self, session: Session, tracks_data: List[Dict[str, Any]]) -> int:
        """Bulk insert tracks.
        
        Args:
            session: Database session
            tracks_data: List of track dictionaries
            
        Returns:
            Number of tracks inserted
        """
        if not tracks_data:
            return 0
        
        try:
            # Use SQLAlchemy bulk insert
            session.execute(
                text("""
                    INSERT INTO tracks 
                    (release_id, position, title, duration, duration_seconds, type)
                    VALUES (:release_id, :position, :title, :duration, :duration_seconds, :type)
                """),
                tracks_data
            )
            return len(tracks_data)
        except Exception as e:
            logger.error(f"Error bulk inserting tracks: {e}")
            return 0


def get_relationship_processor() -> RelationshipProcessor:
    """Factory function to create relationship processor.
    
    Returns:
        RelationshipProcessor instance
    """
    return RelationshipProcessor()