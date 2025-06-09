"""
Test data extraction utility for DiscoStar.

Extracts a collection-driven sample of data from the production database
for use in unit testing.
"""

import logging
import random
import sqlite3
from pathlib import Path
from typing import Set, List, Dict, Any, Optional

from sqlalchemy.orm import Session
from sqlalchemy import text

from .database import get_database_manager
from .models import (
    User, UserCollection, Artist, Label, Master, Release,
    ReleaseArtist, ReleaseLabel, Track, CollectionFolder,
    DataSource, SyncStatus
)


logger = logging.getLogger(__name__)


class DiscogsTestDataExtractor:
    """Extracts collection-driven test data from production database."""
    
    def __init__(self, source_db_path: str, target_db_path: str):
        """Initialize test data extractor.
        
        Args:
            source_db_path: Path to production database
            target_db_path: Path to target test database
        """
        self.source_db_path = source_db_path
        self.target_db_path = target_db_path
        
        # Track collected IDs to ensure referential integrity
        self.selected_release_ids: Set[int] = set()
        self.selected_master_ids: Set[int] = set()
        self.selected_artist_ids: Set[int] = set()
        self.selected_label_ids: Set[int] = set()
        self.selected_user_ids: Set[int] = set()
    
    def extract_test_data(self, collection_sample_size: int = 200) -> None:
        """Extract test data using collection-driven sampling.
        
        Args:
            collection_sample_size: Number of collection items to sample
        """
        logger.info(f"Extracting test data with {collection_sample_size} collection items")
        
        # Step 1: Select random collection items
        collection_items = self._select_random_collection_items(collection_sample_size)
        
        # Step 2: Collect all dependencies
        self._collect_dependencies(collection_items)
        
        # Step 3: Add extra data for variety
        self._add_extra_data()
        
        # Step 4: Export to test database
        self._export_to_test_database()
        
        logger.info("Test data extraction completed")
    
    def _select_random_collection_items(self, sample_size: int) -> List[Dict[str, Any]]:
        """Select random collection items from user collection.
        
        Args:
            sample_size: Number of items to select
            
        Returns:
            List of collection item records
        """
        with sqlite3.connect(self.source_db_path) as conn:
            cursor = conn.cursor()
            
            # Get total collection count
            cursor.execute("SELECT COUNT(*) FROM user_collection")
            total_count = cursor.fetchone()[0]
            
            logger.info(f"Total collection items: {total_count}")
            
            # Select random sample
            cursor.execute("""
                SELECT * FROM user_collection 
                ORDER BY RANDOM() 
                LIMIT ?
            """, (sample_size,))
            
            columns = [desc[0] for desc in cursor.description]
            collection_items = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            # Track release IDs and user IDs
            for item in collection_items:
                self.selected_release_ids.add(item['release_id'])
                self.selected_user_ids.add(item['user_id'])
            
            logger.info(f"Selected {len(collection_items)} collection items")
            return collection_items
    
    def _collect_dependencies(self, collection_items: List[Dict[str, Any]]) -> None:
        """Collect all dependencies for selected collection items.
        
        Args:
            collection_items: Selected collection items
        """
        with sqlite3.connect(self.source_db_path) as conn:
            cursor = conn.cursor()
            
            # Get release dependencies
            release_ids_str = ','.join(map(str, self.selected_release_ids))
            
            # Collect master IDs from releases
            cursor.execute(f"""
                SELECT DISTINCT master_id FROM releases 
                WHERE id IN ({release_ids_str}) AND master_id IS NOT NULL
            """)
            master_ids = [row[0] for row in cursor.fetchall()]
            self.selected_master_ids.update(master_ids)
            
            # Collect artist IDs from release_artists
            cursor.execute(f"""
                SELECT DISTINCT artist_id FROM release_artists 
                WHERE release_id IN ({release_ids_str})
            """)
            artist_ids = [row[0] for row in cursor.fetchall()]
            self.selected_artist_ids.update(artist_ids)
            
            # Collect label IDs from release_labels
            cursor.execute(f"""
                SELECT DISTINCT label_id FROM release_labels 
                WHERE release_id IN ({release_ids_str})
            """)
            label_ids = [row[0] for row in cursor.fetchall()]
            self.selected_label_ids.update(label_ids)
        
        logger.info(f"Collected dependencies: {len(self.selected_master_ids)} masters, "
                   f"{len(self.selected_artist_ids)} artists, {len(self.selected_label_ids)} labels")
    
    def _add_extra_data(self) -> None:
        """Add extra random data for test variety."""
        with sqlite3.connect(self.source_db_path) as conn:
            cursor = conn.cursor()
            
            # Add 50 extra random releases
            cursor.execute("""
                SELECT id FROM releases 
                WHERE id NOT IN ({}) 
                ORDER BY RANDOM() 
                LIMIT 50
            """.format(','.join(map(str, self.selected_release_ids))))
            extra_release_ids = [row[0] for row in cursor.fetchall()]
            self.selected_release_ids.update(extra_release_ids)
            
            # Add 100 extra random artists
            cursor.execute("""
                SELECT id FROM artists 
                WHERE id NOT IN ({}) 
                ORDER BY RANDOM() 
                LIMIT 100
            """.format(','.join(map(str, self.selected_artist_ids))))
            extra_artist_ids = [row[0] for row in cursor.fetchall()]
            self.selected_artist_ids.update(extra_artist_ids)
            
            # Add 30 extra random masters
            cursor.execute("""
                SELECT id FROM masters 
                WHERE id NOT IN ({}) 
                ORDER BY RANDOM() 
                LIMIT 30
            """.format(','.join(map(str, self.selected_master_ids))))
            extra_master_ids = [row[0] for row in cursor.fetchall()]
            self.selected_master_ids.update(extra_master_ids)
            
            # Add 30 extra random labels
            cursor.execute("""
                SELECT id FROM labels 
                WHERE id NOT IN ({}) 
                ORDER BY RANDOM() 
                LIMIT 30
            """.format(','.join(map(str, self.selected_label_ids))))
            extra_label_ids = [row[0] for row in cursor.fetchall()]
            self.selected_label_ids.update(extra_label_ids)
        
        logger.info(f"Added extra data for variety")
    
    def _export_to_test_database(self) -> None:
        """Export selected data to test database."""
        # Create target database
        target_path = Path(self.target_db_path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        
        if target_path.exists():
            target_path.unlink()
        
        with sqlite3.connect(self.source_db_path) as source_conn:
            with sqlite3.connect(self.target_db_path) as target_conn:
                # Copy schema
                self._copy_schema(source_conn, target_conn)
                
                # Copy data
                self._copy_data(source_conn, target_conn)
        
        logger.info(f"Test database created at: {self.target_db_path}")
    
    def _copy_schema(self, source_conn: sqlite3.Connection, target_conn: sqlite3.Connection) -> None:
        """Copy database schema to target database."""
        source_cursor = source_conn.cursor()
        target_cursor = target_conn.cursor()
        
        # Get all CREATE statements
        source_cursor.execute("""
            SELECT sql FROM sqlite_master 
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """)
        
        for (sql,) in source_cursor.fetchall():
            if sql:
                target_cursor.execute(sql)
        
        # Copy indexes
        source_cursor.execute("""
            SELECT sql FROM sqlite_master 
            WHERE type='index' AND name NOT LIKE 'sqlite_%'
        """)
        
        for (sql,) in source_cursor.fetchall():
            if sql:
                try:
                    target_cursor.execute(sql)
                except sqlite3.OperationalError:
                    # Index might already exist due to table creation
                    pass
        
        target_conn.commit()
    
    def _copy_data(self, source_conn: sqlite3.Connection, target_conn: sqlite3.Connection) -> None:
        """Copy selected data to target database."""
        source_cursor = source_conn.cursor()
        target_cursor = target_conn.cursor()
        
        # Copy users
        if self.selected_user_ids:
            user_ids_str = ','.join(map(str, self.selected_user_ids))
            source_cursor.execute(f"SELECT * FROM users WHERE id IN ({user_ids_str})")
            users = source_cursor.fetchall()
            if users:
                placeholders = ','.join(['?' for _ in range(len(users[0]))])
                target_cursor.executemany(f"INSERT INTO users VALUES ({placeholders})", users)
        
        # Copy artists
        if self.selected_artist_ids:
            artist_ids_str = ','.join(map(str, self.selected_artist_ids))
            source_cursor.execute(f"SELECT * FROM artists WHERE id IN ({artist_ids_str})")
            artists = source_cursor.fetchall()
            if artists:
                placeholders = ','.join(['?' for _ in range(len(artists[0]))])
                target_cursor.executemany(f"INSERT INTO artists VALUES ({placeholders})", artists)
        
        # Copy labels
        if self.selected_label_ids:
            label_ids_str = ','.join(map(str, self.selected_label_ids))
            source_cursor.execute(f"SELECT * FROM labels WHERE id IN ({label_ids_str})")
            labels = source_cursor.fetchall()
            if labels:
                placeholders = ','.join(['?' for _ in range(len(labels[0]))])
                target_cursor.executemany(f"INSERT INTO labels VALUES ({placeholders})", labels)
        
        # Copy masters
        if self.selected_master_ids:
            master_ids_str = ','.join(map(str, self.selected_master_ids))
            source_cursor.execute(f"SELECT * FROM masters WHERE id IN ({master_ids_str})")
            masters = source_cursor.fetchall()
            if masters:
                placeholders = ','.join(['?' for _ in range(len(masters[0]))])
                target_cursor.executemany(f"INSERT INTO masters VALUES ({placeholders})", masters)
        
        # Copy releases
        if self.selected_release_ids:
            release_ids_str = ','.join(map(str, self.selected_release_ids))
            source_cursor.execute(f"SELECT * FROM releases WHERE id IN ({release_ids_str})")
            releases = source_cursor.fetchall()
            if releases:
                placeholders = ','.join(['?' for _ in range(len(releases[0]))])
                target_cursor.executemany(f"INSERT INTO releases VALUES ({placeholders})", releases)
        
        # Copy relationship tables
        self._copy_relationship_tables(source_cursor, target_cursor)
        
        # Copy collection data
        self._copy_collection_data(source_cursor, target_cursor)
        
        target_conn.commit()
    
    def _copy_relationship_tables(self, source_cursor: sqlite3.Cursor, target_cursor: sqlite3.Cursor) -> None:
        """Copy relationship tables with referential integrity."""
        # Copy release_artists
        if self.selected_release_ids and self.selected_artist_ids:
            release_ids_str = ','.join(map(str, self.selected_release_ids))
            artist_ids_str = ','.join(map(str, self.selected_artist_ids))
            source_cursor.execute(f"""
                SELECT * FROM release_artists 
                WHERE release_id IN ({release_ids_str}) 
                AND artist_id IN ({artist_ids_str})
            """)
            release_artists = source_cursor.fetchall()
            if release_artists:
                placeholders = ','.join(['?' for _ in range(len(release_artists[0]))])
                target_cursor.executemany(f"INSERT INTO release_artists VALUES ({placeholders})", release_artists)
        
        # Copy release_labels
        if self.selected_release_ids and self.selected_label_ids:
            release_ids_str = ','.join(map(str, self.selected_release_ids))
            label_ids_str = ','.join(map(str, self.selected_label_ids))
            source_cursor.execute(f"""
                SELECT * FROM release_labels 
                WHERE release_id IN ({release_ids_str}) 
                AND label_id IN ({label_ids_str})
            """)
            release_labels = source_cursor.fetchall()
            if release_labels:
                placeholders = ','.join(['?' for _ in range(len(release_labels[0]))])
                target_cursor.executemany(f"INSERT INTO release_labels VALUES ({placeholders})", release_labels)
        
        # Copy tracks
        if self.selected_release_ids:
            release_ids_str = ','.join(map(str, self.selected_release_ids))
            source_cursor.execute(f"SELECT * FROM tracks WHERE release_id IN ({release_ids_str})")
            tracks = source_cursor.fetchall()
            if tracks:
                placeholders = ','.join(['?' for _ in range(len(tracks[0]))])
                target_cursor.executemany(f"INSERT INTO tracks VALUES ({placeholders})", tracks)
    
    def _copy_collection_data(self, source_cursor: sqlite3.Cursor, target_cursor: sqlite3.Cursor) -> None:
        """Copy user collection and related data."""
        # Copy user_collection for selected releases and users
        if self.selected_user_ids and self.selected_release_ids:
            user_ids_str = ','.join(map(str, self.selected_user_ids))
            release_ids_str = ','.join(map(str, self.selected_release_ids))
            source_cursor.execute(f"""
                SELECT * FROM user_collection 
                WHERE user_id IN ({user_ids_str}) 
                AND release_id IN ({release_ids_str})
            """)
            collection = source_cursor.fetchall()
            if collection:
                placeholders = ','.join(['?' for _ in range(len(collection[0]))])
                target_cursor.executemany(f"INSERT INTO user_collection VALUES ({placeholders})", collection)
        
        # Copy collection_folders
        if self.selected_user_ids:
            user_ids_str = ','.join(map(str, self.selected_user_ids))
            source_cursor.execute(f"SELECT * FROM collection_folders WHERE user_id IN ({user_ids_str})")
            folders = source_cursor.fetchall()
            if folders:
                placeholders = ','.join(['?' for _ in range(len(folders[0]))])
                target_cursor.executemany(f"INSERT INTO collection_folders VALUES ({placeholders})", folders)
        
        # Copy sync_status
        if self.selected_user_ids:
            user_ids_str = ','.join(map(str, self.selected_user_ids))
            source_cursor.execute(f"SELECT * FROM sync_status WHERE user_id IN ({user_ids_str})")
            sync_status = source_cursor.fetchall()
            if sync_status:
                placeholders = ','.join(['?' for _ in range(len(sync_status[0]))])
                target_cursor.executemany(f"INSERT INTO sync_status VALUES ({placeholders})", sync_status)
        
        # Copy data_sources
        source_cursor.execute("SELECT * FROM data_sources")
        data_sources = source_cursor.fetchall()
        if data_sources:
            placeholders = ','.join(['?' for _ in range(len(data_sources[0]))])
            target_cursor.executemany(f"INSERT INTO data_sources VALUES ({placeholders})", data_sources)


def extract_test_data(
    source_db_path: Optional[str] = None,
    target_db_path: str = "tests/data/test_database.db",
    collection_sample_size: int = 200
) -> None:
    """Extract test data from production database.
    
    Args:
        source_db_path: Path to source database. If None, uses default location.
        target_db_path: Path to target test database
        collection_sample_size: Number of collection items to sample
    """
    if source_db_path is None:
        from ..utils.config import get_data_directory
        source_db_path = str(get_data_directory() / "discostar.db")
    
    extractor = DiscogsTestDataExtractor(source_db_path, target_db_path)
    extractor.extract_test_data(collection_sample_size)