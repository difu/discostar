"""
Unit tests for database models.

Tests model creation, relationships, and basic functionality.
"""

import pytest
from datetime import datetime, timedelta

from src.core.database.models import (
    User, UserCollection, Artist, Label, Master, Release,
    ReleaseArtist, ReleaseLabel, Track, CollectionFolder
)
from tests.fixtures.model_factories import (
    create_test_user, create_test_artist, create_test_label,
    create_test_master, create_test_release, create_test_collection_item,
    create_test_track
)


class TestUserModel:
    """Test User model functionality."""
    
    def test_create_user(self, clean_db_session):
        """Test creating a user."""
        user = create_test_user()
        clean_db_session.add(user)
        clean_db_session.commit()
        
        assert user.id is not None
        assert user.discogs_username == "test_user"
        assert user.display_name == "Test User"
    
    def test_user_relationships(self, clean_db_session):
        """Test user relationships with collection."""
        user = create_test_user()
        clean_db_session.add(user)
        clean_db_session.commit()
        
        # Create collection item
        collection_item = create_test_collection_item(
            user_id=user.id,
            release_id=123456
        )
        clean_db_session.add(collection_item)
        clean_db_session.commit()
        
        # Test relationship
        assert len(user.collection) == 1
        assert user.collection[0].release_id == 123456


class TestArtistModel:
    """Test Artist model functionality."""
    
    def test_create_artist(self, clean_db_session):
        """Test creating an artist."""
        artist = create_test_artist(
            artist_id=12345,
            name="Test Artist",
            real_name="Real Test Artist"
        )
        clean_db_session.add(artist)
        clean_db_session.commit()
        
        assert artist.id == 12345
        assert artist.name == "Test Artist"
        assert artist.real_name == "Real Test Artist"
        assert artist.data_quality == "Correct"
    
    def test_artist_json_fields(self, clean_db_session):
        """Test artist JSON field storage."""
        artist = create_test_artist()
        artist.name_variations = ["Alt Name 1", "Alt Name 2"]
        artist.aliases = ["Alias 1"]
        artist.urls = ["https://example.com"]
        
        clean_db_session.add(artist)
        clean_db_session.commit()
        
        # Reload from database
        reloaded = clean_db_session.query(Artist).filter_by(id=artist.id).first()
        assert len(reloaded.name_variations) == 2
        assert "Alt Name 1" in reloaded.name_variations
        assert len(reloaded.aliases) == 1
        assert len(reloaded.urls) == 1


class TestReleaseModel:
    """Test Release model functionality."""
    
    def test_create_release(self, clean_db_session):
        """Test creating a release."""
        release = create_test_release(
            release_id=123456,
            title="Test Album",
            year=2020
        )
        clean_db_session.add(release)
        clean_db_session.commit()
        
        assert release.id == 123456
        assert release.title == "Test Album"
        assert release.year == 2020
        assert release.country == "US"
    
    def test_release_master_relationship(self, clean_db_session):
        """Test release-master relationship."""
        # Create master
        master = create_test_master(master_id=54321, title="Master Album")
        clean_db_session.add(master)
        clean_db_session.commit()
        
        # Create release linked to master
        release = create_test_release(
            release_id=123456,
            master_id=master.id,
            title="Release of Master Album"
        )
        clean_db_session.add(release)
        clean_db_session.commit()
        
        # Test relationship
        assert release.master is not None
        assert release.master.id == 54321
        assert len(master.releases) == 1
        assert master.releases[0].id == 123456
    
    def test_release_tracklist_json(self, clean_db_session):
        """Test release tracklist JSON storage."""
        release = create_test_release()
        release.tracklist = [
            {"position": "A1", "title": "Track 1", "duration": "3:45"},
            {"position": "A2", "title": "Track 2", "duration": "4:12"}
        ]
        
        clean_db_session.add(release)
        clean_db_session.commit()
        
        # Reload and verify
        reloaded = clean_db_session.query(Release).filter_by(id=release.id).first()
        assert len(reloaded.tracklist) == 2
        assert reloaded.tracklist[0]["title"] == "Track 1"
        assert reloaded.tracklist[1]["duration"] == "4:12"


class TestCollectionModel:
    """Test UserCollection model functionality."""
    
    def test_create_collection_item(self, clean_db_session):
        """Test creating a collection item."""
        # Create user first
        user = create_test_user()
        clean_db_session.add(user)
        clean_db_session.commit()
        
        # Create collection item
        item = create_test_collection_item(
            user_id=user.id,
            release_id=123456,
            rating=5,
            notes="Great album!"
        )
        clean_db_session.add(item)
        clean_db_session.commit()
        
        assert item.user_id == user.id
        assert item.release_id == 123456
        assert item.rating == 5
        assert item.notes == "Great album!"
    
    def test_collection_basic_information_json(self, clean_db_session):
        """Test collection item basic_information JSON field."""
        user = create_test_user()
        clean_db_session.add(user)
        clean_db_session.commit()
        
        basic_info = {
            "id": 123456,
            "title": "Test Album",
            "artists": [{"name": "Test Artist"}],
            "year": 2020
        }
        
        item = create_test_collection_item(
            user_id=user.id,
            release_id=123456
        )
        item.basic_information = basic_info
        
        clean_db_session.add(item)
        clean_db_session.commit()
        
        # Reload and verify
        reloaded = clean_db_session.query(UserCollection).filter_by(id=item.id).first()
        assert reloaded.basic_information["title"] == "Test Album"
        assert reloaded.basic_information["year"] == 2020
        assert len(reloaded.basic_information["artists"]) == 1


class TestTrackModel:
    """Test Track model functionality."""
    
    def test_create_track(self, clean_db_session):
        """Test creating a track."""
        # Create release first
        release = create_test_release(release_id=123456)
        clean_db_session.add(release)
        clean_db_session.commit()
        
        # Create track
        track = create_test_track(
            release_id=release.id,
            position="A1",
            title="Test Track",
            duration="3:45"
        )
        clean_db_session.add(track)
        clean_db_session.commit()
        
        assert track.release_id == release.id
        assert track.position == "A1"
        assert track.title == "Test Track"
        assert track.duration == "3:45"
        assert track.duration_seconds == 225  # 3*60 + 45
    
    def test_track_release_relationship(self, clean_db_session):
        """Test track-release relationship."""
        release = create_test_release(release_id=123456)
        clean_db_session.add(release)
        clean_db_session.commit()
        
        # Create multiple tracks
        track1 = create_test_track(release_id=release.id, position="A1", title="Track 1")
        track2 = create_test_track(release_id=release.id, position="A2", title="Track 2")
        
        clean_db_session.add_all([track1, track2])
        clean_db_session.commit()
        
        # Test relationship
        assert len(release.track_listing) == 2
        assert release.track_listing[0].title == "Track 1"
        assert release.track_listing[1].title == "Track 2"


class TestRelationshipModels:
    """Test relationship models (many-to-many)."""
    
    def test_release_artist_relationship(self, clean_db_session):
        """Test ReleaseArtist relationship model."""
        # Create entities
        artist = create_test_artist(artist_id=12345)
        release = create_test_release(release_id=54321)
        
        clean_db_session.add_all([artist, release])
        clean_db_session.commit()
        
        # Create relationship
        release_artist = ReleaseArtist(
            release_id=release.id,
            artist_id=artist.id,
            role="Primary",
            name="Artist Name as Credited",
            join_relation="&"
        )
        
        clean_db_session.add(release_artist)
        clean_db_session.commit()
        
        assert release_artist.release_id == release.id
        assert release_artist.artist_id == artist.id
        assert release_artist.role == "Primary"
    
    def test_release_label_relationship(self, clean_db_session):
        """Test ReleaseLabel relationship model."""
        # Create entities
        label = create_test_label(label_id=12345)
        release = create_test_release(release_id=54321)
        
        clean_db_session.add_all([label, release])
        clean_db_session.commit()
        
        # Create relationship
        release_label = ReleaseLabel(
            release_id=release.id,
            label_id=label.id,
            catalog_number="TEST001"
        )
        
        clean_db_session.add(release_label)
        clean_db_session.commit()
        
        assert release_label.release_id == release.id
        assert release_label.label_id == label.id
        assert release_label.catalog_number == "TEST001"