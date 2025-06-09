"""
SQLAlchemy models for DiscoStar database schema.
"""

import json
from datetime import datetime
from typing import Optional, List, Dict, Any

from sqlalchemy import (
    Column, Integer, String, Text, DateTime, Date, 
    ForeignKey, CheckConstraint, UniqueConstraint
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.types import TypeDecorator


Base = declarative_base()


class JSONField(TypeDecorator):
    """JSON field type for SQLite compatibility."""
    
    impl = Text
    cache_ok = True
    
    def process_bind_param(self, value, dialect):
        if value is not None:
            value = json.dumps(value)
        return value
    
    def process_result_value(self, value, dialect):
        if value is not None:
            value = json.loads(value)
        return value


class Artist(Base):
    """Discogs artist entity."""
    
    __tablename__ = 'artists'
    
    id = Column(Integer, primary_key=True)  # Discogs artist ID
    name = Column(String, nullable=False)
    real_name = Column(String)
    profile = Column(Text)
    data_quality = Column(String)
    name_variations = Column(JSONField)  # List of name variations
    aliases = Column(JSONField)  # List of aliases
    urls = Column(JSONField)  # List of URLs
    members = Column(JSONField)  # List of band members
    groups = Column(JSONField)  # List of groups artist is member of
    images = Column(JSONField)  # List of images
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    release_credits = relationship("ReleaseArtist", back_populates="artist")
    
    def __repr__(self):
        return f"<Artist(id={self.id}, name='{self.name}')>"


class Label(Base):
    """Discogs label entity."""
    
    __tablename__ = 'labels'
    
    id = Column(Integer, primary_key=True)  # Discogs label ID
    name = Column(String, nullable=False)
    contact_info = Column(Text)
    profile = Column(Text)
    data_quality = Column(String)
    urls = Column(JSONField)  # List of URLs
    parent_label_id = Column(Integer, ForeignKey('labels.id'))
    subsidiaries = Column(JSONField)  # List of sublabel IDs
    images = Column(JSONField)  # List of images
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    parent = relationship("Label", remote_side=[id])
    releases = relationship("ReleaseLabel", back_populates="label")
    
    def __repr__(self):
        return f"<Label(id={self.id}, name='{self.name}')>"


class Master(Base):
    """Discogs master release entity."""
    
    __tablename__ = 'masters'
    
    id = Column(Integer, primary_key=True)  # Discogs master ID
    title = Column(String, nullable=False)
    main_release_id = Column(Integer)  # ID of main release
    year = Column(Integer)
    data_quality = Column(String)
    notes = Column(Text)
    artists = Column(JSONField)  # List of artists
    genres = Column(JSONField)  # List of genres
    styles = Column(JSONField)  # List of styles
    images = Column(JSONField)  # List of images
    videos = Column(JSONField)  # List of videos
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    releases = relationship("Release", back_populates="master")
    
    def __repr__(self):
        return f"<Master(id={self.id}, title='{self.title}')>"


class Release(Base):
    """Discogs release entity."""
    
    __tablename__ = 'releases'
    
    id = Column(Integer, primary_key=True)  # Discogs release ID
    master_id = Column(Integer, ForeignKey('masters.id'))
    title = Column(String, nullable=False)
    year = Column(Integer)
    country = Column(String)
    released = Column(Date)
    notes = Column(Text)
    data_quality = Column(String)
    status = Column(String)  # Accepted, Draft, etc.
    artists = Column(JSONField)  # List of artists on release
    extraartists = Column(JSONField)  # List of extra artists (producers, etc.)
    labels = Column(JSONField)  # List of labels
    companies = Column(JSONField)  # List of companies
    genres = Column(JSONField)  # List of genres
    styles = Column(JSONField)  # List of styles
    formats = Column(JSONField)  # List of format details
    tracklist = Column(JSONField)  # List of tracks
    identifiers = Column(JSONField)  # List of identifiers (barcode, etc.)
    images = Column(JSONField)  # List of images
    videos = Column(JSONField)  # List of videos
    estimated_weight = Column(Integer)  # Estimated shipping weight
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    master = relationship("Master", back_populates="releases")
    artist_credits = relationship("ReleaseArtist", back_populates="release")
    label_credits = relationship("ReleaseLabel", back_populates="release")
    track_listing = relationship("Track", back_populates="release")
    
    def __repr__(self):
        return f"<Release(id={self.id}, title='{self.title}')>"


class ReleaseArtist(Base):
    """Many-to-many relationship between releases and artists."""
    
    __tablename__ = 'release_artists'
    
    release_id = Column(Integer, ForeignKey('releases.id'), primary_key=True)
    artist_id = Column(Integer, ForeignKey('artists.id'), primary_key=True)
    role = Column(String, primary_key=True, default='')
    name = Column(String)  # Artist name as credited
    anv = Column(String)  # Artist name variation
    join_relation = Column(String)  # "&", "feat.", etc.
    tracks = Column(String)  # Track numbers where artist appears
    
    # Relationships
    release = relationship("Release", back_populates="artist_credits")
    artist = relationship("Artist", back_populates="release_credits")
    
    def __repr__(self):
        return f"<ReleaseArtist(release_id={self.release_id}, artist_id={self.artist_id})>"


class ReleaseLabel(Base):
    """Many-to-many relationship between releases and labels."""
    
    __tablename__ = 'release_labels'
    
    release_id = Column(Integer, ForeignKey('releases.id'), primary_key=True)
    label_id = Column(Integer, ForeignKey('labels.id'), primary_key=True)
    catalog_number = Column(String, primary_key=True, default='')
    
    # Relationships
    release = relationship("Release", back_populates="label_credits")
    label = relationship("Label", back_populates="releases")
    
    def __repr__(self):
        return f"<ReleaseLabel(release_id={self.release_id}, label_id={self.label_id})>"


class Track(Base):
    """Track listing for releases."""
    
    __tablename__ = 'tracks'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    release_id = Column(Integer, ForeignKey('releases.id'), nullable=False)
    position = Column(String)  # "A1", "1", "B2", etc.
    title = Column(String, nullable=False)
    duration = Column(String)  # "3:45" - keep for display purposes
    duration_seconds = Column(Integer)  # Duration in seconds for proper sorting
    type = Column(String)  # "track", "index", "heading"
    
    # Relationships
    release = relationship("Release", back_populates="track_listing")
    
    def __repr__(self):
        return f"<Track(id={self.id}, title='{self.title}')>"


class User(Base):
    """User account information."""
    
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    discogs_username = Column(String, unique=True, nullable=False)
    discogs_user_id = Column(Integer, unique=True)
    display_name = Column(String)
    profile_url = Column(String)
    avatar_url = Column(String)
    last_sync = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    collection = relationship("UserCollection", back_populates="user")
    folders = relationship("CollectionFolder", back_populates="user")
    sync_status = relationship("SyncStatus", back_populates="user")
    
    def __repr__(self):
        return f"<User(id={self.id}, username='{self.discogs_username}')>"


class UserCollection(Base):
    """User's collection items."""
    
    __tablename__ = 'user_collection'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    release_id = Column(Integer, nullable=False)  # Remove FK constraint for now
    folder_id = Column(Integer, default=1)  # Discogs folder ID
    instance_id = Column(Integer)  # Discogs collection instance ID
    rating = Column(Integer, CheckConstraint('rating >= 0 AND rating <= 5'))  # Allow 0 for unrated
    notes = Column(Text)
    date_added = Column(DateTime)
    basic_information = Column(JSONField)  # Cached release info
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        UniqueConstraint('user_id', 'release_id', 'instance_id'),
    )
    
    # Relationships
    user = relationship("User", back_populates="collection")
    
    def __repr__(self):
        return f"<UserCollection(user_id={self.user_id}, release_id={self.release_id})>"


class CollectionFolder(Base):
    """User's collection folders."""
    
    __tablename__ = 'collection_folders'
    
    id = Column(Integer, primary_key=True)  # Discogs folder ID
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    name = Column(String, nullable=False)
    count = Column(Integer, default=0)
    
    # Relationships
    user = relationship("User", back_populates="folders")
    
    def __repr__(self):
        return f"<CollectionFolder(id={self.id}, name='{self.name}')>"


class DataSource(Base):
    """Track data sources for records."""
    
    __tablename__ = 'data_sources'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_type = Column(String)  # 'collection', 'wantlist', etc.
    source_name = Column(String)  # 'discogs_api', 'xml_dump', etc.
    last_updated = Column(DateTime)
    source_metadata = Column(JSONField)  # Additional metadata
    
    def __repr__(self):
        return f"<DataSource(type={self.source_type}, name={self.source_name})>"


class SyncStatus(Base):
    """Track synchronization status."""
    
    __tablename__ = 'sync_status'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey('users.id'))
    sync_type = Column(String)  # 'collection', 'wantlist', 'folders'
    last_sync = Column(DateTime)
    records_processed = Column(Integer)
    status = Column(String)  # 'success', 'error', 'partial'
    error_message = Column(Text)
    
    # Relationships
    user = relationship("User", back_populates="sync_status")
    
    def __repr__(self):
        return f"<SyncStatus(user_id={self.user_id}, type='{self.sync_type}')>"