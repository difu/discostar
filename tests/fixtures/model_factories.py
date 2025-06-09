"""
Model factories for creating test data.

Provides factory functions to create test instances of database models
with realistic sample data.
"""

import random
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

from src.core.database.models import (
    User, UserCollection, Artist, Label, Master, Release,
    ReleaseArtist, ReleaseLabel, Track, CollectionFolder,
    DataSource, SyncStatus
)


def create_test_user(
    discogs_username: str = "test_user",
    discogs_user_id: Optional[int] = None,
    display_name: Optional[str] = None
) -> User:
    """Create a test user.
    
    Args:
        discogs_username: Discogs username
        discogs_user_id: Discogs user ID
        display_name: Display name
        
    Returns:
        User instance
    """
    if discogs_user_id is None:
        discogs_user_id = random.randint(100000, 999999)
    
    if display_name is None:
        display_name = discogs_username.replace('_', ' ').title()
    
    return User(
        discogs_username=discogs_username,
        discogs_user_id=discogs_user_id,
        display_name=display_name,
        profile_url=f"https://www.discogs.com/user/{discogs_username}",
        avatar_url=f"https://img.discogs.com/user-{discogs_user_id}.jpg",
        last_sync=datetime.utcnow() - timedelta(days=1),
        created_at=datetime.utcnow() - timedelta(days=30)
    )


def create_test_artist(
    artist_id: Optional[int] = None,
    name: str = "Test Artist",
    real_name: Optional[str] = None,
    profile: Optional[str] = None
) -> Artist:
    """Create a test artist.
    
    Args:
        artist_id: Artist ID
        name: Artist name
        real_name: Real name
        profile: Artist profile/biography
        
    Returns:
        Artist instance
    """
    if artist_id is None:
        artist_id = random.randint(1000, 99999)
    
    return Artist(
        id=artist_id,
        name=name,
        real_name=real_name,
        profile=profile or f"Biography of {name}",
        data_quality="Correct",
        name_variations=[f"{name} (Alternative)", f"{name} Variant"],
        aliases=[f"{name} Alias"],
        urls=["https://example.com/artist"],
        images=[{"type": "primary", "uri": "https://img.discogs.com/test.jpg"}]
    )


def create_test_label(
    label_id: Optional[int] = None,
    name: str = "Test Label",
    contact_info: Optional[str] = None,
    profile: Optional[str] = None
) -> Label:
    """Create a test label.
    
    Args:
        label_id: Label ID
        name: Label name
        contact_info: Contact information
        profile: Label profile
        
    Returns:
        Label instance
    """
    if label_id is None:
        label_id = random.randint(1000, 99999)
    
    return Label(
        id=label_id,
        name=name,
        contact_info=contact_info or "contact@testlabel.com",
        profile=profile or f"Profile of {name}",
        data_quality="Correct",
        urls=["https://testlabel.com"]
    )


def create_test_master(
    master_id: Optional[int] = None,
    title: str = "Test Album",
    main_release_id: Optional[int] = None,
    year: Optional[int] = None
) -> Master:
    """Create a test master release.
    
    Args:
        master_id: Master ID
        title: Album title
        main_release_id: Main release ID
        year: Release year
        
    Returns:
        Master instance
    """
    if master_id is None:
        master_id = random.randint(1000, 99999)
    
    if year is None:
        year = random.randint(1960, 2024)
    
    return Master(
        id=master_id,
        title=title,
        main_release_id=main_release_id,
        year=year,
        data_quality="Correct",
        notes=f"Notes for {title}",
        artists=[{"id": 1, "name": "Test Artist"}],
        genres=["Rock", "Pop"],
        styles=["Alternative Rock", "Indie Pop"]
    )


def create_test_release(
    release_id: Optional[int] = None,
    master_id: Optional[int] = None,
    title: str = "Test Release",
    year: Optional[int] = None,
    country: str = "US"
) -> Release:
    """Create a test release.
    
    Args:
        release_id: Release ID
        master_id: Master release ID
        title: Release title
        year: Release year
        country: Country code
        
    Returns:
        Release instance
    """
    if release_id is None:
        release_id = random.randint(1000, 99999)
    
    if year is None:
        year = random.randint(1960, 2024)
    
    return Release(
        id=release_id,
        master_id=master_id,
        title=title,
        year=year,
        country=country,
        data_quality="Correct",
        status="Accepted",
        artists=[{"id": 1, "name": "Test Artist", "role": ""}],
        labels=[{"id": 1, "name": "Test Label", "catno": "TEST001"}],
        genres=["Rock"],
        styles=["Alternative Rock"],
        formats=[{"name": "Vinyl", "qty": "1", "descriptions": ["LP", "Album"]}],
        tracklist=[
            {"position": "A1", "title": "Track 1", "duration": "3:45"},
            {"position": "A2", "title": "Track 2", "duration": "4:12"},
            {"position": "B1", "title": "Track 3", "duration": "3:33"}
        ]
    )


def create_test_collection_item(
    user_id: int,
    release_id: int,
    rating: Optional[int] = None,
    notes: Optional[str] = None,
    folder_id: int = 1
) -> UserCollection:
    """Create a test collection item.
    
    Args:
        user_id: User ID
        release_id: Release ID
        rating: User rating (0-5)
        notes: User notes
        folder_id: Collection folder ID
        
    Returns:
        UserCollection instance
    """
    if rating is None:
        rating = random.choice([0, 3, 4, 5])  # Realistic rating distribution
    
    basic_info = {
        "id": release_id,
        "title": "Test Release",
        "year": 2020,
        "artists": [{"name": "Test Artist"}],
        "labels": [{"name": "Test Label", "catno": "TEST001"}],
        "formats": [{"name": "Vinyl", "qty": "1"}],
        "thumb": "https://img.discogs.com/test-thumb.jpg"
    }
    
    return UserCollection(
        user_id=user_id,
        release_id=release_id,
        folder_id=folder_id,
        instance_id=random.randint(100000000, 999999999),
        rating=rating,
        notes=notes,
        date_added=datetime.utcnow() - timedelta(days=random.randint(1, 365)),
        basic_information=basic_info
    )


def create_test_track(
    release_id: int,
    position: str = "A1",
    title: str = "Test Track",
    duration: str = "3:45"
) -> Track:
    """Create a test track.
    
    Args:
        release_id: Release ID
        position: Track position
        title: Track title
        duration: Track duration
        
    Returns:
        Track instance
    """
    # Convert duration to seconds
    duration_parts = duration.split(':')
    duration_seconds = int(duration_parts[0]) * 60 + int(duration_parts[1])
    
    return Track(
        release_id=release_id,
        position=position,
        title=title,
        duration=duration,
        duration_seconds=duration_seconds,
        type="track"
    )


def create_test_collection_folder(
    folder_id: int,
    user_id: int,
    name: str = "Test Folder",
    count: int = 0
) -> CollectionFolder:
    """Create a test collection folder.
    
    Args:
        folder_id: Folder ID
        user_id: User ID
        name: Folder name
        count: Item count in folder
        
    Returns:
        CollectionFolder instance
    """
    return CollectionFolder(
        id=folder_id,
        user_id=user_id,
        name=name,
        count=count
    )


def create_complete_test_collection(
    user: User,
    num_items: int = 10
) -> List[Dict[str, Any]]:
    """Create a complete test collection with related data.
    
    Args:
        user: User instance
        num_items: Number of collection items to create
        
    Returns:
        Dictionary containing all created test data
    """
    artists = []
    labels = []
    masters = []
    releases = []
    collection_items = []
    tracks = []
    
    for i in range(num_items):
        # Create related entities
        artist = create_test_artist(
            artist_id=1000 + i,
            name=f"Artist {i+1}"
        )
        artists.append(artist)
        
        label = create_test_label(
            label_id=2000 + i,
            name=f"Label {i+1}"
        )
        labels.append(label)
        
        master = create_test_master(
            master_id=3000 + i,
            title=f"Album {i+1}",
            year=2000 + (i % 24)
        )
        masters.append(master)
        
        release = create_test_release(
            release_id=4000 + i,
            master_id=master.id,
            title=f"Release {i+1}",
            year=master.year
        )
        releases.append(release)
        
        # Create collection item
        collection_item = create_test_collection_item(
            user_id=user.id,
            release_id=release.id,
            rating=random.choice([0, 3, 4, 5]),
            notes=f"Notes for release {i+1}" if i % 3 == 0 else None
        )
        collection_items.append(collection_item)
        
        # Create tracks
        for j in range(3):  # 3 tracks per release
            track = create_test_track(
                release_id=release.id,
                position=f"A{j+1}",
                title=f"Track {j+1}",
                duration=f"{random.randint(2,5)}:{random.randint(10,59):02d}"
            )
            tracks.append(track)
    
    return {
        "user": user,
        "artists": artists,
        "labels": labels,
        "masters": masters,
        "releases": releases,
        "collection_items": collection_items,
        "tracks": tracks
    }