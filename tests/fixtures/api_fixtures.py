"""
API test fixtures for DiscoStar.

Provides mock responses for Discogs API calls and test data for API testing.
"""

import json
from typing import Dict, Any, List
from datetime import datetime


# Sample Discogs API responses for testing

SAMPLE_USER_IDENTITY = {
    "id": 123456,
    "username": "test_user",
    "resource_url": "https://api.discogs.com/users/test_user",
    "consumer_name": "DiscoStar Test"
}

SAMPLE_USER_PROFILE = {
    "id": 123456,
    "username": "test_user",
    "name": "Test User",
    "email": "test@example.com",
    "profile": "Test user profile",
    "home_page": "https://example.com",
    "location": "Test City",
    "registered": "2020-01-01T00:00:00",
    "num_collection": 200,
    "num_wantlist": 50,
    "num_for_sale": 10,
    "num_lists": 5,
    "releases_contributed": 0,
    "releases_rated": 150,
    "rating_avg": 4.2,
    "inventory_url": "https://api.discogs.com/users/test_user/inventory",
    "collection_folders_url": "https://api.discogs.com/users/test_user/collection/folders",
    "collection_url": "https://api.discogs.com/users/test_user/collection",
    "wantlist_url": "https://api.discogs.com/users/test_user/wants",
    "uri": "https://www.discogs.com/user/test_user",
    "resource_url": "https://api.discogs.com/users/test_user",
    "avatar_url": "https://img.discogs.com/user-123456.jpg"
}

SAMPLE_COLLECTION_FOLDERS = {
    "folders": [
        {
            "id": 0,
            "name": "Uncategorized",
            "count": 150,
            "resource_url": "https://api.discogs.com/users/test_user/collection/folders/0"
        },
        {
            "id": 1,
            "name": "All",
            "count": 200,
            "resource_url": "https://api.discogs.com/users/test_user/collection/folders/1"
        },
        {
            "id": 12345,
            "name": "Favorites",
            "count": 50,
            "resource_url": "https://api.discogs.com/users/test_user/collection/folders/12345"
        }
    ]
}

SAMPLE_COLLECTION_ITEM = {
    "id": 123456789,
    "instance_id": 987654321,
    "date_added": "2021-06-23T04:34:47-07:00",
    "rating": 5,
    "notes": [
        {"field_id": 1, "value": "Near Mint (NM or M-)"},
        {"field_id": 2, "value": "Near Mint (NM or M-)"},
        {"field_id": 3, "value": "First pressing"}
    ],
    "folder_id": 1,
    "basic_information": {
        "id": 4484267,
        "master_id": 534607,
        "master_url": "https://api.discogs.com/masters/534607",
        "resource_url": "https://api.discogs.com/releases/4484267",
        "title": "Test Album",
        "year": 2013,
        "formats": [
            {
                "name": "Vinyl",
                "qty": "1",
                "descriptions": ["LP", "Album"]
            }
        ],
        "labels": [
            {
                "name": "Test Records",
                "catno": "TEST001",
                "entity_type": "1",
                "entity_type_name": "Label",
                "id": 61218,
                "resource_url": "https://api.discogs.com/labels/61218"
            }
        ],
        "artists": [
            {
                "name": "Test Artist",
                "anv": "",
                "join": "",
                "role": "",
                "tracks": "",
                "id": 12368,
                "resource_url": "https://api.discogs.com/artists/12368"
            }
        ],
        "thumb": "https://img.discogs.com/test-thumb.jpg",
        "cover_image": "https://img.discogs.com/test-cover.jpg",
        "genres": ["Rock", "Electronic"],
        "styles": ["Alternative Rock", "Ambient"]
    }
}

SAMPLE_COLLECTION_RESPONSE = {
    "pagination": {
        "page": 1,
        "pages": 1,
        "per_page": 50,
        "items": 2,
        "urls": {}
    },
    "releases": [
        SAMPLE_COLLECTION_ITEM,
        {
            "id": 123456790,
            "instance_id": 987654322,
            "date_added": "2020-09-04T20:35:53-07:00",
            "rating": 0,
            "notes": [],
            "folder_id": 1,
            "basic_information": {
                "id": 393643,
                "master_id": 75754,
                "master_url": "https://api.discogs.com/masters/75754",
                "resource_url": "https://api.discogs.com/releases/393643",
                "title": "Another Test Album",
                "year": 1987,
                "formats": [
                    {
                        "name": "Vinyl",
                        "qty": "1",
                        "descriptions": ["LP", "Album", "Stereo"]
                    }
                ],
                "labels": [
                    {
                        "name": "Another Test Records",
                        "catno": "TEST002",
                        "entity_type": "1",
                        "entity_type_name": "Label",
                        "id": 902,
                        "resource_url": "https://api.discogs.com/labels/902"
                    }
                ],
                "artists": [
                    {
                        "name": "Another Test Artist",
                        "anv": "",
                        "join": "",
                        "role": "",
                        "tracks": "",
                        "id": 208220,
                        "resource_url": "https://api.discogs.com/artists/208220"
                    }
                ],
                "thumb": "https://img.discogs.com/test-thumb2.jpg",
                "cover_image": "https://img.discogs.com/test-cover2.jpg",
                "genres": ["Rock"],
                "styles": ["Blues Rock", "Hard Rock"]
            }
        }
    ]
}

SAMPLE_RELEASE_DETAILS = {
    "id": 4484267,
    "status": "Accepted",
    "year": 2013,
    "resource_url": "https://api.discogs.com/releases/4484267",
    "uri": "https://www.discogs.com/release/4484267-Test-Artist-Test-Album",
    "artists": [
        {
            "name": "Test Artist",
            "anv": "",
            "join": "",
            "role": "",
            "tracks": "",
            "id": 12368,
            "resource_url": "https://api.discogs.com/artists/12368"
        }
    ],
    "artists_sort": "Test Artist",
    "labels": [
        {
            "name": "Test Records",
            "catno": "TEST001",
            "entity_type": "1",
            "entity_type_name": "Label",
            "id": 61218,
            "resource_url": "https://api.discogs.com/labels/61218"
        }
    ],
    "companies": [],
    "formats": [
        {
            "name": "Box Set",
            "qty": "1",
            "descriptions": []
        },
        {
            "name": "Vinyl",
            "qty": "3",
            "descriptions": ["LP", "Album"]
        }
    ],
    "data_quality": "Needs Vote",
    "community": {
        "want": 245,
        "have": 789,
        "rating": {
            "count": 45,
            "average": 4.2
        }
    },
    "format_quantity": 4,
    "date_added": "2013-05-15T01:07:07-07:00",
    "date_changed": "2021-11-28T14:29:23-07:00",
    "num_for_sale": 12,
    "lowest_price": 29.99,
    "master_id": 534607,
    "master_url": "https://api.discogs.com/masters/534607",
    "title": "Test Album",
    "country": "UK",
    "released": "2013",
    "notes": "Limited edition box set",
    "released_formatted": "2013",
    "identifiers": [
        {
            "type": "Barcode",
            "value": "1234567890123"
        }
    ],
    "videos": [
        {
            "uri": "https://www.youtube.com/watch?v=test123",
            "title": "Test Video",
            "description": "Official video",
            "duration": 240,
            "embed": True
        }
    ],
    "genres": ["Rock", "Electronic"],
    "styles": ["Alternative Rock", "Ambient"],
    "tracklist": [
        {
            "position": "A1",
            "type_": "track",
            "title": "Test Track 1",
            "duration": "4:23"
        },
        {
            "position": "A2", 
            "type_": "track",
            "title": "Test Track 2",
            "duration": "3:45"
        },
        {
            "position": "B1",
            "type_": "track", 
            "title": "Test Track 3",
            "duration": "5:12"
        }
    ],
    "extraartists": [
        {
            "name": "Test Producer",
            "anv": "",
            "join": "",
            "role": "Producer",
            "tracks": "",
            "id": 54321,
            "resource_url": "https://api.discogs.com/artists/54321"
        }
    ],
    "images": [
        {
            "type": "primary",
            "uri": "https://img.discogs.com/test-image.jpg",
            "resource_url": "https://img.discogs.com/test-image.jpg",
            "uri150": "https://img.discogs.com/test-image-150.jpg",
            "width": 600,
            "height": 600
        }
    ],
    "thumb": "https://img.discogs.com/test-thumb.jpg",
    "estimated_weight": 500,
    "blocked_from_sale": False
}

SAMPLE_RATE_LIMIT_HEADERS = {
    "X-Discogs-Ratelimit": "60",
    "X-Discogs-Ratelimit-Used": "1", 
    "X-Discogs-Ratelimit-Remaining": "59"
}

SAMPLE_ERROR_RESPONSES = {
    "rate_limit": {
        "status_code": 429,
        "headers": {
            "X-Discogs-Ratelimit": "60",
            "X-Discogs-Ratelimit-Used": "60",
            "X-Discogs-Ratelimit-Remaining": "0",
            "Retry-After": "60"
        },
        "json": {
            "message": "You are making requests too quickly."
        }
    },
    "unauthorized": {
        "status_code": 401,
        "json": {
            "message": "Invalid user token."
        }
    },
    "not_found": {
        "status_code": 404,
        "json": {
            "message": "The requested resource was not found."
        }
    },
    "server_error": {
        "status_code": 500,
        "json": {
            "message": "Internal server error."
        }
    }
}


def create_mock_collection_response(
    items: List[Dict[str, Any]],
    page: int = 1,
    per_page: int = 50
) -> Dict[str, Any]:
    """Create a mock collection response with custom items.
    
    Args:
        items: List of collection items
        page: Page number
        per_page: Items per page
        
    Returns:
        Mock collection response
    """
    total_items = len(items)
    total_pages = (total_items + per_page - 1) // per_page
    
    start_idx = (page - 1) * per_page
    end_idx = min(start_idx + per_page, total_items)
    page_items = items[start_idx:end_idx]
    
    return {
        "pagination": {
            "page": page,
            "pages": total_pages,
            "per_page": per_page,
            "items": total_items,
            "urls": {}
        },
        "releases": page_items
    }


def create_mock_collection_item(
    release_id: int,
    title: str = "Test Release",
    artist_name: str = "Test Artist",
    year: int = 2020,
    rating: int = 0,
    notes: List[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Create a mock collection item.
    
    Args:
        release_id: Release ID
        title: Release title
        artist_name: Artist name
        year: Release year
        rating: User rating
        notes: User notes
        
    Returns:
        Mock collection item
    """
    if notes is None:
        notes = []
    
    return {
        "id": release_id * 10,  # Collection item ID
        "instance_id": release_id * 100,
        "date_added": "2021-01-01T00:00:00-07:00",
        "rating": rating,
        "notes": notes,
        "folder_id": 1,
        "basic_information": {
            "id": release_id,
            "master_id": release_id + 1000000,
            "master_url": f"https://api.discogs.com/masters/{release_id + 1000000}",
            "resource_url": f"https://api.discogs.com/releases/{release_id}",
            "title": title,
            "year": year,
            "formats": [{"name": "Vinyl", "qty": "1", "descriptions": ["LP", "Album"]}],
            "labels": [{"name": "Test Label", "catno": f"TEST{release_id:03d}"}],
            "artists": [{"name": artist_name, "id": release_id + 10000}],
            "thumb": f"https://img.discogs.com/thumb-{release_id}.jpg",
            "cover_image": f"https://img.discogs.com/cover-{release_id}.jpg",
            "genres": ["Rock"],
            "styles": ["Alternative Rock"]
        }
    }