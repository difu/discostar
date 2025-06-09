"""
Duration parsing utilities for DiscoStar.
"""

import re
from typing import Optional


def parse_duration_to_seconds(duration_str: Optional[str]) -> Optional[int]:
    """Parse a duration string to total seconds.
    
    Supports formats like:
    - "3:45" (3 minutes 45 seconds)
    - "1:23:45" (1 hour 23 minutes 45 seconds)
    - "45" (45 seconds)
    - "" or None (returns None)
    
    Args:
        duration_str: Duration string to parse
        
    Returns:
        Total duration in seconds, or None if invalid/empty
    """
    if not duration_str or not duration_str.strip():
        return None
    
    # Clean the string
    duration_str = duration_str.strip()
    
    # Handle format like "3:45" or "1:23:45"
    time_pattern = r'^(\d+):(\d{2})(?::(\d{2}))?$'
    match = re.match(time_pattern, duration_str)
    
    if match:
        # Parse matched groups
        first_part = int(match.group(1))
        second_part = int(match.group(2))
        third_part = int(match.group(3)) if match.group(3) else None
        
        if third_part is not None:
            # Format: hours:minutes:seconds
            hours, minutes, seconds = first_part, second_part, third_part
            return hours * 3600 + minutes * 60 + seconds
        else:
            # Format: minutes:seconds
            minutes, seconds = first_part, second_part
            return minutes * 60 + seconds
    
    # Handle format like "45" (just seconds)
    seconds_pattern = r'^\d+$'
    if re.match(seconds_pattern, duration_str):
        return int(duration_str)
    
    # If no pattern matches, return None
    return None


def format_seconds_to_duration(seconds: Optional[int]) -> Optional[str]:
    """Format seconds back to duration string.
    
    Args:
        seconds: Total seconds
        
    Returns:
        Formatted duration string like "3:45" or None if input is None
    """
    if seconds is None:
        return None
    
    if seconds < 0:
        return None
    
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    
    if hours > 0:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    else:
        return f"{minutes}:{secs:02d}"