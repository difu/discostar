import asyncio
import logging
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional
from urllib.parse import urljoin

import aiohttp
import click

from ..utils.config import get_dumps_directory


logger = logging.getLogger(__name__)


class DiscogsDumpDownloader:
    """Handles downloading Discogs XML database dumps."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the downloader with configuration.
        
        Args:
            config: Application configuration dictionary
        """
        self.config = config
        self.base_url = config.get('discogs', {}).get('xml_dumps', {}).get('base_url', 
                                                      'https://discogs-data-dumps.s3-us-west-2.amazonaws.com/')
        self.dumps_dir = get_dumps_directory()
        
        # File mappings
        self.dump_files = {
            'artists': 'discogs_artists.xml.gz',
            'releases': 'discogs_releases.xml.gz',
            'labels': 'discogs_labels.xml.gz',
            'masters': 'discogs_masters.xml.gz'
        }
    
    async def download_dump(self, dump_type: str, force_download: bool = False) -> bool:
        """Download a specific dump type.
        
        Args:
            dump_type: Type of dump to download (artists, releases, labels, masters)
            force_download: Whether to force re-download existing files
            
        Returns:
            True if download was successful, False otherwise
        """
        if dump_type not in self.dump_files:
            logger.error(f"Unknown dump type: {dump_type}")
            return False
        
        try:
            # Get the latest dump file URL
            latest_url = await self._get_latest_dump_url(dump_type)
            if not latest_url:
                logger.error(f"Could not find latest {dump_type} dump URL")
                return False
            
            # Extract filename from URL
            filename = latest_url.split('/')[-1]
            local_path = self.dumps_dir / filename
            
            # Check if file already exists
            if local_path.exists() and not force_download:
                logger.info(f"File {filename} already exists, skipping download")
                click.echo(f"✓ {filename} already exists (use --force to re-download)")
                return True
            
            # Download the file
            success = await self._download_file(latest_url, local_path)
            return success
            
        except Exception as e:
            logger.error(f"Error downloading {dump_type} dump: {e}")
            return False
    
    async def _get_latest_dump_url(self, dump_type: str) -> Optional[str]:
        """Get the URL for the latest dump file of the specified type.
        
        Args:
            dump_type: Type of dump to find URL for
            
        Returns:
            URL string if found, None otherwise
        """
        try:
            async with aiohttp.ClientSession() as session:
                # First, try to get the directory listing
                async with session.get(self.base_url) as response:
                    if response.status != 200:
                        logger.error(f"Failed to access dump directory: {response.status}")
                        return None
                    
                    content = await response.text()
                    
                    # Look for the most recent dated file for this dump type
                    pattern = rf'(\d{{4}})(\d{{2}})(\d{{2}})/{re.escape(self.dump_files[dump_type])}'
                    matches = re.findall(pattern, content)
                    
                    if not matches:
                        # Fallback: try direct file access with today's date format
                        from datetime import datetime
                        today = datetime.now()
                        for days_back in range(7):  # Try last 7 days
                            date = today - timedelta(days=days_back)
                            date_str = date.strftime('%Y%m%d')
                            test_url = urljoin(self.base_url, f'{date_str}/{self.dump_files[dump_type]}')
                            
                            if await self._url_exists(session, test_url):
                                return test_url
                        
                        logger.error(f"No recent {dump_type} dump files found")
                        return None
                    
                    # Sort by date (year, month, day) and get the most recent
                    latest_date = max(matches, key=lambda x: (x[0], x[1], x[2]))
                    date_str = ''.join(latest_date)
                    
                    latest_url = urljoin(self.base_url, f'{date_str}/{self.dump_files[dump_type]}')
                    logger.info(f"Found latest {dump_type} dump: {latest_url}")
                    return latest_url
                    
        except Exception as e:
            logger.error(f"Error finding latest {dump_type} dump URL: {e}")
            return None
    
    async def _url_exists(self, session: aiohttp.ClientSession, url: str) -> bool:
        """Check if a URL exists by making a HEAD request.
        
        Args:
            session: aiohttp session to use
            url: URL to check
            
        Returns:
            True if URL exists, False otherwise
        """
        try:
            async with session.head(url) as response:
                return response.status == 200
        except:
            return False
    
    async def _download_file(self, url: str, local_path: Path) -> bool:
        """Download a file from URL to local path with progress tracking.
        
        Args:
            url: URL to download from
            local_path: Local path to save file to
            
        Returns:
            True if download was successful, False otherwise
        """
        try:
            connector = aiohttp.TCPConnector(limit=10)
            timeout = aiohttp.ClientTimeout(total=3600)  # 1 hour timeout
            
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                async with session.get(url) as response:
                    if response.status != 200:
                        logger.error(f"HTTP {response.status} when downloading {url}")
                        return False
                    
                    # Get file size for progress tracking
                    total_size = int(response.headers.get('content-length', 0))
                    
                    # Setup progress bar
                    with click.progressbar(
                        length=total_size,
                        label=f"Downloading {local_path.name}",
                        show_eta=True,
                        show_percent=True
                    ) as bar:
                        
                        with open(local_path, 'wb') as f:
                            downloaded = 0
                            async for chunk in response.content.iter_chunked(8192):
                                f.write(chunk)
                                downloaded += len(chunk)
                                bar.update(len(chunk))
                    
                    logger.info(f"Successfully downloaded {local_path.name} ({total_size:,} bytes)")
                    return True
                    
        except asyncio.TimeoutError:
            logger.error(f"Timeout downloading {url}")
            # Clean up partial file
            if local_path.exists():
                local_path.unlink()
            return False
        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            # Clean up partial file
            if local_path.exists():
                local_path.unlink()
            return False
    
    def get_downloaded_dumps(self) -> Dict[str, Path]:
        """Get list of already downloaded dump files.
        
        Returns:
            Dictionary mapping dump types to their local file paths
        """
        downloaded = {}
        
        for dump_type, base_filename in self.dump_files.items():
            # Look for any files matching the pattern (with date prefix)
            pattern = f"*{base_filename}"
            matches = list(self.dumps_dir.glob(pattern))
            
            if matches:
                # Get the most recent one
                latest_file = max(matches, key=lambda p: p.stat().st_mtime)
                downloaded[dump_type] = latest_file
        
        return downloaded
    
    def get_dump_info(self, dump_type: str) -> Optional[Dict[str, Any]]:
        """Get information about a downloaded dump file.
        
        Args:
            dump_type: Type of dump to get info for
            
        Returns:
            Dictionary with file info, or None if not found
        """
        downloaded = self.get_downloaded_dumps()
        
        if dump_type not in downloaded:
            return None
        
        file_path = downloaded[dump_type]
        stat = file_path.stat()
        
        return {
            'path': file_path,
            'size': stat.st_size,
            'modified': stat.st_mtime,
            'exists': True
        }


