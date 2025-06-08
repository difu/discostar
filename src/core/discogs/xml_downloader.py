import asyncio
import logging
import re
import ssl
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
        
        # Dump types
        self.dump_types = ['artists', 'releases', 'labels', 'masters']
    
    async def download_dump(self, dump_type: str, force_download: bool = False) -> bool:
        """Download a specific dump type.
        
        Args:
            dump_type: Type of dump to download (artists, releases, labels, masters)
            force_download: Whether to force re-download existing files
            
        Returns:
            True if download was successful, False otherwise
        """
        if dump_type not in self.dump_types:
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
            # Create SSL context that handles certificate issues
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            
            connector = aiohttp.TCPConnector(ssl=ssl_context)
            async with aiohttp.ClientSession(connector=connector) as session:
                # Get the current year first, then check previous year if needed
                current_year = datetime.now().year
                years_to_check = [current_year, current_year - 1]
                
                for year in years_to_check:
                    data_url = urljoin(self.base_url, f'data/{year}/')
                    
                    async with session.get(data_url) as response:
                        if response.status != 200:
                            logger.debug(f"Failed to access {year} directory: {response.status}")
                            continue
                        
                        content = await response.text()
                        
                        # Look for files matching: discogs_YYYYMMDD_[type].xml.gz
                        pattern = rf'discogs_(\d{{4}})(\d{{2}})(\d{{2}})_{re.escape(dump_type)}\.xml\.gz'
                        matches = re.findall(pattern, content)
                        
                        if matches:
                            # Sort by date and get the most recent
                            latest_date = max(matches, key=lambda x: (x[0], x[1], x[2]))
                            date_str = ''.join(latest_date)
                            filename = f'discogs_{date_str}_{dump_type}.xml.gz'
                            
                            latest_url = urljoin(self.base_url, f'data/{year}/{filename}')
                            logger.info(f"Found latest {dump_type} dump: {latest_url}")
                            return latest_url
                
                # If no files found in year directories, try direct access for recent dates
                today = datetime.now()
                for days_back in range(30):  # Try last 30 days
                    date = today - timedelta(days=days_back)
                    year = date.year
                    date_str = date.strftime('%Y%m%d')
                    filename = f'discogs_{date_str}_{dump_type}.xml.gz'
                    test_url = urljoin(self.base_url, f'data/{year}/{filename}')
                    
                    if await self._url_exists(test_url):
                        logger.info(f"Found {dump_type} dump via direct access: {test_url}")
                        return test_url
                
                logger.error(f"No recent {dump_type} dump files found")
                return None
                    
        except Exception as e:
            logger.error(f"Error finding latest {dump_type} dump URL: {e}")
            return None
    
    async def _url_exists(self, url: str) -> bool:
        """Check if a URL exists by making a HEAD request.
        
        Args:
            url: URL to check
            
        Returns:
            True if URL exists, False otherwise
        """
        try:
            # Create SSL context that handles certificate issues
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            
            connector = aiohttp.TCPConnector(ssl=ssl_context)
            async with aiohttp.ClientSession(connector=connector) as session:
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
            # Create SSL context that handles certificate issues
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            
            connector = aiohttp.TCPConnector(limit=10, ssl=ssl_context)
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
        
        for dump_type in self.dump_types:
            # Look for any files matching the pattern: discogs_YYYYMMDD_[type].xml.gz
            pattern = f"discogs_*_{dump_type}.xml.gz"
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


