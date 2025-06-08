import asyncio
import logging
import ssl
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import aiohttp

from ..utils.config import load_config


logger = logging.getLogger(__name__)


class DiscogsAPIError(Exception):
    """Base exception for Discogs API errors."""
    pass


class DiscogsRateLimitError(DiscogsAPIError):
    """Exception raised when API rate limit is exceeded."""
    pass


class DiscogsAuthenticationError(DiscogsAPIError):
    """Exception raised for authentication errors."""
    pass


class DiscogsAPIClient:
    """Asynchronous Discogs API client with rate limiting and error handling."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the Discogs API client.
        
        Args:
            config: Configuration dictionary. If None, loads from default config.
        """
        self.config = config or load_config()
        self.base_url = self.config['discogs']['api']['base_url']
        self.token = self.config['discogs']['api']['token']
        self.username = self.config['discogs']['api']['username']
        self.user_agent = self.config['discogs']['api']['user_agent']
        self.rate_limit = self.config['discogs']['api']['rate_limit']
        self.verify_ssl = self.config['discogs']['api'].get('verify_ssl', True)
        
        # Rate limiting state
        self._last_request_time = 0.0
        self._requests_this_minute = 0
        self._minute_start = time.time()
        
        # Session will be created when needed
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self._ensure_session()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
    
    async def _ensure_session(self):
        """Ensure aiohttp session is created."""
        if self._session is None:
            headers = {
                'User-Agent': self.user_agent,
                'Authorization': f'Discogs token={self.token}'
            }
            
            # Create SSL context that handles certificate verification issues
            ssl_context = ssl.create_default_context()
            
            # Allow disabling SSL verification for development environments
            if not self.verify_ssl:
                logger.warning("SSL verification disabled - only use for development!")
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
            
            # Create connector with SSL context
            connector = aiohttp.TCPConnector(
                ssl=ssl_context,
                limit=30,
                limit_per_host=10
            )
            
            self._session = aiohttp.ClientSession(
                headers=headers,
                connector=connector,
                timeout=aiohttp.ClientTimeout(total=30, connect=10)
            )
    
    async def close(self):
        """Close the aiohttp session."""
        if self._session:
            await self._session.close()
            self._session = None
    
    async def _wait_for_rate_limit(self):
        """Wait if necessary to respect rate limits."""
        current_time = time.time()
        
        # Reset counter if a minute has passed
        if current_time - self._minute_start >= 60:
            self._requests_this_minute = 0
            self._minute_start = current_time
        
        # If we've hit the rate limit, wait
        if self._requests_this_minute >= self.rate_limit:
            wait_time = 60 - (current_time - self._minute_start)
            if wait_time > 0:
                logger.info(f"Rate limit reached, waiting {wait_time:.1f} seconds")
                await asyncio.sleep(wait_time)
                self._requests_this_minute = 0
                self._minute_start = time.time()
        
        # Ensure minimum time between requests (1 second)
        time_since_last = current_time - self._last_request_time
        if time_since_last < 1.0:
            await asyncio.sleep(1.0 - time_since_last)
    
    async def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make a request to the Discogs API.
        
        Args:
            endpoint: API endpoint path
            params: Query parameters
            
        Returns:
            JSON response data
            
        Raises:
            DiscogsAPIError: For API errors
            DiscogsRateLimitError: For rate limit errors
            DiscogsAuthenticationError: For authentication errors
        """
        await self._ensure_session()
        await self._wait_for_rate_limit()
        
        url = urljoin(self.base_url, endpoint)
        
        try:
            self._last_request_time = time.time()
            self._requests_this_minute += 1
            
            async with self._session.get(url, params=params) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 401:
                    raise DiscogsAuthenticationError("Invalid API token or unauthorized access")
                elif response.status == 429:
                    raise DiscogsRateLimitError("Rate limit exceeded")
                else:
                    error_msg = f"API request failed with status {response.status}"
                    try:
                        error_data = await response.json()
                        if 'message' in error_data:
                            error_msg += f": {error_data['message']}"
                    except:
                        pass
                    raise DiscogsAPIError(error_msg)
        
        except aiohttp.ClientError as e:
            raise DiscogsAPIError(f"Network error: {e}")
    
    async def get_user_identity(self) -> Dict[str, Any]:
        """Get the authenticated user's identity.
        
        Returns:
            User identity data
        """
        return await self._make_request('/oauth/identity')
    
    async def get_user_collection(self, username: Optional[str] = None, 
                                 folder_id: int = 0, page: int = 1, 
                                 per_page: int = 100) -> Dict[str, Any]:
        """Get user's collection.
        
        Args:
            username: Username (defaults to configured username)
            folder_id: Collection folder ID (0 = All)
            page: Page number (1-based)
            per_page: Items per page (max 100)
            
        Returns:
            Collection data with pagination info
        """
        username = username or self.username
        endpoint = f'/users/{username}/collection/folders/{folder_id}/releases'
        params = {
            'page': page,
            'per_page': min(per_page, 100)
        }
        
        return await self._make_request(endpoint, params)
    
    async def get_all_collection_items(self, username: Optional[str] = None, 
                                     folder_id: int = 0) -> List[Dict[str, Any]]:
        """Get all items from user's collection.
        
        Args:
            username: Username (defaults to configured username)
            folder_id: Collection folder ID (0 = All)
            
        Returns:
            List of all collection items
        """
        username = username or self.username
        all_items = []
        page = 1
        
        logger.info(f"Fetching collection for user {username}")
        
        while True:
            data = await self.get_user_collection(username, folder_id, page, 100)
            items = data.get('releases', [])
            
            if not items:
                break
            
            all_items.extend(items)
            logger.info(f"Fetched page {page}, total items: {len(all_items)}")
            
            # Check if there are more pages
            pagination = data.get('pagination', {})
            if page >= pagination.get('pages', 1):
                break
            
            page += 1
        
        logger.info(f"Completed collection fetch: {len(all_items)} total items")
        return all_items
    
    async def get_user_wantlist(self, username: Optional[str] = None, 
                               page: int = 1, per_page: int = 100) -> Dict[str, Any]:
        """Get user's wantlist.
        
        Args:
            username: Username (defaults to configured username)
            page: Page number (1-based)
            per_page: Items per page (max 100)
            
        Returns:
            Wantlist data with pagination info
        """
        username = username or self.username
        endpoint = f'/users/{username}/wants'
        params = {
            'page': page,
            'per_page': min(per_page, 100)
        }
        
        return await self._make_request(endpoint, params)
    
    async def get_all_wantlist_items(self, username: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get all items from user's wantlist.
        
        Args:
            username: Username (defaults to configured username)
            
        Returns:
            List of all wantlist items
        """
        username = username or self.username
        all_items = []
        page = 1
        
        logger.info(f"Fetching wantlist for user {username}")
        
        while True:
            data = await self.get_user_wantlist(username, page, 100)
            items = data.get('wants', [])
            
            if not items:
                break
            
            all_items.extend(items)
            logger.info(f"Fetched page {page}, total items: {len(all_items)}")
            
            # Check if there are more pages
            pagination = data.get('pagination', {})
            if page >= pagination.get('pages', 1):
                break
            
            page += 1
        
        logger.info(f"Completed wantlist fetch: {len(all_items)} total items")
        return all_items
    
    async def get_release(self, release_id: int) -> Dict[str, Any]:
        """Get release details by ID.
        
        Args:
            release_id: Discogs release ID
            
        Returns:
            Release data
        """
        endpoint = f'/releases/{release_id}'
        return await self._make_request(endpoint)
    
    async def get_master(self, master_id: int) -> Dict[str, Any]:
        """Get master release details by ID.
        
        Args:
            master_id: Discogs master release ID
            
        Returns:
            Master release data
        """
        endpoint = f'/masters/{master_id}'
        return await self._make_request(endpoint)
    
    async def get_artist(self, artist_id: int) -> Dict[str, Any]:
        """Get artist details by ID.
        
        Args:
            artist_id: Discogs artist ID
            
        Returns:
            Artist data
        """
        endpoint = f'/artists/{artist_id}'
        return await self._make_request(endpoint)
    
    async def get_label(self, label_id: int) -> Dict[str, Any]:
        """Get label details by ID.
        
        Args:
            label_id: Discogs label ID
            
        Returns:
            Label data
        """
        endpoint = f'/labels/{label_id}'
        return await self._make_request(endpoint)