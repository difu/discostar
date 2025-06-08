"""
XML parsing utilities for Discogs database dumps.
"""

import gzip
import logging
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any, Optional, Iterator, List, Callable
from datetime import datetime

import click

from ..database.models import Artist, Release, Label, Master


logger = logging.getLogger(__name__)


class BaseXMLParser(ABC):
    """Base class for XML dump parsers."""
    
    def __init__(self, file_path: Path, progress_callback: Optional[Callable[[int], None]] = None):
        """Initialize parser with file path.
        
        Args:
            file_path: Path to the XML dump file (can be gzipped)
            progress_callback: Optional callback function for progress updates
        """
        self.file_path = file_path
        self.total_records = 0
        self.processed_records = 0
        self.error_count = 0
        self.progress_callback = progress_callback
        
    def _open_file(self):
        """Open the XML file, handling gzip compression."""
        if self.file_path.suffix == '.gz':
            return gzip.open(self.file_path, 'rt', encoding='utf-8')
        else:
            return open(self.file_path, 'r', encoding='utf-8')
    
    def _safe_text(self, element: Optional[ET.Element]) -> Optional[str]:
        """Safely extract text from XML element."""
        if element is not None and element.text:
            return element.text.strip()
        return None
    
    def _safe_int(self, element: Optional[ET.Element]) -> Optional[int]:
        """Safely extract integer from XML element."""
        text = self._safe_text(element)
        if text:
            try:
                return int(text)
            except ValueError:
                return None
        return None
    
    def _extract_urls(self, parent: ET.Element) -> List[str]:
        """Extract URLs from parent element."""
        urls = []
        urls_elem = parent.find('urls')
        if urls_elem is not None:
            for url_elem in urls_elem.findall('url'):
                url = self._safe_text(url_elem)
                if url:
                    urls.append(url)
        return urls
    
    def _extract_name_variations(self, parent: ET.Element) -> List[str]:
        """Extract name variations from parent element."""
        variations = []
        namevars_elem = parent.find('namevariations')
        if namevars_elem is not None:
            for name_elem in namevars_elem.findall('name'):
                name = self._safe_text(name_elem)
                if name:
                    variations.append(name)
        return variations
    
    def _extract_aliases(self, parent: ET.Element) -> List[Dict[str, Any]]:
        """Extract aliases from parent element."""
        aliases = []
        aliases_elem = parent.find('aliases')
        if aliases_elem is not None:
            for alias_elem in aliases_elem.findall('name'):
                alias_id = alias_elem.get('id')
                alias_name = self._safe_text(alias_elem)
                if alias_name:
                    alias_data = {'name': alias_name}
                    if alias_id:
                        alias_data['id'] = int(alias_id)
                    aliases.append(alias_data)
        return aliases
    
    def _extract_genres_styles(self, parent: ET.Element) -> tuple[List[str], List[str]]:
        """Extract genres and styles from parent element."""
        genres = []
        styles = []
        
        genres_elem = parent.find('genres')
        if genres_elem is not None:
            for genre_elem in genres_elem.findall('genre'):
                genre = self._safe_text(genre_elem)
                if genre:
                    genres.append(genre)
        
        styles_elem = parent.find('styles')
        if styles_elem is not None:
            for style_elem in styles_elem.findall('style'):
                style = self._safe_text(style_elem)
                if style:
                    styles.append(style)
        
        return genres, styles
    
    @abstractmethod
    def parse_record(self, element: ET.Element) -> Optional[Any]:
        """Parse a single record from XML element.
        
        Args:
            element: XML element representing a record
            
        Returns:
            Parsed record object or None if parsing failed
        """
        pass
    
    def parse_file(self) -> Iterator[Any]:
        """Parse the entire XML file and yield records.
        
        Yields:
            Parsed record objects
        """
        logger.info(f"Starting to parse {self.file_path}")
        
        with self._open_file() as f:
            try:
                # Use iterparse for memory-efficient parsing of large files
                for event, elem in ET.iterparse(f, events=('start', 'end')):
                    if event == 'end' and elem.tag in self.get_record_tags():
                        try:
                            record = self.parse_record(elem)
                            if record:
                                self.processed_records += 1
                                yield record
                            else:
                                self.error_count += 1
                        except Exception as e:
                            self.error_count += 1
                            logger.debug(f"Error parsing record: {e}")
                        
                        # Clear the element to free memory
                        elem.clear()
                        
                        # Report progress
                        total_seen = self.processed_records + self.error_count
                        if self.progress_callback and total_seen % 1000 == 0:
                            self.progress_callback(self.processed_records)
                        
                        # Log progress every 10000 records
                        if total_seen % 10000 == 0:
                            logger.info(f"Processed {self.processed_records:,} records, {self.error_count} errors")
                            
            except ET.ParseError as e:
                logger.error(f"XML parsing error in {self.file_path}: {e}")
                raise
            except Exception as e:
                logger.error(f"Error parsing {self.file_path}: {e}")
                raise
        
        logger.info(f"Finished parsing {self.file_path}. Total records: {self.processed_records:,}, Errors: {self.error_count}")
    
    @abstractmethod
    def get_record_tags(self) -> List[str]:
        """Get the XML tag names that represent records.
        
        Returns:
            List of tag names
        """
        pass


class ArtistXMLParser(BaseXMLParser):
    """Parser for Discogs artists XML dump."""
    
    def get_record_tags(self) -> List[str]:
        return ['artist']
    
    def parse_record(self, element: ET.Element) -> Optional[Artist]:
        """Parse an artist record from XML element."""
        try:
            # Extract basic information
            artist_id = self._safe_int(element.find('id'))
            if not artist_id:
                return None
            
            name = self._safe_text(element.find('name'))
            if not name:
                return None
            
            real_name = self._safe_text(element.find('realname'))
            profile = self._safe_text(element.find('profile'))
            data_quality = self._safe_text(element.find('data_quality'))
            
            # Extract collections
            urls = self._extract_urls(element)
            name_variations = self._extract_name_variations(element)
            aliases = self._extract_aliases(element)
            
            # Create Artist object
            artist = Artist(
                id=artist_id,
                name=name,
                real_name=real_name,
                profile=profile,
                data_quality=data_quality,
                urls=urls if urls else None,
                name_variations=name_variations if name_variations else None,
                aliases=aliases if aliases else None
            )
            
            return artist
            
        except Exception as e:
            logger.warning(f"Error parsing artist record: {e}")
            return None


class LabelXMLParser(BaseXMLParser):
    """Parser for Discogs labels XML dump."""
    
    def get_record_tags(self) -> List[str]:
        return ['label']
    
    def parse_record(self, element: ET.Element) -> Optional[Label]:
        """Parse a label record from XML element."""
        try:
            # Extract basic information
            label_id = self._safe_int(element.find('id'))
            if not label_id:
                return None
            
            name = self._safe_text(element.find('name'))
            if not name:
                return None
            
            contact_info = self._safe_text(element.find('contactinfo'))
            profile = self._safe_text(element.find('profile'))
            data_quality = self._safe_text(element.find('data_quality'))
            
            # Extract parent label - get ID from attribute
            parent_label = None
            parent_label_elem = element.find('parentLabel')
            if parent_label_elem is not None:
                parent_id = parent_label_elem.get('id')
                if parent_id:
                    try:
                        parent_label = int(parent_id)
                    except ValueError:
                        pass
            
            # Extract sublabels - get IDs from attributes
            sublabels = []
            sublabels_elem = element.find('sublabels')
            if sublabels_elem is not None:
                for sublabel_elem in sublabels_elem.findall('label'):
                    sublabel_id = sublabel_elem.get('id')
                    if sublabel_id:
                        try:
                            sublabels.append(int(sublabel_id))
                        except ValueError:
                            pass
            
            # Extract URLs
            urls = self._extract_urls(element)
            
            # Create Label object
            label = Label(
                id=label_id,
                name=name,
                contact_info=contact_info,
                profile=profile,
                data_quality=data_quality,
                parent_label_id=parent_label,
                subsidiaries=sublabels if sublabels else None,
                urls=urls if urls else None
            )
            
            return label
            
        except Exception as e:
            logger.warning(f"Error parsing label record: {e}")
            return None


class MasterXMLParser(BaseXMLParser):
    """Parser for Discogs masters XML dump."""
    
    def get_record_tags(self) -> List[str]:
        return ['master']
    
    def parse_record(self, element: ET.Element) -> Optional[Master]:
        """Parse a master record from XML element."""
        try:
            # Extract master ID from attribute
            master_id_str = element.get('id')
            if not master_id_str:
                return None
            
            try:
                master_id = int(master_id_str)
            except ValueError:
                return None
            
            title = self._safe_text(element.find('title'))
            if not title:
                return None
            
            main_release_id = self._safe_int(element.find('main_release'))
            year = self._safe_int(element.find('year'))
            data_quality = self._safe_text(element.find('data_quality'))
            notes = self._safe_text(element.find('notes'))
            
            # Extract genres and styles
            genres, styles = self._extract_genres_styles(element)
            
            # Create Master object
            master = Master(
                id=master_id,
                title=title,
                main_release_id=main_release_id,
                year=year,
                data_quality=data_quality,
                notes=notes,
                genres=genres if genres else None,
                styles=styles if styles else None
            )
            
            return master
            
        except Exception as e:
            logger.warning(f"Error parsing master record: {e}")
            return None


class ReleaseXMLParser(BaseXMLParser):
    """Parser for Discogs releases XML dump."""
    
    def get_record_tags(self) -> List[str]:
        return ['release']
    
    def parse_record(self, element: ET.Element) -> Optional[Release]:
        """Parse a release record from XML element."""
        try:
            # Extract release ID from attribute
            release_id_str = element.get('id')
            if not release_id_str:
                return None
            
            try:
                release_id = int(release_id_str)
            except ValueError:
                return None
            
            title = self._safe_text(element.find('title'))
            if not title:
                return None
            
            master_id = self._safe_int(element.find('master_id'))
            year = self._safe_int(element.find('year'))
            country = self._safe_text(element.find('country'))
            notes = self._safe_text(element.find('notes'))
            data_quality = self._safe_text(element.find('data_quality'))
            status = element.get('status')  # Status is an attribute
            
            # Parse released date
            released = None
            released_elem = element.find('released')
            if released_elem is not None:
                released_text = self._safe_text(released_elem)
                if released_text:
                    try:
                        # Try to parse various date formats
                        for fmt in ['%Y-%m-%d', '%Y-%m', '%Y']:
                            try:
                                released = datetime.strptime(released_text, fmt).date()
                                break
                            except ValueError:
                                continue
                    except Exception:
                        pass
            
            # Extract genres and styles
            genres, styles = self._extract_genres_styles(element)
            
            # Extract formats
            formats = []
            formats_elem = element.find('formats')
            if formats_elem is not None:
                for format_elem in formats_elem.findall('format'):
                    format_data = {
                        'name': format_elem.get('name'),
                        'qty': format_elem.get('qty'),
                        'text': format_elem.get('text')
                    }
                    
                    # Extract descriptions
                    descriptions = []
                    for desc_elem in format_elem.findall('description'):
                        desc = self._safe_text(desc_elem)
                        if desc:
                            descriptions.append(desc)
                    
                    if descriptions:
                        format_data['descriptions'] = descriptions
                    
                    formats.append(format_data)
            
            # Create Release object
            release = Release(
                id=release_id,
                master_id=master_id,
                title=title,
                year=year,
                country=country,
                released=released,
                notes=notes,
                data_quality=data_quality,
                status=status,
                genres=genres if genres else None,
                styles=styles if styles else None,
                formats=formats if formats else None
            )
            
            return release
            
        except Exception as e:
            logger.warning(f"Error parsing release record: {e}")
            return None