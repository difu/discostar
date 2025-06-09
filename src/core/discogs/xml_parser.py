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
            
            # Extract artists, labels, and tracklist
            artists = self._extract_artists(element)
            extraartists = self._extract_extraartists(element)
            labels = self._extract_labels(element)
            tracklist = self._extract_tracklist(element)
            
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
                artists=artists if artists else None,
                extraartists=extraartists if extraartists else None,
                labels=labels if labels else None,
                tracklist=tracklist if tracklist else None,
                genres=genres if genres else None,
                styles=styles if styles else None,
                formats=formats if formats else None
            )
            
            return release
            
        except Exception as e:
            logger.warning(f"Error parsing release record: {e}")
            return None
    
    def _extract_artists(self, parent: ET.Element) -> List[Dict[str, Any]]:
        """Extract main artists from release element."""
        artists = []
        artists_elem = parent.find('artists')
        if artists_elem is not None:
            for artist_elem in artists_elem.findall('artist'):
                artist_data = self._extract_artist_data(artist_elem)
                if artist_data:
                    artists.append(artist_data)
        return artists
    
    def _extract_extraartists(self, parent: ET.Element) -> List[Dict[str, Any]]:
        """Extract extra artists (producers, etc.) from release element."""
        extraartists = []
        extraartists_elem = parent.find('extraartists')
        if extraartists_elem is not None:
            for artist_elem in extraartists_elem.findall('artist'):
                artist_data = self._extract_artist_data(artist_elem)
                if artist_data:
                    extraartists.append(artist_data)
        return extraartists
    
    def _extract_artist_data(self, artist_elem: ET.Element) -> Optional[Dict[str, Any]]:
        """Extract artist data from artist element."""
        try:
            artist_data = {}
            
            # Get artist ID
            artist_id = artist_elem.find('id')
            if artist_id is not None:
                id_value = self._safe_text(artist_id)
                if id_value:
                    artist_data['id'] = int(id_value)
            
            # Get artist name
            name = self._safe_text(artist_elem.find('name'))
            if name:
                artist_data['name'] = name
            
            # Get artist name variation (anv)
            anv = self._safe_text(artist_elem.find('anv'))
            if anv:
                artist_data['anv'] = anv
            
            # Get join relation
            join = self._safe_text(artist_elem.find('join'))
            if join:
                artist_data['join'] = join
            
            # Get role (for extra artists)
            role = self._safe_text(artist_elem.find('role'))
            if role:
                artist_data['role'] = role
            
            # Get tracks (where artist appears)
            tracks = self._safe_text(artist_elem.find('tracks'))
            if tracks:
                artist_data['tracks'] = tracks
            
            return artist_data if artist_data else None
            
        except Exception as e:
            logger.debug(f"Error extracting artist data: {e}")
            return None
    
    def _extract_labels(self, parent: ET.Element) -> List[Dict[str, Any]]:
        """Extract labels from release element."""
        labels = []
        labels_elem = parent.find('labels')
        if labels_elem is not None:
            for label_elem in labels_elem.findall('label'):
                label_data = self._extract_label_data(label_elem)
                if label_data:
                    labels.append(label_data)
        return labels
    
    def _extract_label_data(self, label_elem: ET.Element) -> Optional[Dict[str, Any]]:
        """Extract label data from label element."""
        try:
            label_data = {}
            
            # Get label ID (usually as attribute)
            label_id = label_elem.get('id')
            if label_id:
                try:
                    label_data['id'] = int(label_id)
                except ValueError:
                    pass
            
            # Get label name (usually as attribute)  
            name = label_elem.get('name')
            if name:
                label_data['name'] = name
            
            # Get catalog number (usually as attribute)
            catno = label_elem.get('catno')
            if catno:
                label_data['catno'] = catno
            
            return label_data if label_data else None
            
        except Exception as e:
            logger.debug(f"Error extracting label data: {e}")
            return None
    
    def _extract_tracklist(self, parent: ET.Element) -> List[Dict[str, Any]]:
        """Extract tracklist from release element."""
        tracks = []
        tracklist_elem = parent.find('tracklist')
        if tracklist_elem is not None:
            for track_elem in tracklist_elem.findall('track'):
                track_data = self._extract_track_data(track_elem)
                if track_data:
                    tracks.append(track_data)
        return tracks
    
    def _extract_track_data(self, track_elem: ET.Element) -> Optional[Dict[str, Any]]:
        """Extract track data from track element."""
        try:
            track_data = {}
            
            # Get position
            position = self._safe_text(track_elem.find('position'))
            if position:
                track_data['position'] = position
            
            # Get title
            title = self._safe_text(track_elem.find('title'))
            if title:
                track_data['title'] = title
            
            # Get duration
            duration = self._safe_text(track_elem.find('duration'))
            if duration:
                track_data['duration'] = duration
            
            # Get type (track, index, heading, etc.)
            track_type = track_elem.get('type')
            if track_type:
                track_data['type_'] = track_type  # Use 'type_' to avoid Python keyword
            
            return track_data if track_data else None
            
        except Exception as e:
            logger.debug(f"Error extracting track data: {e}")
            return None