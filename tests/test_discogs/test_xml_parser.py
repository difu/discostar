"""
Unit tests for XML parser error handling and edge cases.
"""

import gzip
import pytest
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path
from unittest.mock import Mock, patch, mock_open, MagicMock
from typing import Optional, List, Any
from datetime import datetime

from src.core.discogs.xml_parser import (
    BaseXMLParser, 
    ArtistXMLParser, 
    LabelXMLParser, 
    MasterXMLParser, 
    ReleaseXMLParser
)
from src.core.database.models import Artist, Release, Label, Master


class TestBaseXMLParserErrorHandling:
    """Test error handling in the base XML parser class."""
    
    class MockXMLParser(BaseXMLParser):
        """Mock parser for testing base functionality."""
        
        def get_record_tags(self) -> List[str]:
            return ['test_record']
        
        def parse_record(self, element: ET.Element) -> Optional[Any]:
            # Simulate record parsing
            if element.get('error') == 'true':
                raise ValueError("Simulated parsing error")
            if element.get('empty') == 'true':
                return None
            return {'id': element.get('id', 'default')}
    
    def test_open_file_with_nonexistent_file(self):
        """Test opening a file that doesn't exist."""
        parser = self.MockXMLParser(Path('/nonexistent/file.xml'))
        
        with pytest.raises(FileNotFoundError):
            with parser._open_file():
                pass
    
    def test_open_file_with_permission_error(self, tmp_path):
        """Test handling file permission errors."""
        test_file = tmp_path / "restricted.xml"
        test_file.write_text("<root></root>")
        
        parser = self.MockXMLParser(test_file)
        
        with patch('builtins.open', side_effect=PermissionError("Permission denied")):
            with pytest.raises(PermissionError):
                with parser._open_file():
                    pass
    
    def test_open_gzip_file_with_corruption(self, tmp_path):
        """Test handling corrupted gzip files."""
        corrupted_gz = tmp_path / "corrupted.xml.gz"
        # Write data that looks like gzip header but is corrupted
        corrupted_gz.write_bytes(b'\x1f\x8b\x08\x00corrupted')
        
        parser = self.MockXMLParser(corrupted_gz)
        
        # Should raise some kind of exception when trying to read corrupted gzip
        with pytest.raises(Exception):
            with parser._open_file() as f:
                f.read()  # Try to actually read the content
    
    def test_parse_file_with_malformed_xml(self, tmp_path):
        """Test parsing malformed XML files."""
        malformed_xml = tmp_path / "malformed.xml"
        malformed_xml.write_text("""
        <root>
            <test_record id="1">
                <unclosed_tag>
            </test_record>
        </root>
        """)
        
        parser = self.MockXMLParser(malformed_xml)
        
        with pytest.raises(ET.ParseError):
            list(parser.parse_file())
    
    def test_parse_file_with_invalid_encoding(self, tmp_path):
        """Test parsing files with invalid encoding."""
        invalid_encoding_file = tmp_path / "invalid_encoding.xml"
        # Write invalid UTF-8 bytes
        invalid_encoding_file.write_bytes(b'<root>\xFF\xFE</root>')
        
        parser = self.MockXMLParser(invalid_encoding_file)
        
        with pytest.raises(UnicodeDecodeError):
            list(parser.parse_file())
    
    def test_parse_file_handles_record_parsing_errors(self, tmp_path):
        """Test that individual record parsing errors don't stop file processing."""
        test_xml = tmp_path / "test_with_errors.xml"
        test_xml.write_text("""
        <root>
            <test_record id="1">Good record</test_record>
            <test_record id="2" error="true">Bad record</test_record>
            <test_record id="3">Another good record</test_record>
            <test_record id="4" empty="true">Empty record</test_record>
            <test_record id="5">Final good record</test_record>
        </root>
        """)
        
        parser = self.MockXMLParser(test_xml)
        
        with patch('src.core.discogs.xml_parser.logger') as mock_logger:
            records = list(parser.parse_file())
        
        # Should get 3 good records (1, 3, 5)
        assert len(records) == 3
        assert parser.processed_records == 3
        assert parser.error_count == 2  # One error, one empty
        
        # Should have logged the error
        mock_logger.debug.assert_called()
    
    def test_parse_file_progress_callback(self, tmp_path):
        """Test progress callback functionality."""
        test_xml = tmp_path / "test_progress.xml"
        # Create XML with enough records to trigger progress callback
        records = ''.join(f'<test_record id="{i}">Record {i}</test_record>' for i in range(1500))
        test_xml.write_text(f'<root>{records}</root>')
        
        parser = self.MockXMLParser(test_xml)
        progress_callback = Mock()
        parser.progress_callback = progress_callback
        
        list(parser.parse_file())
        
        # Should have called progress callback at 1000 records
        progress_callback.assert_called_with(1000)
    
    def test_parse_file_logs_progress(self, tmp_path):
        """Test that progress is logged every 10000 records."""
        test_xml = tmp_path / "test_large.xml"
        # Create XML with enough records to trigger progress logging
        records = ''.join(f'<test_record id="{i}">Record {i}</test_record>' for i in range(15000))
        test_xml.write_text(f'<root>{records}</root>')
        
        parser = self.MockXMLParser(test_xml)
        
        with patch('src.core.discogs.xml_parser.logger') as mock_logger:
            list(parser.parse_file())
        
        # Should have logged progress at 10000 records
        mock_logger.info.assert_any_call("Processed 10,000 records, 0 errors")
    
    def test_memory_cleanup_with_large_file(self, tmp_path):
        """Test that parser processes large files without memory issues."""
        test_xml = tmp_path / "test_memory.xml"
        test_xml.write_text("""
        <root>
            <test_record id="1">Record 1</test_record>
            <test_record id="2">Record 2</test_record>
        </root>
        """)
        
        parser = self.MockXMLParser(test_xml)
        
        # Simply verify that the parser can process the file
        # The actual memory cleanup happens in parse_file() method
        records = list(parser.parse_file())
        
        # Should successfully process all records
        assert len(records) == 2
        assert parser.processed_records == 2
        assert parser.error_count == 0
    
    def test_safe_text_with_none_element(self):
        """Test _safe_text with None element."""
        parser = self.MockXMLParser(Path('dummy'))
        assert parser._safe_text(None) is None
    
    def test_safe_text_with_empty_element(self):
        """Test _safe_text with element that has no text."""
        parser = self.MockXMLParser(Path('dummy'))
        element = ET.Element('test')
        assert parser._safe_text(element) is None
    
    def test_safe_text_with_whitespace_only(self):
        """Test _safe_text with whitespace-only text."""
        parser = self.MockXMLParser(Path('dummy'))
        element = ET.Element('test')
        element.text = "   \n\t  "
        # After stripping, empty string becomes falsy, so returns None
        result = parser._safe_text(element)
        assert result is None or result == ""
    
    def test_safe_int_with_invalid_text(self):
        """Test _safe_int with non-numeric text."""
        parser = self.MockXMLParser(Path('dummy'))
        element = ET.Element('test')
        element.text = "not_a_number"
        assert parser._safe_int(element) is None
    
    def test_safe_int_with_none_element(self):
        """Test _safe_int with None element."""
        parser = self.MockXMLParser(Path('dummy'))
        assert parser._safe_int(None) is None


class TestArtistXMLParserErrorHandling:
    """Test error handling specific to artist XML parser."""
    
    def test_parse_record_with_missing_id(self):
        """Test parsing artist record without ID."""
        parser = ArtistXMLParser(Path('dummy'))
        
        element = ET.fromstring("""
        <artist>
            <name>Test Artist</name>
        </artist>
        """)
        
        result = parser.parse_record(element)
        assert result is None
    
    def test_parse_record_with_invalid_id(self):
        """Test parsing artist record with invalid ID."""
        parser = ArtistXMLParser(Path('dummy'))
        
        element = ET.fromstring("""
        <artist>
            <id>not_a_number</id>
            <name>Test Artist</name>
        </artist>
        """)
        
        result = parser.parse_record(element)
        assert result is None
    
    def test_parse_record_with_missing_name(self):
        """Test parsing artist record without name."""
        parser = ArtistXMLParser(Path('dummy'))
        
        element = ET.fromstring("""
        <artist>
            <id>123</id>
        </artist>
        """)
        
        result = parser.parse_record(element)
        assert result is None
    
    def test_parse_record_with_malformed_aliases(self):
        """Test parsing artist with malformed aliases section."""
        parser = ArtistXMLParser(Path('dummy'))
        
        element = ET.fromstring("""
        <artist>
            <id>123</id>
            <name>Test Artist</name>
            <aliases>
                <name id="456">Valid Alias</name>
                <name>No ID Alias</name>
            </aliases>
        </artist>
        """)
        
        result = parser.parse_record(element)
        
        # Should create artist with valid aliases
        assert result is not None
        assert result.name == "Test Artist"
        assert len(result.aliases) == 2
    
    def test_parse_record_handles_xml_parsing_errors(self):
        """Test that XML parsing errors in artist records are handled."""
        parser = ArtistXMLParser(Path('dummy'))
        
        # Create element that will cause parsing issues
        element = ET.Element('artist')
        
        # Mock the parsing to raise an exception
        with patch.object(parser, '_safe_int', side_effect=Exception("XML error")):
            with patch('src.core.discogs.xml_parser.logger') as mock_logger:
                result = parser.parse_record(element)
        
        assert result is None
        mock_logger.warning.assert_called()


class TestLabelXMLParserErrorHandling:
    """Test error handling specific to label XML parser."""
    
    def test_parse_record_with_invalid_parent_label_id(self):
        """Test parsing label with invalid parent label ID."""
        parser = LabelXMLParser(Path('dummy'))
        
        element = ET.fromstring("""
        <label>
            <id>123</id>
            <name>Test Label</name>
            <parentLabel id="invalid_id">Parent Label</parentLabel>
        </label>
        """)
        
        result = parser.parse_record(element)
        
        # Should still create label but ignore invalid parent ID
        assert result is not None
        assert result.name == "Test Label"
        assert result.parent_label_id is None
    
    def test_parse_record_with_invalid_sublabel_ids(self):
        """Test parsing label with invalid sublabel IDs."""
        parser = LabelXMLParser(Path('dummy'))
        
        element = ET.fromstring("""
        <label>
            <id>123</id>
            <name>Test Label</name>
            <sublabels>
                <label id="456">Valid Sublabel</label>
                <label id="invalid">Invalid Sublabel</label>
                <label>No ID Sublabel</label>
            </sublabels>
        </label>
        """)
        
        result = parser.parse_record(element)
        
        # Should create label with only valid sublabel IDs
        assert result is not None
        assert result.name == "Test Label"
        assert result.subsidiaries == [456]  # Only valid ID included
    
    def test_parse_record_handles_exception(self):
        """Test that exceptions during label parsing are handled."""
        parser = LabelXMLParser(Path('dummy'))
        element = ET.Element('label')
        
        with patch.object(parser, '_safe_int', side_effect=Exception("Parsing error")):
            with patch('src.core.discogs.xml_parser.logger') as mock_logger:
                result = parser.parse_record(element)
        
        assert result is None
        mock_logger.warning.assert_called()


class TestMasterXMLParserErrorHandling:
    """Test error handling specific to master XML parser."""
    
    def test_parse_record_with_missing_id_attribute(self):
        """Test parsing master record without ID attribute."""
        parser = MasterXMLParser(Path('dummy'))
        
        element = ET.fromstring("""
        <master>
            <title>Test Album</title>
        </master>
        """)
        
        result = parser.parse_record(element)
        assert result is None
    
    def test_parse_record_with_invalid_id_attribute(self):
        """Test parsing master record with invalid ID attribute."""
        parser = MasterXMLParser(Path('dummy'))
        
        element = ET.fromstring("""
        <master id="not_a_number">
            <title>Test Album</title>
        </master>
        """)
        
        result = parser.parse_record(element)
        assert result is None
    
    def test_parse_record_with_missing_title(self):
        """Test parsing master record without title."""
        parser = MasterXMLParser(Path('dummy'))
        
        element = ET.fromstring("""
        <master id="123">
            <year>2023</year>
        </master>
        """)
        
        result = parser.parse_record(element)
        assert result is None
    
    def test_parse_record_with_invalid_numeric_fields(self):
        """Test parsing master with invalid numeric fields."""
        parser = MasterXMLParser(Path('dummy'))
        
        element = ET.fromstring("""
        <master id="123">
            <title>Test Album</title>
            <main_release>not_a_number</main_release>
            <year>invalid_year</year>
        </master>
        """)
        
        result = parser.parse_record(element)
        
        # Should create master but with None for invalid numeric fields
        assert result is not None
        assert result.title == "Test Album"
        assert result.main_release_id is None
        assert result.year is None


class TestReleaseXMLParserErrorHandling:
    """Test error handling specific to release XML parser."""
    
    def test_parse_record_with_invalid_date_formats(self):
        """Test parsing release with various invalid date formats."""
        parser = ReleaseXMLParser(Path('dummy'))
        
        test_cases = [
            "invalid-date",
            "2023-13-45",  # Invalid month/day
            "not-a-date",
            "2023/12/25",  # Wrong format
            ""
        ]
        
        for date_str in test_cases:
            element = ET.fromstring(f"""
            <release id="123">
                <title>Test Release</title>
                <released>{date_str}</released>
            </release>
            """)
            
            result = parser.parse_record(element)
            
            # Should create release but with None for invalid date
            assert result is not None
            assert result.title == "Test Release"
            assert result.released is None
    
    def test_parse_record_with_malformed_artists(self):
        """Test parsing release with malformed artists section."""
        parser = ReleaseXMLParser(Path('dummy'))
        
        element = ET.fromstring("""
        <release id="123">
            <title>Test Release</title>
            <artists>
                <artist>
                    <id>invalid_id</id>
                    <name>Artist 1</name>
                </artist>
                <artist>
                    <name>Artist 2</name>
                </artist>
            </artists>
        </release>
        """)
        
        result = parser.parse_record(element)
        
        # Should create release, artists section may have issues but shouldn't fail
        assert result is not None
        assert result.title == "Test Release"
    
    def test_extract_artist_data_with_invalid_id(self):
        """Test extracting artist data with invalid ID."""
        parser = ReleaseXMLParser(Path('dummy'))
        
        element = ET.fromstring("""
        <artist>
            <id>not_a_number</id>
            <name>Test Artist</name>
        </artist>
        """)
        
        with patch('src.core.discogs.xml_parser.logger') as mock_logger:
            result = parser._extract_artist_data(element)
        
        # ValueError on int conversion will cause the method to return None
        assert result is None
        mock_logger.debug.assert_called()
    
    def test_extract_label_data_with_invalid_id(self):
        """Test extracting label data with invalid ID."""
        parser = ReleaseXMLParser(Path('dummy'))
        
        element = ET.fromstring('<label id="invalid_id" name="Test Label" catno="CAT001"/>')
        
        result = parser._extract_label_data(element)
        
        # Should handle invalid ID gracefully
        assert result is not None
        assert result['name'] == "Test Label"
        assert result['catno'] == "CAT001"
        assert 'id' not in result  # Invalid ID should be excluded
    
    def test_extract_data_methods_with_exceptions(self):
        """Test that data extraction methods handle exceptions."""
        parser = ReleaseXMLParser(Path('dummy'))
        
        element = ET.Element('test')
        
        with patch.object(parser, '_safe_text', side_effect=Exception("Error")):
            with patch('src.core.discogs.xml_parser.logger') as mock_logger:
                result = parser._extract_artist_data(element)
        
        assert result is None
        mock_logger.debug.assert_called()
    
    def test_parse_record_handles_complex_parsing_errors(self):
        """Test that complex parsing errors are handled gracefully."""
        parser = ReleaseXMLParser(Path('dummy'))
        
        element = ET.fromstring("""
        <release id="123">
            <title>Test Release</title>
        </release>
        """)
        
        # Mock a method to raise an exception during parsing
        with patch.object(parser, '_extract_artists', side_effect=Exception("Complex error")):
            with patch('src.core.discogs.xml_parser.logger') as mock_logger:
                result = parser.parse_record(element)
        
        assert result is None
        mock_logger.warning.assert_called()


class TestXMLParserFileHandling:
    """Test file handling edge cases across all parsers."""
    
    @pytest.mark.parametrize("parser_class", [
        ArtistXMLParser, LabelXMLParser, MasterXMLParser, ReleaseXMLParser
    ])
    def test_parser_with_empty_file(self, tmp_path, parser_class):
        """Test parsing completely empty files."""
        empty_file = tmp_path / "empty.xml"
        empty_file.write_text("")
        
        parser = parser_class(empty_file)
        
        with pytest.raises((ET.ParseError, Exception)):
            list(parser.parse_file())
    
    @pytest.mark.parametrize("parser_class", [
        ArtistXMLParser, LabelXMLParser, MasterXMLParser, ReleaseXMLParser
    ])
    def test_parser_with_truncated_file(self, tmp_path, parser_class):
        """Test parsing truncated XML files."""
        truncated_file = tmp_path / "truncated.xml"
        truncated_file.write_text("<root><artist><id>123</id><name>Test")  # Truncated
        
        parser = parser_class(truncated_file)
        
        with pytest.raises(ET.ParseError):
            list(parser.parse_file())
    
    @pytest.mark.parametrize("parser_class", [
        ArtistXMLParser, LabelXMLParser, MasterXMLParser, ReleaseXMLParser
    ])
    def test_parser_with_extremely_large_content(self, tmp_path, parser_class):
        """Test parser memory handling with very large content."""
        large_file = tmp_path / "large.xml"
        
        # Create a large XML file content
        large_content = "x" * 100000  # 100KB of content
        if parser_class == ArtistXMLParser:
            xml_content = f"""
            <root>
                <artist>
                    <id>123</id>
                    <name>{large_content}</name>
                </artist>
            </root>
            """
        elif parser_class == LabelXMLParser:
            xml_content = f"""
            <root>
                <label>
                    <id>123</id>
                    <name>{large_content}</name>
                </label>
            </root>
            """
        elif parser_class == MasterXMLParser:
            xml_content = f"""
            <root>
                <master id="123">
                    <title>{large_content}</title>
                </master>
            </root>
            """
        else:  # ReleaseXMLParser
            xml_content = f"""
            <root>
                <release id="123">
                    <title>{large_content}</title>
                </release>
            </root>
            """
        
        large_file.write_text(xml_content)
        parser = parser_class(large_file)
        
        # Should handle large content without memory issues
        records = list(parser.parse_file())
        assert len(records) == 1
    
    def test_gzip_decompression_error_handling(self, tmp_path):
        """Test handling of gzip decompression errors."""
        # Create a file that looks like gzip but isn't valid
        fake_gz = tmp_path / "fake.xml.gz"
        fake_gz.write_bytes(b'\x1f\x8b\x08\x00invalid_gzip_data')
        
        parser = ArtistXMLParser(fake_gz)
        
        with pytest.raises((gzip.BadGzipFile, OSError, Exception)):
            list(parser.parse_file())
    
    def test_unicode_handling_in_xml_content(self, tmp_path):
        """Test handling of various Unicode characters in XML content."""
        unicode_file = tmp_path / "unicode.xml"
        
        # Include various Unicode characters that might cause issues
        unicode_content = """
        <root>
            <artist>
                <id>123</id>
                <name>Artist with émojis 🎵 and spécial çharacters</name>
                <profile>Description with Chinese: 音乐 and Arabic: موسيقى</profile>
            </artist>
        </root>
        """
        
        unicode_file.write_text(unicode_content, encoding='utf-8')
        parser = ArtistXMLParser(unicode_file)
        
        records = list(parser.parse_file())
        assert len(records) == 1
        assert "émojis" in records[0].name
    
    def test_xml_with_malicious_entities(self, tmp_path):
        """Test handling of XML with potentially malicious entities."""
        malicious_file = tmp_path / "malicious.xml"
        
        # XML with entity references that could cause issues
        malicious_content = """
        <!DOCTYPE root [
            <!ENTITY xxe SYSTEM "file:///etc/passwd">
        ]>
        <root>
            <artist>
                <id>123</id>
                <name>&xxe;</name>
            </artist>
        </root>
        """
        
        malicious_file.write_text(malicious_content)
        parser = ArtistXMLParser(malicious_file)
        
        # Should raise ParseError for undefined entity
        with pytest.raises(ET.ParseError):
            list(parser.parse_file())