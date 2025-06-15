#!/usr/bin/env python3

import csv
import json
from typing import Dict, List, Any, Optional, Union
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime

from ..database.database import get_database_url
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class AnalyticsEngine:
    """Analytics engine for running collection analysis queries."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.database_url = get_database_url(config)
        self.engine = create_engine(self.database_url)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

    def get_session(self) -> Session:
        """Get a database session."""
        return self.SessionLocal()

    def run_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results as list of dictionaries."""
        session = self.get_session()
        try:
            result = session.execute(text(query), params or {})
            columns = result.keys()
            rows = result.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        finally:
            session.close()

    def artist_collaborations(self, artist1_name: str, artist2_name: str) -> List[Dict[str, Any]]:
        """Find all releases where two artists collaborated."""
        query = """
        SELECT DISTINCT r.id, r.title, r.released, r.country
        FROM releases r
        JOIN release_artists ra1 ON r.id = ra1.release_id  
        JOIN release_artists ra2 ON r.id = ra2.release_id
        JOIN artists a1 ON ra1.artist_id = a1.id
        JOIN artists a2 ON ra2.artist_id = a2.id
        WHERE LOWER(a1.name) LIKE LOWER(:artist1) 
        AND LOWER(a2.name) LIKE LOWER(:artist2)
        AND a1.id != a2.id
        ORDER BY r.released DESC, r.title
        """
        return self.run_query(query, {
            'artist1': f'%{artist1_name}%',
            'artist2': f'%{artist2_name}%'
        })

    def releases_by_label(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Count releases by label, ordered by most releases."""
        query = """
        SELECT l.name as label_name, COUNT(*) as release_count
        FROM labels l
        JOIN release_labels rl ON l.id = rl.label_id
        JOIN releases r ON rl.release_id = r.id
        GROUP BY l.id, l.name 
        ORDER BY COUNT(*) DESC
        LIMIT :limit
        """
        return self.run_query(query, {'limit': limit})

    def longest_tracks(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Find the longest tracks in the collection."""
        query = """
        SELECT r.title as release_title, t.title as track_title, 
               t.duration, t.duration_seconds
        FROM tracks t
        JOIN releases r ON t.release_id = r.id
        WHERE t.duration_seconds IS NOT NULL
        ORDER BY t.duration_seconds DESC 
        LIMIT :limit
        """
        return self.run_query(query, {'limit': limit})

    def favorite_decade(self) -> List[Dict[str, Any]]:
        """Analyze collection by decade, preventing duplicate counting of same albums."""
        query = """
        WITH earliest_releases AS (
            SELECT
                r.master_id,
                MIN(
                    COALESCE(
                        CAST(strftime('%Y', r.released) AS INTEGER),
                        m.year,
                        CAST(json_extract(uc.basic_information, '$.year') AS INTEGER)
                    )
                ) as earliest_year
            FROM releases r
            INNER JOIN user_collection uc ON r.id = uc.release_id
            LEFT JOIN masters m ON r.master_id = m.id
            WHERE r.master_id IS NOT NULL
              AND (
                  r.released IS NOT NULL OR
                  m.year IS NOT NULL OR
                  json_extract(uc.basic_information, '$.year') IS NOT NULL
              )
            GROUP BY r.master_id

            UNION ALL

            -- Include releases without master_id (standalone releases)
            SELECT
                NULL as master_id,
                COALESCE(
                    CAST(strftime('%Y', r.released) AS INTEGER),
                    CAST(json_extract(uc.basic_information, '$.year') AS INTEGER)
                ) as earliest_year
            FROM releases r
            INNER JOIN user_collection uc ON r.id = uc.release_id
            WHERE r.master_id IS NULL
              AND (
                  r.released IS NOT NULL OR
                  json_extract(uc.basic_information, '$.year') IS NOT NULL
              )
        ),
        decade_counts AS (
            SELECT
                (earliest_year / 10) * 10 as decade_start,
                COUNT(*) as release_count
            FROM earliest_releases
            WHERE earliest_year IS NOT NULL
            GROUP BY (earliest_year / 10) * 10
        )
        SELECT
            decade_start,
            (decade_start || 's') as decade,
            release_count,
            ROUND(100.0 * release_count / SUM(release_count) OVER(), 2) as percentage
        FROM decade_counts
        ORDER BY release_count DESC
        """
        return self.run_query(query)

    def multiple_copies(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Find releases where you own multiple copies/variants."""
        query = """
        WITH duplicate_releases AS (
            SELECT
                r.master_id,
                m.title as master_title,
                COUNT(DISTINCT uc.release_id) as copy_count
            FROM releases r
            INNER JOIN user_collection uc ON r.id = uc.release_id
            INNER JOIN masters m ON r.master_id = m.id
            WHERE r.master_id IS NOT NULL AND r.master_id > 0
            GROUP BY r.master_id, m.title
            HAVING COUNT(DISTINCT uc.release_id) > 1
        )
        SELECT
            master_title as release_name,
            copy_count
        FROM duplicate_releases
        ORDER BY copy_count DESC, master_title
        LIMIT :limit
        """
        return self.run_query(query, {'limit': limit})

    def collection_summary(self) -> Dict[str, Any]:
        """Get overall collection statistics."""
        queries = {
            'total_releases': "SELECT COUNT(*) as count FROM user_collection",
            'total_artists': """
                SELECT COUNT(DISTINCT a.id) as count
                FROM artists a
                JOIN release_artists ra ON a.id = ra.artist_id
                JOIN user_collection uc ON ra.release_id = uc.release_id
            """,
            'total_labels': """
                SELECT COUNT(DISTINCT l.id) as count  
                FROM labels l
                JOIN release_labels rl ON l.id = rl.label_id
                JOIN user_collection uc ON rl.release_id = uc.release_id
            """,
            'earliest_year': """
                SELECT MIN(
                    COALESCE(
                        CAST(strftime('%Y', r.released) AS INTEGER),
                        m.year,
                        CAST(json_extract(uc.basic_information, '$.year') AS INTEGER)
                    )
                ) as earliest_year
                FROM releases r
                INNER JOIN user_collection uc ON r.id = uc.release_id
                LEFT JOIN masters m ON r.master_id = m.id
            """,
            'latest_year': """
                SELECT MAX(
                    COALESCE(
                        CAST(strftime('%Y', r.released) AS INTEGER),
                        m.year,
                        CAST(json_extract(uc.basic_information, '$.year') AS INTEGER)
                    )
                ) as latest_year
                FROM releases r
                INNER JOIN user_collection uc ON r.id = uc.release_id
                LEFT JOIN masters m ON r.master_id = m.id
            """
        }
        
        summary = {}
        for key, query in queries.items():
            result = self.run_query(query)
            if result:
                summary[key] = result[0].get('count') or result[0].get('earliest_year') or result[0].get('latest_year')
            else:
                summary[key] = 0
                
        return summary

    def top_artists(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Find most collected artists."""
        query = """
        SELECT a.name as artist_name, COUNT(DISTINCT uc.release_id) as release_count
        FROM artists a
        JOIN release_artists ra ON a.id = ra.artist_id
        JOIN user_collection uc ON ra.release_id = uc.release_id
        GROUP BY a.id, a.name
        ORDER BY COUNT(DISTINCT uc.release_id) DESC, a.name
        LIMIT :limit
        """
        return self.run_query(query, {'limit': limit})

    def genre_analysis(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Analyze collection by genre."""
        query = """
        SELECT 
            value as genre,
            COUNT(DISTINCT uc.release_id) as release_count,
            ROUND(100.0 * COUNT(DISTINCT uc.release_id) / 
                  (SELECT COUNT(*) FROM user_collection), 2) as percentage
        FROM user_collection uc
        JOIN releases r ON uc.release_id = r.id,
        json_each(r.genres)
        WHERE r.genres IS NOT NULL
        AND json_valid(r.genres) = 1
        GROUP BY value
        ORDER BY COUNT(DISTINCT uc.release_id) DESC
        LIMIT :limit
        """
        return self.run_query(query, {'limit': limit})

    def format_analysis(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Analyze collection by format."""
        query = """
        SELECT 
            json_extract(value, '$.name') as format,
            COUNT(DISTINCT uc.release_id) as release_count,
            ROUND(100.0 * COUNT(DISTINCT uc.release_id) / 
                  (SELECT COUNT(*) FROM user_collection), 2) as percentage
        FROM user_collection uc
        JOIN releases r ON uc.release_id = r.id,
        json_each(r.formats)
        WHERE r.formats IS NOT NULL
        AND json_valid(r.formats) = 1
        GROUP BY json_extract(value, '$.name')
        ORDER BY COUNT(DISTINCT uc.release_id) DESC
        LIMIT :limit
        """
        return self.run_query(query, {'limit': limit})

    def year_analysis(self, limit: int = 30) -> List[Dict[str, Any]]:
        """Analyze collection by year."""
        query = """
        SELECT 
            COALESCE(
                CAST(strftime('%Y', r.released) AS INTEGER),
                m.year,
                CAST(json_extract(uc.basic_information, '$.year') AS INTEGER)
            ) as year,
            COUNT(*) as release_count
        FROM releases r
        INNER JOIN user_collection uc ON r.id = uc.release_id
        LEFT JOIN masters m ON r.master_id = m.id
        WHERE COALESCE(
            CAST(strftime('%Y', r.released) AS INTEGER),
            m.year,
            CAST(json_extract(uc.basic_information, '$.year') AS INTEGER)
        ) IS NOT NULL
        GROUP BY COALESCE(
            CAST(strftime('%Y', r.released) AS INTEGER),
            m.year,
            CAST(json_extract(uc.basic_information, '$.year') AS INTEGER)
        )
        ORDER BY COUNT(*) DESC
        LIMIT :limit
        """
        return self.run_query(query, {'limit': limit})


class OutputFormatter:
    """Format analytics results for different output modes."""
    
    @staticmethod
    def format_human_readable(data: List[Dict[str, Any]], title: str = "") -> str:
        """Format results in human-readable format."""
        if not data:
            return f"{title}\n{'=' * len(title)}\nNo results found.\n"
        
        output = []
        if title:
            output.append(title)
            output.append('=' * len(title))
        
        # Determine column widths
        if data:
            keys = list(data[0].keys())
            widths = {}
            for key in keys:
                widths[key] = max(len(str(key)), max(len(str(row.get(key, ''))) for row in data))
        
            # Header
            header = " | ".join(str(key).ljust(widths[key]) for key in keys)
            output.append(header)
            output.append("-" * len(header))
            
            # Rows
            for row in data:
                row_str = " | ".join(str(row.get(key, '')).ljust(widths[key]) for key in keys)
                output.append(row_str)
        
        return '\n'.join(output) + '\n'
    
    @staticmethod
    def format_csv(data: List[Dict[str, Any]]) -> str:
        """Format results as CSV."""
        if not data:
            return ""
        
        import io
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
        return output.getvalue()
    
    @staticmethod
    def format_json(data: List[Dict[str, Any]]) -> str:
        """Format results as JSON."""
        return json.dumps(data, indent=2, default=str)
    
    @staticmethod
    def format_summary(summary: Dict[str, Any]) -> str:
        """Format collection summary in human-readable format."""
        output = []
        output.append("Collection Summary")
        output.append("=" * 17)
        output.append(f"Total Releases: {summary.get('total_releases', 0):,}")
        output.append(f"Total Artists: {summary.get('total_artists', 0):,}")
        output.append(f"Total Labels: {summary.get('total_labels', 0):,}")
        
        earliest = summary.get('earliest_year')
        latest = summary.get('latest_year')
        if earliest and latest:
            span = latest - earliest
            output.append(f"Year Range: {earliest} - {latest} ({span} years)")
        
        return '\n'.join(output) + '\n'