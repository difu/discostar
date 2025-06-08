"""
Data ingestion pipeline for Discogs XML dumps.
"""

import logging
from pathlib import Path
from typing import Dict, Any, Optional, List, Type
from datetime import datetime

import click
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from ..database.models import Base, Artist, Release, Label, Master, DataSource
from ..database.database import get_database_url
from .xml_parser import ArtistXMLParser, ReleaseXMLParser, LabelXMLParser, MasterXMLParser, BaseXMLParser


logger = logging.getLogger(__name__)


class DataIngestionPipeline:
    """Handles ingestion of Discogs XML dump data into the database."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the ingestion pipeline.
        
        Args:
            config: Application configuration dictionary
        """
        self.config = config
        self.database_url = get_database_url(config)
        self.engine = create_engine(self.database_url)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        
        # Batch processing settings
        ingestion_config = config.get('ingestion', {})
        self.batch_size = ingestion_config.get('batch_size', 1000)
        self.commit_interval = ingestion_config.get('commit_interval', 10000)
        self.max_error_rate = ingestion_config.get('max_error_rate', 0.1)
        self.progress_update_interval = ingestion_config.get('progress_update_interval', 1000)
        
        # Parser mapping
        self.parsers = {
            'artists': ArtistXMLParser,
            'releases': ReleaseXMLParser,
            'labels': LabelXMLParser,
            'masters': MasterXMLParser
        }
        
        # Model mapping
        self.models = {
            'artists': Artist,
            'releases': Release,
            'labels': Label,
            'masters': Master
        }
    
    def ingest_dump(self, dump_type: str, file_path: Path, force: bool = False) -> bool:
        """Ingest a specific XML dump into the database.
        
        Args:
            dump_type: Type of dump (artists, releases, labels, masters)
            file_path: Path to the XML dump file
            force: Whether to force re-ingestion of already processed files
            
        Returns:
            True if ingestion was successful, False otherwise
        """
        if dump_type not in self.parsers:
            logger.error(f"Unknown dump type: {dump_type}")
            return False
        
        if not file_path.exists():
            logger.error(f"Dump file not found: {file_path}")
            return False
        
        # Check release strategy for releases dump
        if dump_type == 'releases':
            release_strategy = self.config.get('ingestion', {}).get('releases', {}).get('strategy', 'all')
            if release_strategy == 'skip':
                logger.info("Skipping releases ingestion due to strategy: 'skip'")
                click.echo("⏭️  Skipping releases ingestion (strategy: skip)")
                return True
            elif release_strategy == 'collection_only':
                # Check if we have collection data
                session = self.SessionLocal()
                try:
                    from ..database.models import UserCollection
                    collection_count = session.query(UserCollection).count()
                    if collection_count == 0:
                        logger.warning("No collection data found for 'collection_only' strategy")
                        click.echo("⚠️  No collection data found. Sync collection first or change strategy to 'all'")
                        return True
                finally:
                    session.close()
        
        # Check if this file has already been processed
        if not force and self._is_file_processed(dump_type, file_path):
            logger.info(f"File {file_path.name} already processed, skipping (use --force to re-process)")
            return True
        
        logger.info(f"Starting ingestion of {dump_type} dump: {file_path}")
        
        try:
            # Initialize parser with progress callback
            parser_class = self.parsers[dump_type]
            
            # Initialize progress tracking variables
            progress_counter = {'count': 0}
            
            def progress_callback(count):
                progress_counter['count'] = count
                if count % 10000 == 0:
                    click.echo(f"Processed {count:,} records...")
            
            parser = parser_class(file_path, progress_callback)
            
            # Process records in batches
            success = self._process_records_batch(parser, dump_type, file_path)
            
            if success:
                logger.info(f"Successfully ingested {dump_type} dump: {parser.processed_records:,} records, {parser.error_count} errors")
                return True
            else:
                logger.error(f"Failed to ingest {dump_type} dump")
                return False
                
        except Exception as e:
            logger.error(f"Error during ingestion of {dump_type} dump: {e}")
            return False
    
    def _process_records_batch(self, parser: BaseXMLParser, dump_type: str, file_path: Path) -> bool:
        """Process records from parser in batches.
        
        Args:
            parser: XML parser instance
            dump_type: Type of dump being processed
            file_path: Path to the dump file
            
        Returns:
            True if processing was successful, False otherwise
        """
        session = self.SessionLocal()
        
        try:
            batch = []
            total_processed = 0
            total_errors = 0
            
            for record in parser.parse_file():
                batch.append(record)
                
                # Process batch when it reaches the configured size
                if len(batch) >= self.batch_size:
                    processed, errors = self._process_batch(session, batch, dump_type)
                    total_processed += processed
                    total_errors += errors
                    batch.clear()
                    
                    # Commit periodically to avoid large transactions
                    if total_processed % self.commit_interval == 0:
                        session.commit()
                        click.echo(f"Processing {dump_type} - {total_processed:,} records, {total_errors} errors")
            
            # Process remaining records in the final batch
            if batch:
                processed, errors = self._process_batch(session, batch, dump_type)
                total_processed += processed
                total_errors += errors
            
            # Final commit
            session.commit()
            
            # Record the successful ingestion
            self._record_data_source(session, dump_type, file_path, total_processed)
            session.commit()
            
            logger.info(f"Ingestion complete: {total_processed:,} records processed, {total_errors} errors")
            
            # Consider ingestion successful if we processed records with reasonable error rate
            error_rate = total_errors / max(total_processed, 1) if total_processed > 0 else 0
            if error_rate > self.max_error_rate:
                logger.warning(f"High error rate during ingestion: {error_rate:.1%} (max allowed: {self.max_error_rate:.1%})")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error during batch processing: {e}")
            session.rollback()
            return False
        finally:
            session.close()
    
    def _process_batch(self, session: Session, batch: List[Any], dump_type: str) -> tuple[int, int]:
        """Process a batch of records.
        
        Args:
            session: Database session
            batch: List of record objects
            dump_type: Type of dump being processed
            
        Returns:
            Tuple of (processed_count, error_count)
        """
        processed = 0
        errors = 0
        
        try:
            # Try to process the entire batch at once for better performance
            for record in batch:
                session.merge(record)
            processed = len(batch)
        except (IntegrityError, SQLAlchemyError) as e:
            # If batch processing fails, process records individually
            session.rollback()
            logger.debug(f"Batch processing failed for {dump_type}, processing individually: {e}")
            
            for record in batch:
                try:
                    # Start a new transaction for each record
                    session.merge(record)
                    session.flush()  # Flush to catch errors early
                    processed += 1
                except IntegrityError as e:
                    logger.debug(f"Integrity error for {dump_type} record {getattr(record, 'id', 'unknown')}: {e}")
                    session.rollback()
                    errors += 1
                except SQLAlchemyError as e:
                    logger.warning(f"Database error for {dump_type} record {getattr(record, 'id', 'unknown')}: {e}")
                    session.rollback()
                    errors += 1
                except Exception as e:
                    logger.warning(f"Unexpected error for {dump_type} record {getattr(record, 'id', 'unknown')}: {e}")
                    session.rollback()
                    errors += 1
        
        return processed, errors
    
    def _is_file_processed(self, dump_type: str, file_path: Path) -> bool:
        """Check if a file has already been processed.
        
        Args:
            dump_type: Type of dump
            file_path: Path to the dump file
            
        Returns:
            True if file has been processed, False otherwise
        """
        session = self.SessionLocal()
        
        try:
            # Get the table name for this dump type
            table_name = self.models[dump_type].__tablename__
            
            # Check if there's a data source record for this file
            data_source = session.query(DataSource).filter(
                DataSource.table_name == table_name,
                DataSource.dump_file == file_path.name,
                DataSource.source_type == 'xml_dump'
            ).first()
            
            return data_source is not None
            
        except Exception as e:
            logger.warning(f"Error checking if file is processed: {e}")
            return False
        finally:
            session.close()
    
    def _record_data_source(self, session: Session, dump_type: str, file_path: Path, record_count: int):
        """Record information about the data source.
        
        Args:
            session: Database session
            dump_type: Type of dump
            file_path: Path to the dump file
            record_count: Number of records processed
        """
        try:
            # Get the table name for this dump type
            table_name = self.models[dump_type].__tablename__
            
            # Extract date from filename (format: discogs_YYYYMMDD_type.xml.gz)
            source_date = None
            parts = file_path.stem.split('_')  # Remove .gz extension
            if len(parts) >= 2:
                date_str = parts[1]
                if len(date_str) == 8:  # YYYYMMDD
                    try:
                        source_date = datetime.strptime(date_str, '%Y%m%d').date()
                    except ValueError:
                        pass
            
            # Create or update data source record
            data_source = session.query(DataSource).filter(
                DataSource.table_name == table_name,
                DataSource.dump_file == file_path.name
            ).first()
            
            if data_source:
                # Update existing record
                data_source.source_date = source_date
                data_source.record_id = record_count  # Use as record count
            else:
                # Create new record
                data_source = DataSource(
                    table_name=table_name,
                    record_id=record_count,  # Use as record count
                    source_type='xml_dump',
                    source_date=source_date,
                    dump_file=file_path.name
                )
                session.add(data_source)
            
        except Exception as e:
            logger.warning(f"Error recording data source: {e}")
    
    def get_ingestion_status(self) -> Dict[str, Any]:
        """Get the status of data ingestion for all dump types.
        
        Returns:
            Dictionary with ingestion status information
        """
        session = self.SessionLocal()
        status = {}
        
        try:
            for dump_type, model in self.models.items():
                table_name = model.__tablename__
                
                # Get record count
                record_count = session.query(model).count()
                
                # Get data source info
                data_source = session.query(DataSource).filter(
                    DataSource.table_name == table_name,
                    DataSource.source_type == 'xml_dump'
                ).first()
                
                status[dump_type] = {
                    'record_count': record_count,
                    'last_ingestion': data_source.source_date if data_source else None,
                    'dump_file': data_source.dump_file if data_source else None,
                    'ingested': record_count > 0
                }
        
        except Exception as e:
            logger.error(f"Error getting ingestion status: {e}")
        finally:
            session.close()
        
        return status
    
    def clear_data(self, dump_type: str) -> bool:
        """Clear all data for a specific dump type.
        
        Args:
            dump_type: Type of dump to clear
            
        Returns:
            True if successful, False otherwise
        """
        if dump_type not in self.models:
            logger.error(f"Unknown dump type: {dump_type}")
            return False
        
        session = self.SessionLocal()
        
        try:
            model = self.models[dump_type]
            table_name = model.__tablename__
            
            # Delete all records
            deleted_count = session.query(model).delete()
            
            # Delete data source records
            session.query(DataSource).filter(
                DataSource.table_name == table_name,
                DataSource.source_type == 'xml_dump'
            ).delete()
            
            session.commit()
            
            logger.info(f"Cleared {deleted_count:,} {dump_type} records")
            return True
            
        except Exception as e:
            logger.error(f"Error clearing {dump_type} data: {e}")
            session.rollback()
            return False
        finally:
            session.close()


def get_ingestion_pipeline(config: Dict[str, Any]) -> DataIngestionPipeline:
    """Factory function to create ingestion pipeline.
    
    Args:
        config: Application configuration
        
    Returns:
        DataIngestionPipeline instance
    """
    return DataIngestionPipeline(config)