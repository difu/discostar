"""
Database migration utilities for DiscoStar.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

from sqlalchemy import text, inspect
from sqlalchemy.orm import Session

from .database import get_database_manager
from .models import Base


logger = logging.getLogger(__name__)


class Migration:
    """Represents a database migration."""
    
    def __init__(self, version: str, description: str, up_sql: str, down_sql: str = ""):
        """Initialize migration.
        
        Args:
            version: Migration version (e.g., "001", "002")
            description: Human-readable description
            up_sql: SQL to apply the migration
            down_sql: SQL to rollback the migration
        """
        self.version = version
        self.description = description
        self.up_sql = up_sql
        self.down_sql = down_sql
        self.timestamp = datetime.utcnow()
    
    def __repr__(self):
        return f"<Migration(version='{self.version}', description='{self.description}')>"


class MigrationManager:
    """Manages database migrations."""
    
    def __init__(self):
        """Initialize migration manager."""
        self.db_manager = get_database_manager()
        self._ensure_migration_table()
    
    def _ensure_migration_table(self) -> None:
        """Create migration tracking table if it doesn't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version TEXT PRIMARY KEY,
            description TEXT NOT NULL,
            applied_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        with self.db_manager.engine.connect() as connection:
            connection.execute(text(create_table_sql))
            connection.commit()
    
    def get_applied_migrations(self) -> List[str]:
        """Get list of applied migration versions.
        
        Returns:
            List of migration versions that have been applied
        """
        with self.db_manager.engine.connect() as connection:
            result = connection.execute(
                text("SELECT version FROM schema_migrations ORDER BY version")
            )
            return [row[0] for row in result.fetchall()]
    
    def apply_migration(self, migration: Migration) -> bool:
        """Apply a migration.
        
        Args:
            migration: Migration to apply
            
        Returns:
            True if successful, False otherwise
        """
        applied_migrations = self.get_applied_migrations()
        
        if migration.version in applied_migrations:
            logger.info(f"Migration {migration.version} already applied, skipping")
            return True
        
        try:
            with self.db_manager.engine.connect() as connection:
                # Special handling for initial migration - check if tables exist
                if migration.version == "001":
                    inspector = inspect(self.db_manager.engine)
                    existing_tables = inspector.get_table_names()
                    if 'artists' in existing_tables:
                        logger.info("Tables already exist, marking initial migration as applied")
                        # Just record the migration as applied without executing
                        connection.execute(
                            text("INSERT INTO schema_migrations (version, description) VALUES (:version, :description)"),
                            {"version": migration.version, "description": migration.description}
                        )
                        connection.commit()
                        return True
                
                # Execute migration SQL
                if migration.up_sql.strip():
                    statements = [stmt.strip() for stmt in migration.up_sql.split(';') if stmt.strip()]
                    for statement in statements:
                        logger.debug(f"Executing: {statement[:100]}...")
                        connection.execute(text(statement))
                
                # Record migration as applied
                connection.execute(
                    text("INSERT INTO schema_migrations (version, description) VALUES (:version, :description)"),
                    {"version": migration.version, "description": migration.description}
                )
                connection.commit()
                
                logger.info(f"Applied migration {migration.version}: {migration.description}")
                return True
                
        except Exception as e:
            logger.error(f"Failed to apply migration {migration.version}: {e}")
            return False
    
    def rollback_migration(self, migration: Migration) -> bool:
        """Rollback a migration.
        
        Args:
            migration: Migration to rollback
            
        Returns:
            True if successful, False otherwise
        """
        applied_migrations = self.get_applied_migrations()
        
        if migration.version not in applied_migrations:
            logger.info(f"Migration {migration.version} not applied, nothing to rollback")
            return True
        
        if not migration.down_sql.strip():
            logger.error(f"No rollback SQL defined for migration {migration.version}")
            return False
        
        try:
            with self.db_manager.engine.connect() as connection:
                # Execute rollback SQL
                statements = [stmt.strip() for stmt in migration.down_sql.split(';') if stmt.strip()]
                for statement in statements:
                    logger.debug(f"Executing rollback: {statement[:100]}...")
                    connection.execute(text(statement))
                
                # Remove migration record
                connection.execute(
                    text("DELETE FROM schema_migrations WHERE version = :version"),
                    {"version": migration.version}
                )
                connection.commit()
                
                logger.info(f"Rolled back migration {migration.version}: {migration.description}")
                return True
                
        except Exception as e:
            logger.error(f"Failed to rollback migration {migration.version}: {e}")
            return False
    
    def get_migration_status(self) -> Dict[str, Any]:
        """Get current migration status.
        
        Returns:
            Dictionary with migration status information
        """
        applied_migrations = self.get_applied_migrations()
        
        # Check if database schema exists
        inspector = inspect(self.db_manager.engine)
        existing_tables = inspector.get_table_names()
        
        return {
            "applied_migrations": applied_migrations,
            "total_applied": len(applied_migrations),
            "existing_tables": existing_tables,
            "schema_current": len(existing_tables) > 1  # More than just migration table
        }


def create_initial_migration() -> Migration:
    """Create the initial migration for the database schema.
    
    Returns:
        Initial migration object
    """
    # Read the schema SQL file
    schema_file = Path(__file__).parent / "schema.sql"
    
    if schema_file.exists():
        with open(schema_file, 'r') as f:
            schema_sql = f.read()
    else:
        # Fallback to creating from SQLAlchemy models
        from sqlalchemy.schema import CreateTable
        
        schema_statements = []
        for table in Base.metadata.tables.values():
            create_statement = str(CreateTable(table).compile(
                dialect=get_database_manager().engine.dialect
            ))
            schema_statements.append(create_statement)
        
        schema_sql = ";\n".join(schema_statements)
    
    return Migration(
        version="001",
        description="Initial schema creation",
        up_sql=schema_sql,
        down_sql="DROP TABLE IF EXISTS sync_status; DROP TABLE IF EXISTS data_sources; DROP TABLE IF EXISTS collection_folders; DROP TABLE IF EXISTS user_collection; DROP TABLE IF EXISTS users; DROP TABLE IF EXISTS tracks; DROP TABLE IF EXISTS release_labels; DROP TABLE IF EXISTS release_artists; DROP TABLE IF EXISTS releases; DROP TABLE IF EXISTS masters; DROP TABLE IF EXISTS labels; DROP TABLE IF EXISTS artists;"
    )


def create_duration_seconds_migration() -> Migration:
    """Create migration to add duration_seconds column to tracks table.
    
    Returns:
        Migration object for adding duration_seconds column
    """
    up_sql = """
    ALTER TABLE tracks ADD COLUMN duration_seconds INTEGER;
    CREATE INDEX idx_tracks_duration_seconds ON tracks(duration_seconds);
    """
    
    down_sql = """
    DROP INDEX IF EXISTS idx_tracks_duration_seconds;
    ALTER TABLE tracks DROP COLUMN duration_seconds;
    """
    
    return Migration(
        version="002",
        description="Add duration_seconds column to tracks table for proper sorting",
        up_sql=up_sql,
        down_sql=down_sql
    )


def run_migrations() -> bool:
    """Run all pending migrations.
    
    Returns:
        True if all migrations succeeded, False otherwise
    """
    migration_manager = MigrationManager()
    
    # Define migrations in order
    migrations = [
        create_initial_migration(),
        create_duration_seconds_migration(),
        # Add future migrations here
    ]
    
    success = True
    for migration in migrations:
        if not migration_manager.apply_migration(migration):
            success = False
            break
    
    return success


def get_migration_status() -> Dict[str, Any]:
    """Get current migration status.
    
    Returns:
        Dictionary with migration status information
    """
    migration_manager = MigrationManager()
    return migration_manager.get_migration_status()