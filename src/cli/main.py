#!/usr/bin/env python3

import asyncio
import sys
from pathlib import Path

import click

from ..core.utils.config import load_config, setup_logging, get_dumps_directory, validate_config
from ..core.discogs.xml_downloader import DiscogsDumpDownloader
from ..core.discogs.data_ingestion import get_ingestion_pipeline
from ..core.discogs.collection_sync import CollectionSync
from ..core.database.database import init_database


@click.group()
@click.option('--config', '-c', type=click.Path(exists=True), 
              help='Path to configuration file')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
@click.pass_context
def cli(ctx, config, verbose):
    """DiscoStar - Analyze your record collection with Discogs data."""
    ctx.ensure_object(dict)
    
    # Load configuration
    config_path = config or 'config/settings.yaml'
    ctx.obj['config'] = load_config(config_path)
    
    # Setup logging
    log_level = 'DEBUG' if verbose else ctx.obj['config'].get('logging', {}).get('level', 'INFO')
    setup_logging(log_level)


@cli.command()
@click.pass_context
def init(ctx):
    """Initialize DiscoStar database and directories."""
    config = ctx.obj['config']
    
    click.echo("🎵 Initializing DiscoStar...")
    
    # Create necessary directories
    directories = [
        'data/dumps',
        'data/cache',
        'logs'
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        click.echo(f"✓ Created directory: {directory}")
    
    # Initialize database
    try:
        from ..core.database.database import get_database_url
        database_url = get_database_url(config)
        init_database(database_url)
        click.echo("✓ Database initialized")
    except Exception as e:
        click.echo(f"❌ Database initialization failed: {e}")
        sys.exit(1)
    
    click.echo("✅ DiscoStar initialization complete!")
    click.echo("\nNext steps:")
    click.echo("1. Set up your .env file with DISCOGS_API_TOKEN and DISCOGS_USERNAME")
    click.echo("2. Run 'discostar download-dumps' to fetch Discogs data")
    click.echo("3. Run 'discostar ingest-data' to import XML data into database")
    click.echo("4. Run 'discostar sync-collection' to sync your personal collection")


@cli.command('download-dumps')
@click.option('--type', 'dump_type', 
              type=click.Choice(['artists', 'releases', 'labels', 'masters', 'all']),
              default='all', help='Type of dump to download')
@click.option('--force', is_flag=True, help='Force re-download even if files exist')
@click.pass_context
def download_dumps(ctx, dump_type, force):
    """Download Discogs XML database dumps."""
    config = ctx.obj['config']
    
    click.echo("🎵 Starting Discogs XML dump download...")
    
    try:
        asyncio.run(_download_dumps_async(config, dump_type, force))
    except KeyboardInterrupt:
        click.echo("\n❌ Download cancelled by user")
        sys.exit(1)
    except Exception as e:
        click.echo(f"\n❌ Download failed: {e}")
        sys.exit(1)


async def _download_dumps_async(config, dump_type, force):
    """Async wrapper for dump downloads."""
    downloader = DiscogsDumpDownloader(config)
    
    if dump_type == 'all':
        dump_types = ['artists', 'releases', 'labels', 'masters']
    else:
        dump_types = [dump_type]
    
    for dtype in dump_types:
        click.echo(f"\n📥 Downloading {dtype} dump...")
        success = await downloader.download_dump(dtype, force_download=force)
        
        if success:
            click.echo(f"✅ {dtype.capitalize()} dump downloaded successfully")
        else:
            click.echo(f"❌ Failed to download {dtype} dump")
            return False
    
    click.echo("\n🎉 All downloads completed successfully!")
    return True


@cli.command('ingest-data')
@click.option('--type', 'dump_type', 
              type=click.Choice(['artists', 'releases', 'labels', 'masters', 'all']),
              default='all', help='Type of dump to ingest')
@click.option('--force', is_flag=True, help='Force re-ingestion even if data exists')
@click.pass_context
def ingest_data(ctx, dump_type, force):
    """Ingest XML dump data into the database."""
    config = ctx.obj['config']
    
    # Check release strategy and provide guidance
    release_strategy = config.get('ingestion', {}).get('releases', {}).get('strategy', 'all')
    if dump_type in ['all', 'releases'] and release_strategy == 'collection_only':
        # Check if collection data exists
        try:
            from ..core.database.models import UserCollection
            from ..core.database.database import get_database_url
            from sqlalchemy import create_engine
            from sqlalchemy.orm import sessionmaker
            
            database_url = get_database_url(config)
            engine = create_engine(database_url)
            SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
            session = SessionLocal()
            
            try:
                collection_count = session.query(UserCollection).count()
                if collection_count == 0:
                    click.echo("⚠️  Using 'collection_only' strategy but no collection data found!")
                    click.echo("💡 Recommended workflow for collection_only strategy:")
                    click.echo("   1. discostar ingest-data --type artists")
                    click.echo("   2. discostar ingest-data --type labels") 
                    click.echo("   3. discostar ingest-data --type masters")
                    click.echo("   4. discostar sync-collection")
                    click.echo("   5. discostar ingest-data --type releases")
                    click.echo("\n🔄 Run 'discostar collection-workflow' for guided setup")
                    return
                else:
                    click.echo(f"📋 Using collection_only strategy with {collection_count} collection items")
            finally:
                session.close()
        except Exception as e:
            click.echo(f"⚠️  Error checking collection data: {e}")
    
    click.echo("🎵 Starting XML data ingestion...")
    
    try:
        # Get ingestion pipeline
        pipeline = get_ingestion_pipeline(config)
        dumps_dir = get_dumps_directory()
        
        # Determine which dump types to process
        if dump_type == 'all':
            dump_types = ['artists', 'labels', 'masters', 'releases']  # Process in dependency order
        else:
            dump_types = [dump_type]
        
        success_count = 0
        
        for dtype in dump_types:
            click.echo(f"\n📥 Processing {dtype} data...")
            
            # Find the dump file
            pattern = f"discogs_*_{dtype}.xml.gz"
            dump_files = list(dumps_dir.glob(pattern))
            
            if not dump_files:
                click.echo(f"❌ No {dtype} dump file found. Run 'discostar download-dumps' first.")
                continue
            
            # Use the most recent file
            dump_file = max(dump_files, key=lambda p: p.stat().st_mtime)
            click.echo(f"Processing file: {dump_file.name}")
            
            # Ingest the data
            success = pipeline.ingest_dump(dtype, dump_file, force=force)
            
            if success:
                click.echo(f"✅ {dtype.capitalize()} data ingested successfully")
                success_count += 1
            else:
                click.echo(f"❌ Failed to ingest {dtype} data")
        
        if success_count == len(dump_types):
            click.echo("\n🎉 All data ingestion completed successfully!")
        else:
            click.echo(f"\n⚠️  Ingestion completed with some failures ({success_count}/{len(dump_types)} successful)")
            
    except KeyboardInterrupt:
        click.echo("\n❌ Ingestion cancelled by user")
        sys.exit(1)
    except Exception as e:
        click.echo(f"\n❌ Ingestion failed: {e}")
        sys.exit(1)


@cli.command('status')
@click.pass_context
def status(ctx):
    """Show ingestion status and database statistics."""
    config = ctx.obj['config']
    
    try:
        # Get ingestion pipeline
        pipeline = get_ingestion_pipeline(config)
        
        # Get ingestion status
        status_info = pipeline.get_ingestion_status()
        
        click.echo("📊 DiscoStar Database Status\n")
        
        for dump_type, info in status_info.items():
            status_icon = "✅" if info['ingested'] else "❌"
            click.echo(f"{status_icon} {dump_type.capitalize()}:")
            click.echo(f"    Records: {info['record_count']:,}")
            if info['last_ingestion']:
                click.echo(f"    Last Ingestion: {info['last_ingestion']}")
            if info['dump_file']:
                click.echo(f"    Source File: {info['dump_file']}")
            click.echo()
        
        # Show download status
        downloader = DiscogsDumpDownloader(config)
        downloaded = downloader.get_downloaded_dumps()
        
        click.echo("📦 Downloaded Dumps:")
        for dump_type in ['artists', 'releases', 'labels', 'masters']:
            if dump_type in downloaded:
                file_path = downloaded[dump_type]
                click.echo(f"    ✅ {dump_type}: {file_path.name}")
            else:
                click.echo(f"    ❌ {dump_type}: Not downloaded")
        
    except Exception as e:
        click.echo(f"❌ Error getting status: {e}")
        sys.exit(1)


@cli.command('clear-data')
@click.option('--type', 'dump_type', 
              type=click.Choice(['artists', 'releases', 'labels', 'masters']),
              required=True, help='Type of data to clear')
@click.confirmation_option(prompt='Are you sure you want to clear this data?')
@click.pass_context
def clear_data(ctx, dump_type):
    """Clear ingested data from the database."""
    config = ctx.obj['config']
    
    try:
        # Get ingestion pipeline
        pipeline = get_ingestion_pipeline(config)
        
        click.echo(f"🗑️  Clearing {dump_type} data...")
        
        success = pipeline.clear_data(dump_type)
        
        if success:
            click.echo(f"✅ {dump_type.capitalize()} data cleared successfully")
        else:
            click.echo(f"❌ Failed to clear {dump_type} data")
            sys.exit(1)
            
    except Exception as e:
        click.echo(f"❌ Error clearing data: {e}")
        sys.exit(1)


@cli.command('sync-collection')
@click.option('--force', is_flag=True, help='Force refresh of all collection data')
@click.pass_context
def sync_collection(ctx, force):
    """Sync your Discogs collection with the local database."""
    config = ctx.obj['config']
    
    # Validate API configuration
    if not validate_config(config):
        click.echo("❌ Missing required API configuration.")
        click.echo("Please set DISCOGS_API_TOKEN and DISCOGS_USERNAME environment variables.")
        sys.exit(1)
    
    click.echo("🎵 Syncing collection from Discogs API...")
    
    try:
        asyncio.run(_sync_collection_async(config, force))
    except KeyboardInterrupt:
        click.echo("\n❌ Collection sync cancelled by user")
        sys.exit(1)
    except Exception as e:
        click.echo(f"\n❌ Collection sync failed: {e}")
        sys.exit(1)


async def _sync_collection_async(config, force):
    """Async wrapper for collection sync."""
    sync = CollectionSync(config)
    
    try:
        stats = await sync.sync_collection(force_refresh=force)
        
        click.echo("\n✅ Collection sync completed!")
        click.echo(f"📊 Statistics:")
        click.echo(f"   Items added: {stats['collection_items_added']}")
        click.echo(f"   Items updated: {stats['collection_items_updated']}")
        click.echo(f"   Releases fetched: {stats['releases_fetched']}")
        click.echo(f"   Artists fetched: {stats['artists_fetched']}")
        click.echo(f"   Labels fetched: {stats['labels_fetched']}")
        if stats['errors'] > 0:
            click.echo(f"   Errors: {stats['errors']}")
        
    except Exception as e:
        click.echo(f"❌ Collection sync failed: {e}")
        raise


@cli.command('sync-wantlist')
@click.option('--force', is_flag=True, help='Force refresh of all wantlist data')
@click.pass_context
def sync_wantlist(ctx, force):
    """Sync your Discogs wantlist with the local database."""
    config = ctx.obj['config']
    
    # Validate API configuration
    if not validate_config(config):
        click.echo("❌ Missing required API configuration.")
        click.echo("Please set DISCOGS_API_TOKEN and DISCOGS_USERNAME environment variables.")
        sys.exit(1)
    
    click.echo("🎵 Syncing wantlist from Discogs API...")
    
    try:
        asyncio.run(_sync_wantlist_async(config, force))
    except KeyboardInterrupt:
        click.echo("\n❌ Wantlist sync cancelled by user")
        sys.exit(1)
    except Exception as e:
        click.echo(f"\n❌ Wantlist sync failed: {e}")
        sys.exit(1)


async def _sync_wantlist_async(config, force):
    """Async wrapper for wantlist sync."""
    sync = CollectionSync(config)
    
    try:
        stats = await sync.sync_wantlist(force_refresh=force)
        
        click.echo("\n✅ Wantlist sync completed!")
        click.echo(f"📊 Statistics:")
        click.echo(f"   Items added: {stats['wantlist_items_added']}")
        click.echo(f"   Items updated: {stats['wantlist_items_updated']}")
        click.echo(f"   Releases fetched: {stats['releases_fetched']}")
        if stats['errors'] > 0:
            click.echo(f"   Errors: {stats['errors']}")
        
    except Exception as e:
        click.echo(f"❌ Wantlist sync failed: {e}")
        raise


@cli.command('collection-workflow')
@click.pass_context
def collection_workflow(ctx):
    """Guided workflow for collection-only strategy setup."""
    config = ctx.obj['config']
    
    click.echo("🎵 Collection-Only Workflow Guide")
    click.echo("================================")
    click.echo("This guide will help you set up DiscoStar with the 'collection_only' strategy.")
    click.echo("This strategy only imports releases that are in your personal collection,")
    click.echo("significantly reducing database size and improving performance.\n")
    
    # Check configuration
    release_strategy = config.get('ingestion', {}).get('releases', {}).get('strategy', 'all')
    if release_strategy != 'collection_only':
        click.echo("⚠️  Your configuration is not set to 'collection_only' strategy.")
        click.echo("💡 Update your config/settings.yaml file to:")
        click.echo("   ingestion:")
        click.echo("     releases:")
        click.echo("       strategy: \"collection_only\"")
        click.echo()
        if not click.confirm("Continue with current configuration?"):
            return
    
    # Check API configuration
    if not validate_config(config):
        click.echo("❌ Missing required API configuration.")
        click.echo("Please set DISCOGS_API_TOKEN and DISCOGS_USERNAME environment variables.")
        return
    
    try:
        from ..core.database.models import UserCollection, Artist, Label, Master, Release
        from ..core.database.database import get_database_url
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        
        database_url = get_database_url(config)
        engine = create_engine(database_url)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        session = SessionLocal()
        
        try:
            # Check current status
            collection_count = session.query(UserCollection).count()
            artist_count = session.query(Artist).count()
            label_count = session.query(Label).count()
            master_count = session.query(Master).count()
            release_count = session.query(Release).count()
            
            click.echo("📊 Current Database Status:")
            click.echo(f"   Artists: {artist_count:,}")
            click.echo(f"   Labels: {label_count:,}")
            click.echo(f"   Masters: {master_count:,}")
            click.echo(f"   Releases: {release_count:,}")
            click.echo(f"   Collection Items: {collection_count:,}")
            click.echo()
            
            # Determine next steps
            if artist_count == 0:
                click.echo("📋 Next Step: Download and ingest reference data")
                click.echo("   discostar download-dumps")
                click.echo("   discostar ingest-data --type artists")
                click.echo("   discostar ingest-data --type labels")
                click.echo("   discostar ingest-data --type masters")
            elif collection_count == 0:
                click.echo("📋 Next Step: Sync your collection")
                click.echo("   discostar sync-collection")
            elif release_count == 0:
                click.echo("📋 Next Step: Ingest collection releases")
                click.echo("   discostar ingest-data --type releases")
            else:
                click.echo("✅ Collection-only setup appears complete!")
                click.echo("💡 You can now run 'discostar optimize-db --clean-unused' to remove unused releases")
            
        finally:
            session.close()
            
    except Exception as e:
        click.echo(f"❌ Error checking workflow status: {e}")


@cli.command('process-relationships')
@click.pass_context
def process_relationships(ctx):
    """Process relationships for existing releases to populate join tables."""
    config = ctx.obj['config']
    
    click.echo("🔗 Processing release relationships...")
    click.echo("This will populate join tables (release_artists, release_labels, tracks)")
    click.echo("from existing release JSON data.\n")
    
    try:
        # Get ingestion pipeline
        pipeline = get_ingestion_pipeline(config)
        
        # Check if there are releases to process
        from ..core.database.models import Release
        from ..core.database.database import get_database_url
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        
        database_url = get_database_url(config)
        engine = create_engine(database_url)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        session = SessionLocal()
        
        try:
            release_count = session.query(Release).count()
            
            if release_count == 0:
                click.echo("❌ No releases found in database. Ingest release data first.")
                return
            
            click.echo(f"📊 Found {release_count:,} releases to process")
            
            if not click.confirm("Continue with relationship processing?"):
                return
            
        finally:
            session.close()
        
        # Process relationships
        stats = pipeline.process_existing_relationships()
        
        if stats.get('errors', 0) > 0:
            click.echo(f"⚠️  Processing completed with {stats['errors']} errors")
        else:
            click.echo("✅ Relationship processing completed successfully!")
            
        click.echo(f"📊 Results:")
        click.echo(f"   Releases processed: {stats.get('releases_processed', 0):,}")
        click.echo(f"   Artist relationships: {stats.get('artists_created', 0):,}")
        click.echo(f"   Label relationships: {stats.get('labels_created', 0):,}")
        click.echo(f"   Tracks created: {stats.get('tracks_created', 0):,}")
        
    except Exception as e:
        click.echo(f"❌ Error processing relationships: {e}")
        sys.exit(1)


@cli.command('optimize-db')
@click.option('--strategy', type=click.Choice(['collection_only', 'all']), 
              help='Set release storage strategy')
@click.option('--clean-unused', is_flag=True, 
              help='Remove releases not in any collection')
@click.confirmation_option(prompt='This will modify your database. Continue?')
@click.pass_context
def optimize_db(ctx, strategy, clean_unused):
    """Optimize database for collection-focused usage."""
    config = ctx.obj['config']
    
    try:
        from ..core.database.models import UserCollection, Release
        from ..core.database.database import get_database_url
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        
        # Check if we have collection data
        database_url = get_database_url(config)
        engine = create_engine(database_url)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        session = SessionLocal()
        
        try:
            collection_count = session.query(UserCollection).count()
            
            if collection_count == 0:
                click.echo("❌ No collection data found. Sync your collection first with:")
                click.echo("   discostar sync-collection")
                return
            
            click.echo(f"📊 Found {collection_count:,} items in collections")
            
            if clean_unused:
                # Get collection release IDs
                collection_release_ids = {r[0] for r in session.query(UserCollection.release_id).distinct().all()}
                
                # Count total releases
                total_releases = session.query(Release).count()
                
                # Count releases not in collections
                unused_releases = session.query(Release).filter(
                    ~Release.id.in_(collection_release_ids)
                ).count()
                
                click.echo(f"📈 Database contains {total_releases:,} total releases")
                click.echo(f"🗑️  Found {unused_releases:,} releases not in any collection")
                
                if unused_releases > 0:
                    click.echo("Removing unused releases...")
                    
                    # Delete unused releases
                    deleted = session.query(Release).filter(
                        ~Release.id.in_(collection_release_ids)
                    ).delete(synchronize_session=False)
                    
                    session.commit()
                    
                    click.echo(f"✅ Removed {deleted:,} unused releases")
                    click.echo(f"💾 Database size reduced by ~{(deleted/total_releases)*100:.1f}%")
                else:
                    click.echo("✅ No unused releases found")
            
            if strategy:
                click.echo(f"📝 Updating release strategy to: {strategy}")
                # Note: This would require updating the config file
                click.echo("💡 Update your config/settings.yaml file to:")
                click.echo(f"   ingestion.releases.strategy: \"{strategy}\"")
            
        finally:
            session.close()
            
    except Exception as e:
        click.echo(f"❌ Error optimizing database: {e}")
        sys.exit(1)


def main():
    """Entry point for the CLI."""
    cli()


if __name__ == '__main__':
    main()