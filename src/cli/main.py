#!/usr/bin/env python3

import asyncio
import sys
from pathlib import Path

import click

from ..core.utils.config import load_config, setup_logging
from ..core.discogs.xml_downloader import DiscogsDumpDownloader


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
    
    click.echo("✅ DiscoStar initialization complete!")
    click.echo("\nNext steps:")
    click.echo("1. Set up your .env file with DISCOGS_API_TOKEN")
    click.echo("2. Run 'discostar download-dumps' to fetch Discogs data")


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


def main():
    """Entry point for the CLI."""
    cli()


if __name__ == '__main__':
    main()