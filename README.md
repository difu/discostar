# 🎵 DiscoStar

A powerful Python CLI tool for analyzing your personal record collection using Discogs data. DiscoStar combines XML data dumps with real-time API calls to provide deep insights into your music collection.

## ✨ Features

- **Hybrid Data Approach**: Combines Discogs XML dumps for reference data with API calls for personal collection
- **Collection Sync**: Sync your personal collection from Discogs API with real-time progress tracking
- **High-Performance Ingestion**: Memory-efficient XML parsing with batch processing (10,000+ records/second)
- **Rate-Limited API Client**: Respects Discogs API limits with configurable SSL handling
- **Real-time Progress Tracking**: Visual progress indicators and detailed status reporting
- **Robust Error Handling**: Comprehensive error recovery with sub-1% error rates
- **Local Database**: SQLite for development, with Azure PostgreSQL support for production
- **CLI Interface**: Clean command-line interface for all operations
- **Web Interface**: Future Flask-based web dashboard (coming soon)
- **Cloud Ready**: Terraform infrastructure for Azure deployment

## 🚀 Quick Start

### Prerequisites

- Python 3.9 or higher
- Discogs account and API token

### Installation

1. Clone the repository:
```bash
git clone https://github.com/difu/discostar.git
cd discostar
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements-dev.txt
```

4. Set up configuration:
```bash
cp .env.example .env
# Edit .env with your Discogs API token and username
```

5. Initialize the database:
```bash
discostar init
```

### Basic Usage

```bash
# Download Discogs XML dumps
discostar download-dumps

# Import XML data into database
discostar ingest-data

# Sync your personal collection from Discogs API
discostar sync-collection

# Check ingestion and sync status
discostar status

# Generate collection statistics (coming soon)
discostar stats
```

## ⚡ Performance Metrics

DiscoStar is optimized for processing large Discogs datasets efficiently:

### XML Ingestion Performance
- **Processing Speed**: ~10,000 records/second
- **Memory Efficiency**: Uses iterative XML parsing for files >1GB
- **Error Rate**: <0.001% (sub-1% error tolerance)
- **Batch Processing**: Configurable batch sizes (default: 1,000 records)
- **Progress Tracking**: Real-time updates every 10,000 records

### Database Performance
- **Batch Commits**: Every 10,000 records to optimize transaction overhead
- **Memory Usage**: Minimal memory footprint with streaming processing
- **Storage**: SQLite for local development, PostgreSQL for production scale

### API Performance
- **Collection Sync**: 603 collection items synced in ~8 seconds
- **Rate Limiting**: 60 requests/minute with 1-second minimum between requests
- **Error Recovery**: Automatic retry logic for transient API failures
- **Progress Tracking**: Real-time statistics during sync operations

### Benchmark Results
Tested with Discogs June 2025 XML dumps on a Macbook Pro M4:
- **Artists**: 1,060,000+ records processed in ~2 minutes
- **Collection Sync**: 603 personal collection items in ~8 seconds
- **Releases**: Estimated 8+ million records (full dataset)
- **Labels**: Estimated 1.5+ million records
- **Masters**: Estimated 2+ million records

## 🔗 Database Schema & Relationships

DiscoStar uses a normalized database schema with both JSON fields and relational join tables for optimal flexibility:

### Data Storage Approach
- **JSON Fields**: Store raw Discogs data in JSON format for completeness
- **Join Tables**: Normalized relationships for efficient queries and analytics
- **Hybrid Benefits**: Maintains data integrity while enabling complex SQL queries

### Join Tables
DiscoStar automatically populates join tables during release ingestion:

| Table | Purpose | Example Query |
|-------|---------|---------------|
| **`release_artists`** | Artist-release relationships with roles | Find all releases by producer |
| **`release_labels`** | Label-release relationships with catalog numbers | Group releases by label |
| **`tracks`** | Individual track listings with positions | Search for specific songs |

### Relationship Processing
```bash
# Automatic: Join tables populated during release ingestion
discostar ingest-data --type releases

# Manual: Process existing releases to populate join tables  
discostar process-relationships

# Check results
discostar status  # Shows join table counts
```

### Query Examples
With join tables populated, you can run complex analytics:

```sql
-- Find all releases where Artist X collaborated with Artist Y
SELECT r.title FROM releases r
JOIN release_artists ra1 ON r.id = ra1.release_id  
JOIN release_artists ra2 ON r.id = ra2.release_id
WHERE ra1.artist_id = 1 AND ra2.artist_id = 2;

-- Count releases by label
SELECT l.name, COUNT(*) FROM labels l
JOIN release_labels rl ON l.id = rl.label_id
GROUP BY l.name ORDER BY COUNT(*) DESC;

-- Find longest tracks in collection
SELECT r.title, t.title, t.duration FROM tracks t
JOIN releases r ON t.release_id = r.id
ORDER BY t.duration_seconds DESC LIMIT 10;

-- Find favorite decade based on collection (earliest version of each master release only)
  WITH earliest_releases AS (
      SELECT
          r.master_id,
          MIN(CAST(strftime('%Y', r.released) AS INTEGER)) as earliest_year
      FROM releases r
      INNER JOIN user_collection uc ON r.id = uc.release_id
      WHERE r.master_id IS NOT NULL
        AND r.released IS NOT NULL
        AND strftime('%Y', r.released) IS NOT NULL
      GROUP BY r.master_id

      UNION ALL

      -- Include releases without master_id (standalone releases)
      SELECT
          NULL as master_id,
          CAST(strftime('%Y', r.released) AS INTEGER) as earliest_year
      FROM releases r
      INNER JOIN user_collection uc ON r.id = uc.release_id
      WHERE r.master_id IS NULL
        AND r.released IS NOT NULL
        AND strftime('%Y', r.released) IS NOT NULL
  ),
  decade_counts AS (
      SELECT
          (earliest_year / 10) * 10 as decade_start,
          COUNT(*) as release_count
      FROM earliest_releases
      GROUP BY (earliest_year / 10) * 10
  )
  SELECT
      decade_start,
      (decade_start || 's') as decade,
      release_count,
      ROUND(100.0 * release_count / SUM(release_count) OVER(), 2) as percentage
  FROM decade_counts
  ORDER BY release_count DESC;
```

## 💾 Storage Strategy

DiscoStar offers flexible release data management to balance completeness with performance:

### Release Storage Options

| Strategy | Records | Use Case | Storage | Query Speed |
|----------|---------|----------|---------|-------------|
| **`all`** | 8M+ releases | Complete dataset, discovery | ~2GB+ | Slower |
| **`skip`** | 0 releases | Collection-only analysis | ~50MB | Fastest |
| **`collection_only`** | 100s-1000s | Personal collection focus | ~100MB | Fast |

### Recommended Workflow

```bash
# Option 1: Start with essential data only
echo "strategy: skip" >> config/settings.yaml
discostar ingest-data --type artists,labels,masters
# Later: sync collection via API

# Option 2: Import everything, optimize later  
discostar ingest-data  # All data including 8M+ releases
# After collection sync:
discostar optimize-db --clean-unused  # Remove unused releases
```

### Configuration

Edit `config/settings.yaml`:
```yaml
ingestion:
  releases:
    strategy: "skip"  # or "all", "collection_only"
```

## 📊 Analytics Features (Coming Soon)

- **Genre Analysis**: Breakdown of your collection by genre and subgenre
- **Timeline Visualization**: See how your collection spans different decades
- **Label Statistics**: Most collected labels and their distribution
- **Artist Insights**: Top artists in your collection
- **Format Analysis**: Vinyl, CD, digital distribution
- **Value Tracking**: Collection value trends over time
- **Rarity Metrics**: Identify rare releases in your collection

## 🏗️ Architecture

```
discostar/
├── src/
│   ├── core/           # Shared business logic
│   │   ├── database/   # Database models and operations
│   │   ├── discogs/    # API client and XML processing
│   │   ├── analytics/  # Statistical analysis
│   │   └── utils/      # Utilities and configuration
│   ├── cli/            # Command-line interface
│   └── web/            # Web interface (future)
├── infrastructure/     # Azure deployment resources
├── tests/             # Test suite
├── data/              # Local data storage
└── config/            # Configuration files
```

## 🔧 Configuration

DiscoStar uses YAML configuration with environment variable overrides.

### Available CLI Commands

```bash
# Core commands
discostar init                    # Initialize database and directories
discostar download-dumps          # Download all XML dumps
discostar ingest-data            # Import XML data into database
discostar sync-collection        # Sync your collection from Discogs API
discostar status                 # Show database and sync status

# Collection sync options
discostar sync-collection --force       # Force refresh of collection data
discostar sync-wantlist                 # Sync wantlist (coming soon)

# Advanced XML ingestion options
discostar download-dumps --type artists  # Download specific dump type
discostar ingest-data --type releases    # Import specific data type
discostar ingest-data --force            # Force re-ingestion
discostar clear-data --type artists      # Clear specific data type

# Relationship processing (join tables)
discostar process-relationships          # Populate join tables from release JSON data

# Collection-only workflow guidance
discostar collection-workflow            # Interactive guide for collection-only setup

# Database optimization (after collection sync)
discostar optimize-db --clean-unused     # Remove releases not in collections

# Verbose logging
discostar -v <command>           # Enable detailed logging
```

### Environment Variables

```bash
# Required
DISCOGS_API_TOKEN=your_discogs_api_token
DISCOGS_USERNAME=your_username

# Optional
DATABASE_URL=sqlite:///data/discostar.db
AZURE_STORAGE_CONNECTION_STRING=your_azure_connection
```

### Configuration File

See `config/settings.yaml` for detailed configuration options including:
- Database settings
- Discogs API configuration and rate limiting
- SSL verification settings (for development environments)
- Logging configuration
- Cache settings
- XML ingestion batch processing parameters

## 🧪 Development

### Running Tests

```bash
pytest
```

### Code Quality

```bash
# Format code
black src/ tests/

# Lint code
flake8 src/ tests/

# Type checking
mypy src/
```

### Project Structure

- **Core Modules**: Business logic separated into focused modules
- **CLI Interface**: Click-based command structure
- **Database Layer**: SQLAlchemy models matching Discogs schema
- **API Client**: Async HTTP client with rate limiting and error handling
- **Collection Sync**: Real-time synchronization with progress tracking
- **Async Processing**: aiohttp for concurrent API operations
- **Testing**: Pytest with async support

## ☁️ Deployment

### Azure Deployment

__TODO__ nothing done yet 😊

1. Configure Azure credentials
2. Deploy infrastructure:
```bash
cd infrastructure/terraform
terraform init
terraform plan
terraform apply
```

3. Deploy application:
```bash
# Build and push Docker container
docker build -t discostar .
# Deploy to Azure Container Instances
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Make your changes
4. Add tests for new functionality
5. Run the test suite: `pytest`
6. Format code: `black src/ tests/`
7. Submit a pull request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [Discogs](https://www.discogs.com/) for providing the comprehensive music database and API
- The open-source community for the excellent Python libraries that make this project possible

## 📞 Support

- Create an [issue](https://github.com/yourusername/discostar/issues) for bug reports or feature requests
- Check the [documentation](https://github.com/yourusername/discostar/wiki) for detailed guides

---

**DiscoStar** - Illuminate your music collection with data-driven insights! ⭐
