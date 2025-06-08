# 🎵 DiscoStar

A powerful Python CLI tool for analyzing your personal record collection using Discogs data. DiscoStar combines XML data dumps with real-time API calls to provide deep insights into your music collection.

## ✨ Features

- **Hybrid Data Approach**: Combines Discogs XML dumps for reference data with API calls for personal collection
- **High-Performance Ingestion**: Memory-efficient XML parsing with batch processing
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

# Check ingestion status
discostar status

# Sync your collection from Discogs API (coming soon)
discostar sync-collection

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

### Benchmark Results
Tested with Discogs June 2025 XML dumps on a Macbook Pro M4:
- **Artists**: 270,000 records processed in ~30 seconds
- **Releases**: Estimated 8+ million records (full dataset)
- **Labels**: Estimated 1.5+ million records
- **Masters**: Estimated 2+ million records

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
discostar status                 # Show database and download status

# Advanced options
discostar download-dumps --type artists  # Download specific dump type
discostar ingest-data --type releases    # Import specific data type
discostar ingest-data --force            # Force re-ingestion
discostar clear-data --type artists      # Clear specific data type

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
- API rate limiting
- Logging configuration
- Cache settings

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
- **Async Processing**: aiohttp for concurrent API operations
- **Testing**: Pytest with async support

## ☁️ Deployment

### Azure Deployment

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
