# 🎵 DiscoStar

A powerful Python CLI tool for analyzing your personal record collection using Discogs data. DiscoStar combines XML data dumps with real-time API calls to provide deep insights into your music collection.

## ✨ Features

- **Hybrid Data Approach**: Combines Discogs XML dumps for reference data with API calls for personal collection
- **Rich Analytics**: Genre distribution, release timeline analysis, label statistics, and more
- **Async Processing**: Fast, concurrent API calls for efficient data retrieval
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
# Download and process Discogs XML dumps
discostar download-dumps

# Sync your collection from Discogs API
discostar sync-collection

# Generate collection statistics
discostar stats

# Analyze genre distribution
discostar analyze genres

# Show release timeline
discostar analyze timeline
```

## 📊 Analytics Features

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

DiscoStar uses YAML configuration with environment variable overrides:

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
