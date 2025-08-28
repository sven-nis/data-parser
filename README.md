# HTML to Markdown Converter for GCS with PostgreSQL

A Python script that converts HTML files to Markdown format by querying a PostgreSQL database for files with 'ingested' status and processing the corresponding GCS files. The script handles gzipped files and intelligently filters out navigation, footer, and other non-content elements while preserving text and tables.

## Features

- **PostgreSQL Integration**: Queries database for files with 'ingested' status
- **GCS Integration**: Reads from and writes to Google Cloud Storage buckets
- **Gzip Support**: Automatically handles gzipped HTML files
- **Content Filtering**: Removes navigation, footer, sidebar, and advertisement elements
- **Table Preservation**: Properly converts HTML tables to Markdown format
- **Sibling Directory Output**: Saves Markdown files to a sibling 'markdown' directory
- **Status Tracking**: Updates database status to 'converted' or 'failed' after processing
- **Comprehensive Logging**: Detailed logging with configurable levels
- **Error Handling**: Robust error handling with file-level success tracking

## Installation
1. Install uv:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

2. Install dependencies (including dev dependencies):
```bash
uv sync
```

## Linting
```bash
uv run ruff format src tests
```

```bash
uv run mypy src tests
```

## Configuration

The script uses environment variables for configuration:

### Required Variables
- `DB_HOST`: PostgreSQL database host
- `DB_NAME`: PostgreSQL database name
- `DB_USER`: PostgreSQL database username
- `DB_PASSWORD`: PostgreSQL database password

### Optional Variables
- `DB_PORT`: PostgreSQL database port (defaults to 5432)
- `LOG_LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR) - defaults to INFO
- `GOOGLE_APPLICATION_CREDENTIALS`: Path to your service account key file

### Example Environment Setup

Create a `.env` file in the project root:

```bash
# Required Database Configuration
DB_HOST=localhost
DB_NAME=file_processing
DB_USER=postgres
DB_PASSWORD=your_password

# Optional
DB_PORT=5432
LOG_LEVEL=INFO
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
```

## Usage

### Basic Usage

```bash
uv run python src/main.py
```

### Environment Variables on Command Line

```bash
DB_HOST=localhost DB_NAME=mydb DB_USER=user DB_PASSWORD=pass uv run python src/main.py
```

## Database Schema

The script expects a PostgreSQL table named `files` with at least these columns:

```sql
CREATE TABLE files (
    id SERIAL PRIMARY KEY,
    gcs_path VARCHAR(255) NOT NULL,  -- Full GCS path like 'gs://bucket/path/file.html.gz'
    status VARCHAR(50) NOT NULL,     -- Status: 'ingested', 'converted', 'failed', etc.
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## How It Works

1. **Database Query**: Connects to PostgreSQL and queries for files with status = 'ingested'
2. **GCS Path Parsing**: Extracts bucket and object path from the `gcs_path` field
3. **File Download**: Downloads and decompresses gzipped HTML files from GCS
4. **HTML Cleaning**: Removes unwanted elements using BeautifulSoup:
   - Navigation elements (`nav`, `.navbar`, `#navigation`, etc.)
   - Footer elements (`footer`, `.footer`, etc.)
   - Sidebar elements (`.sidebar`, `aside`, etc.)
   - Advertisement elements (`.ads`, `.advertisement`, etc.)
   - Script and style tags
   - Empty elements
5. **Markdown Conversion**: Converts cleaned HTML to Markdown with proper table handling
6. **Sibling Directory Upload**: Saves the converted Markdown to a 'markdown' subdirectory in the same bucket
7. **Status Update**: Updates the database record status to 'converted' or 'failed'

## HTML Elements Filtered Out

The script automatically removes these types of elements:
- Navigation: `nav`, `.navigation`, `.navbar`, `.menu`, `#nav`
- Headers/Footers: `header`, `footer`, `.site-footer`, `#footer`
- Sidebars: `aside`, `.sidebar`
- Breadcrumbs: `.breadcrumb`, `.breadcrumbs`
- Social/Sharing: `.social`, `.share`, `.sharing`
- Advertisements: `.advertisement`, `.ads`, `.ad`
- Cookie notices: `.cookie-notice`, `.cookie-banner`
- Scripts and styles: `script`, `style`, `noscript`

## File Path Handling

The script creates a sibling 'markdown' directory for output files:

- Input: `gs://bucket/docs/document.html.gz` → Output: `gs://bucket/docs/markdown/document.md`
- Input: `gs://bucket/pages/page.html` → Output: `gs://bucket/pages/markdown/page.md`
- Input: `gs://bucket/files/file.htm.gz` → Output: `gs://bucket/files/markdown/file.md`

This keeps the converted files organized alongside the original HTML files.

## Error Handling

- Individual file failures don't stop the batch process
- Detailed error logging for debugging
- Returns exit code 0 for complete success, 1 for partial or complete failure
- Graceful handling of missing files, network issues, and parsing errors

## Dependencies

- `google-cloud-storage`: GCS client library
- `psycopg2-binary`: PostgreSQL database adapter
- `beautifulsoup4`: HTML parsing and cleaning
- `markitdown`: HTML to Markdown conversion
- `python-dotenv`: Environment variable management
- `lxml`: Fast XML/HTML parser for BeautifulSoup

## Requirements

- Python 3.12+
- PostgreSQL database with required table schema
- Google Cloud Storage access
- Valid GCS authentication setup

## Database Setup

Create the required table in your PostgreSQL database:

```sql
CREATE TABLE files (
    id SERIAL PRIMARY KEY,
    gcs_path VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for faster status queries
CREATE INDEX idx_files_status ON files(status);

-- Example data insertion
INSERT INTO files (gcs_path, status) VALUES 
('gs://my-bucket/docs/page1.html.gz', 'ingested'),
('gs://my-bucket/docs/page2.html.gz', 'ingested');
```

## Testing

The project includes comprehensive integration tests using [testcontainers](https://testcontainers-python.readthedocs.io/) to spin up real PostgreSQL and fake GCS servers.

### Running Tests

```bash
# Install dev dependencies (includes test dependencies)
uv sync --dev

# Run all tests
uv run pytest

# Run with verbose output
uv run pytest -v

# Run only integration tests
uv run pytest -m integration

# Run only unit tests
uv run pytest -m unit

# Run specific test file
uv run pytest tests/test_integration.py
```

### Test Architecture

The test suite uses:

- **PostgreSQL Container**: Real PostgreSQL database using [testcontainers](https://testcontainers-python.readthedocs.io/)
- **Fake GCS Server**: [fsouza/fake-gcs-server](https://github.com/fsouza/fake-gcs-server) for Google Cloud Storage emulation
- **Test Fixtures**: Comprehensive HTML samples and database fixtures
- **End-to-End Testing**: Complete workflow testing from database query to file conversion

### Test Coverage

Tests cover:

- ✅ Database operations (connection, queries, status updates)
- ✅ GCS file operations (upload, download, path parsing)
- ✅ HTML cleaning and content filtering
- ✅ Markdown conversion with table preservation
- ✅ End-to-end processing workflow
- ✅ Error handling (missing files, corrupted data, network issues)
- ✅ Unicode and special character handling
- ✅ Edge cases and malformed input

### Requirements for Testing

- Docker (for testcontainers)
- Python 3.12+
- All test dependencies installed via `uv sync --dev`

The tests automatically handle:
- Starting/stopping test containers
- Database schema creation
- Test data setup and cleanup
- GCS bucket creation and cleanup