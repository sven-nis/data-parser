"""
Pytest configuration and fixtures for integration tests.
"""

import gzip
import os
from typing import Generator, Dict

import pytest
import psycopg
import requests
from google.cloud import storage
from testcontainers.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer


@pytest.fixture
def postgres_container() -> Generator[PostgresContainer, None, None]:
    """Start a PostgreSQL container for testing."""
    with PostgresContainer("postgres:16") as postgres:
        yield postgres


@pytest.fixture(scope="session")
def fake_gcs_container() -> Generator[DockerContainer, None, None]:
    """Start a fake GCS server container for testing."""
    with (
        DockerContainer("fsouza/fake-gcs-server:latest")
        .with_exposed_ports(4443)
        .with_command("-scheme http") as gcs
    ):
        yield gcs


@pytest.fixture
def db_connection(
    postgres_container: PostgresContainer,
) -> Generator[Dict[str, str], None, None]:
    """Provide database connection parameters."""
    db_params = {
        "host": postgres_container.get_container_host_ip(),
        "port": postgres_container.get_exposed_port(5432),
        "dbname": postgres_container.dbname,
        "user": postgres_container.username,
        "password": postgres_container.password,
    }
    # Create the files table
    with psycopg.connect(**db_params) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS files (
                    id SERIAL PRIMARY KEY,
                    gcs_path VARCHAR(255) NOT NULL,
                    status VARCHAR(50) NOT NULL DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_files_status ON files(status);"
            )
            conn.commit()

    yield db_params


@pytest.fixture
def gcs_client(
    fake_gcs_container: DockerContainer,
) -> Generator[storage.Client, None, None]:
    """Provide a GCS client configured for the fake server."""
    host = fake_gcs_container.get_container_host_ip()
    port = fake_gcs_container.get_exposed_port(4443)
    # Set environment variable for the fake GCS server
    os.environ["STORAGE_EMULATOR_HOST"] = f"http://{host}:{port}"
    print(f"STORAGE_EMULATOR_HOST: {os.environ['STORAGE_EMULATOR_HOST']}")
    # Create client
    client = storage.Client(project="test-project")

    yield client

    # Cleanup
    if "STORAGE_EMULATOR_HOST" in os.environ:
        del os.environ["STORAGE_EMULATOR_HOST"]


@pytest.fixture
def test_bucket(gcs_client: storage.Client) -> Generator[storage.Bucket, None, None]:
    """Create a test bucket for testing."""
    bucket_name = "test-bucket"
    bucket = gcs_client.create_bucket(bucket_name)

    yield bucket

    # Cleanup
    try:
        bucket.delete(force=True)
    except Exception:
        pass  # Ignore cleanup errors


class TestHTMLSamples:
    """Collection of HTML samples for testing various scenarios."""

    @staticmethod
    def get_minimal_html() -> str:
        """Minimal valid HTML document."""
        return """
        <!DOCTYPE html>
        <html>
        <head><title>Minimal</title></head>
        <body>
            <h1>Hello World</h1>
        </body>
        </html>
        """

    @staticmethod
    def get_navigation_heavy_html() -> str:
        """HTML with lots of navigation elements to test filtering."""
        return """
        <!DOCTYPE html>
        <html>
        <head><title>Navigation Test</title></head>
        <body>
            <header class="site-header">
                <nav class="main-nav">
                    <ul>
                        <li><a href="/">Home</a></li>
                        <li><a href="/about">About</a></li>
                    </ul>
                </nav>
            </header>
            
            <div class="navbar">Secondary nav</div>
            <div id="navigation">Tertiary nav</div>
            
            <aside class="sidebar">
                <nav class="sidebar-nav">Sidebar navigation</nav>
            </aside>
            
            <main>
                <h1>Actual Content</h1>
                <p>This is the main content that should be preserved.</p>
            </main>
            
            <div class="breadcrumbs">Home > Category > Page</div>
            <div class="social">Share on social media</div>
            
            <footer class="site-footer">
                <nav class="footer-nav">Footer navigation</nav>
                <p>Copyright notice</p>
            </footer>
        </body>
        </html>
        """

    @staticmethod
    def get_table_heavy_html() -> str:
        """HTML with complex tables to test table conversion."""
        return """
        <!DOCTYPE html>
        <html>
        <head><title>Table Test</title></head>
        <body>
            <h1>Data Tables</h1>
            
            <h2>Simple Table</h2>
            <table>
                <tr>
                    <th>Name</th>
                    <th>Age</th>
                    <th>City</th>
                </tr>
                <tr>
                    <td>John</td>
                    <td>30</td>
                    <td>New York</td>
                </tr>
                <tr>
                    <td>Jane</td>
                    <td>25</td>
                    <td>Boston</td>
                </tr>
            </table>
            
            <h2>Complex Table</h2>
            <table class="data-table">
                <thead>
                    <tr>
                        <th colspan="2">Personal Info</th>
                        <th rowspan="2">Score</th>
                    </tr>
                    <tr>
                        <th>First Name</th>
                        <th>Last Name</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td>Alice</td>
                        <td>Smith</td>
                        <td>95</td>
                    </tr>
                    <tr>
                        <td>Bob</td>
                        <td>Jones</td>
                        <td>87</td>
                    </tr>
                </tbody>
            </table>
            
            <h2>Empty Table</h2>
            <table>
                <tr><th>Header</th></tr>
            </table>
        </body>
        </html>
        """

    @staticmethod
    def get_script_heavy_html() -> str:
        """HTML with lots of scripts and styles to test removal."""
        return """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Script Test</title>
            <style>
                body { background-color: #f0f0f0; }
                .container { max-width: 800px; }
            </style>
            <script>
                function initPage() {
                    console.log('Page initialized');
                }
            </script>
        </head>
        <body onload="initPage()">
            <h1>Content with Scripts</h1>
            
            <p>This paragraph should remain.</p>
            
            <script>
                // Inline script
                document.addEventListener('DOMContentLoaded', function() {
                    alert('Page loaded');
                });
            </script>
            
            <div>
                <p>More content here.</p>
                <noscript>JavaScript is disabled</noscript>
            </div>
            
            <style>
                .special { color: red; }
            </style>
            
            <!-- This is a comment -->
            <p>Final paragraph.</p>
        </body>
        </html>
        """

    @staticmethod
    def get_unicode_html() -> str:
        """HTML with Unicode characters to test encoding."""
        return """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <title>Unicode Test</title>
        </head>
        <body>
            <h1>Unicode Characters: 你好世界</h1>
            <p>Emoji: 🚀 🌟 ✨</p>
            <p>Symbols: © ® ™ € £ ¥</p>
            <p>Accented: café, naïve, résumé</p>
            <p>Math: α + β = γ, ∑ᵢ₌₁ⁿ xᵢ</p>
            
            <table>
                <tr>
                    <th>Language</th>
                    <th>Hello</th>
                </tr>
                <tr>
                    <td>Chinese</td>
                    <td>你好</td>
                </tr>
                <tr>
                    <td>Japanese</td>
                    <td>こんにちは</td>
                </tr>
                <tr>
                    <td>Korean</td>
                    <td>안녕하세요</td>
                </tr>
            </table>
        </body>
        </html>
        """

    @staticmethod
    def get_malformed_html() -> str:
        """Malformed HTML to test error handling."""
        return """
        <html>
        <head><title>Malformed</title>
        <body>
            <h1>Unclosed header
            <p>Paragraph without closing tag
            <div>
                <span>Nested elements
            </div>
            <table>
                <tr><td>Incomplete table
        </html>
        """


@pytest.fixture
def sample_html_files() -> Dict[str, str]:
    html_samples = TestHTMLSamples()
    """Provide comprehensive sample HTML content for testing."""
    return {
        "minimal.html": html_samples.get_minimal_html(),
        "navigation.html": html_samples.get_navigation_heavy_html(),
        "tables.html": html_samples.get_table_heavy_html(),
        "scripts.html": html_samples.get_script_heavy_html(),
        "unicode.html": html_samples.get_unicode_html(),
        "malformed.html": html_samples.get_malformed_html(),
        # Keep the original simple and complex for backward compatibility
        "simple.html": """
        <!DOCTYPE html>
        <html>
        <head><title>Test Page</title></head>
        <body>
            <nav>Navigation content</nav>
            <main>
                <h1>Main Title</h1>
                <p>This is a test paragraph.</p>
                <table>
                    <tr><th>Header 1</th><th>Header 2</th></tr>
                    <tr><td>Cell 1</td><td>Cell 2</td></tr>
                </table>
            </main>
            <footer>Footer content</footer>
        </body>
        </html>
        """,
        "complex.html": """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Complex Page</title>
            <style>body { margin: 0; }</style>
        </head>
        <body>
            <header class="site-header">Header</header>
            <nav class="navbar">
                <ul>
                    <li><a href="/">Home</a></li>
                    <li><a href="/about">About</a></li>
                </ul>
            </nav>
            <div class="container">
                <aside class="sidebar">Sidebar content</aside>
                <main class="content">
                    <h1>Article Title</h1>
                    <p>First paragraph with <strong>bold text</strong>.</p>
                    <h2>Subsection</h2>
                    <ul>
                        <li>List item 1</li>
                        <li>List item 2</li>
                    </ul>
                    <table class="data-table">
                        <thead>
                            <tr><th>Name</th><th>Value</th><th>Description</th></tr>
                        </thead>
                        <tbody>
                            <tr><td>Item A</td><td>100</td><td>First item</td></tr>
                            <tr><td>Item B</td><td>200</td><td>Second item</td></tr>
                        </tbody>
                    </table>
                </main>
            </div>
            <div class="advertisement">Ad content</div>
            <footer class="site-footer">
                <div class="social">Social links</div>
                <p>&copy; 2024 Test Site</p>
            </footer>
            <script>console.log('test');</script>
        </body>
        </html>
        """,
    }


@pytest.fixture
def uploaded_test_files(
    test_bucket: storage.Bucket, sample_html_files: Dict[str, str]
) -> Dict[str, str]:
    """Upload test HTML files to GCS and return their paths."""
    file_paths = {}

    for filename, content in sample_html_files.items():
        # Compress the content
        compressed_content = gzip.compress(content.encode("utf-8"))

        # Upload to GCS
        blob_name = f"docs/{filename}.gz"
        blob = test_bucket.blob(blob_name)
        blob.upload_from_string(compressed_content, content_type="application/gzip")

        file_paths[filename] = f"gs://{test_bucket.name}/{blob_name}"

    return file_paths


@pytest.fixture
def env_vars(db_connection: Dict[str, str]) -> Generator[Dict[str, str], None, None]:
    """Set up environment variables for testing."""
    original_env = {}
    test_env = {
        "DB_HOST": db_connection["host"],
        "DB_PORT": str(db_connection["port"]),
        "DB_NAME": db_connection["dbname"],
        "DB_USER": db_connection["user"],
        "DB_PASSWORD": db_connection["password"],
        "LOG_LEVEL": "DEBUG",
    }

    # Backup original values and set test values
    for key, value in test_env.items():
        original_env[key] = os.environ.get(key)
        os.environ[key] = value

    yield test_env

    # Restore original values
    for key, value in original_env.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


@pytest.fixture
def expected_markdown_outputs():
    """Expected markdown outputs for validation."""
    return {
        "minimal": {"contains": ["# Hello World"], "not_contains": []},
        "navigation": {
            "contains": ["# Actual Content", "main content that should be preserved"],
            "not_contains": [
                "main-nav",
                "Secondary nav",
                "Tertiary nav",
                "Sidebar navigation",
                "Footer navigation",
                "Home > Category > Page",
                "Share on social media",
            ],
        },
        "tables": {
            "contains": [
                "# Data Tables",
                "## Simple Table",
                "Name",
                "Age",
                "City",
                "John",
                "30",
                "New York",
                "## Complex Table",
                "Alice",
                "Smith",
                "95",
            ],
            "not_contains": [],
        },
        "scripts": {
            "contains": [
                "# Content with Scripts",
                "This paragraph should remain",
                "More content here",
                "Final paragraph",
            ],
            "not_contains": [
                "console.log",
                "alert",
                "background-color",
                "addEventListener",
                "color: red",
                "JavaScript is disabled",
            ],
        },
        "unicode": {
            "contains": [
                "你好世界",
                "🚀",
                "🌟",
                "✨",
                "café",
                "naïve",
                "résumé",
                "α + β = γ",
                "你好",
                "こんにちは",
                "안녕하세요",
            ],
            "not_contains": [],
        },
    }
