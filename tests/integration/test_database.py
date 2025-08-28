"""
Tests for database operations.
"""

import psycopg
from typing import Dict
from unittest.mock import MagicMock
from src.main import HTMLToMarkdownConverter


def test_get_ingested_files_empty(
    db_connection: Dict[str, str], env_vars: Dict[str, str]
):
    """Test querying for ingested files when table is empty."""
    converter = HTMLToMarkdownConverter(gcs_client=MagicMock())
    files = converter.get_ingested_files()
    assert files == []


def test_get_ingested_files_with_data(
    db_connection: Dict[str, str], env_vars: Dict[str, str]
):
    """Test querying for ingested files with test data."""
    # Insert test data
    with psycopg.connect(**db_connection) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO files (gcs_path, status) VALUES 
                ('gs://bucket1/file1.html.gz', 'ingested'),
                ('gs://bucket1/file2.html.gz', 'pending'),
                ('gs://bucket1/file3.html.gz', 'ingested'),
                ('gs://bucket1/file4.html.gz', 'converted')
            """)
            conn.commit()

    converter = HTMLToMarkdownConverter(gcs_client=MagicMock())
    files = converter.get_ingested_files()

    # Should only return files with 'ingested' status
    assert len(files) == 2
    assert all(f["status"] == "ingested" for f in files)

    gcs_paths = [f["gcs_path"] for f in files]
    assert "gs://bucket1/file1.html.gz" in gcs_paths
    assert "gs://bucket1/file3.html.gz" in gcs_paths


def test_update_file_status(db_connection: Dict[str, str], env_vars: Dict[str, str]):
    """Test updating file status in the database."""
    # Insert test data
    with psycopg.connect(**db_connection) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO files (gcs_path, status) VALUES 
                ('gs://bucket1/test.html.gz', 'ingested')
                RETURNING id
            """)
            file_id = cursor.fetchone()[0]
            conn.commit()

    converter = HTMLToMarkdownConverter(gcs_client=MagicMock())

    # Update status to converted
    success = converter.update_file_status(file_id, "converted")
    assert success

    # Verify the update
    with psycopg.connect(**db_connection) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT status FROM files WHERE id = %s", (file_id,))
            status = cursor.fetchone()[0]
            assert status == "converted"


def test_parse_gcs_path(env_vars: Dict[str, str]):
    """Test parsing GCS paths."""
    converter = HTMLToMarkdownConverter()

    # Test valid paths
    bucket, obj = converter.parse_gcs_path("gs://my-bucket/path/to/file.html.gz")
    assert bucket == "my-bucket"
    assert obj == "path/to/file.html.gz"

    bucket, obj = converter.parse_gcs_path("gs://test-bucket/simple.html")
    assert bucket == "test-bucket"
    assert obj == "simple.html"

    # Test invalid path
    try:
        converter.parse_gcs_path("invalid://path/file.html")
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "Invalid GCS path format" in str(e)


def test_get_sibling_markdown_path(env_vars: Dict[str, str]):
    """Test generating sibling markdown paths."""
    converter = HTMLToMarkdownConverter(gcs_client=MagicMock())

    # Test .html.gz files
    result = converter.get_sibling_markdown_path("docs/page.html.gz")
    assert result == "docs/markdown/page.md"

    # Test .html files
    result = converter.get_sibling_markdown_path("articles/article.html")
    assert result == "articles/markdown/article.md"

    # Test .htm.gz files
    result = converter.get_sibling_markdown_path("pages/index.htm.gz")
    assert result == "pages/markdown/index.md"

    # Test files in root
    result = converter.get_sibling_markdown_path("readme.html.gz")
    assert result == "markdown/readme.md"

    # Test nested paths
    result = converter.get_sibling_markdown_path("a/b/c/file.html.gz")
    assert result == "a/b/c/markdown/file.md"
