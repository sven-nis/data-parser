"""
End-to-end integration tests using testcontainers.
"""

import gzip
import psycopg
import os
from typing import Dict
from google.cloud import storage
from src.main import HTMLToMarkdownConverter, main


def test_temp(
    db_connection: Dict[str, str],
    test_bucket: storage.Bucket,
    uploaded_test_files: Dict[str, str],
    env_vars: Dict[str, str],
):
    print(uploaded_test_files)
    print(os.environ["STORAGE_EMULATOR_HOST"])


def test_end_to_end_conversion(
    db_connection: Dict[str, str],
    gcs_client: storage.Client,
    test_bucket: storage.Bucket,
    uploaded_test_files: Dict[str, str],
    env_vars: Dict[str, str],
):
    """Test the complete end-to-end conversion process."""
    # Insert files into database with 'ingested' status
    with psycopg.connect(**db_connection) as conn:
        with conn.cursor() as cursor:
            for filename, gcs_path in uploaded_test_files.items():
                cursor.execute(
                    """
                    INSERT INTO files (gcs_path, status) VALUES (%s, %s)
                """,
                    (gcs_path, "ingested"),
                )
            conn.commit()

    # Run the converter
    converter = HTMLToMarkdownConverter(gcs_client=gcs_client)
    successful, total = converter.process_all_files()

    # Check results
    assert successful == total == len(uploaded_test_files)

    # Verify markdown files were created
    for filename in uploaded_test_files.keys():
        base_name = filename.split(".")[0]
        markdown_filename = f"{base_name}.md"
        expected_blob_name = f"docs/markdown/{markdown_filename}"
        blob = test_bucket.blob(expected_blob_name)
        assert blob.exists(), f"Markdown file not found: {expected_blob_name}"

        # Download and check content
        markdown_content = blob.download_as_text()
        assert len(markdown_content) > 0

        # Basic content checks
        if filename == "simple.html":
            assert "Main Title" in markdown_content
            assert "test paragraph" in markdown_content
            # Navigation should be removed
            assert "Navigation content" not in markdown_content
            assert "Footer content" not in markdown_content

        elif filename == "complex.html":
            assert "Article Title" in markdown_content
            assert "First paragraph" in markdown_content
            # Table content should be preserved
            assert "Name" in markdown_content
            assert "Item A" in markdown_content
            # Unwanted elements should be removed
            assert "Sidebar content" not in markdown_content
            assert "Ad content" not in markdown_content

    # Verify database status updates
    with psycopg.connect(**db_connection) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT status FROM files WHERE status = 'converted'")
            converted_files = cursor.fetchall()
            assert len(converted_files) == len(uploaded_test_files)


def test_main_function_success(
    db_connection: Dict[str, str],
    test_bucket: storage.Bucket,
    uploaded_test_files: Dict[str, str],
    env_vars: Dict[str, str],
):
    """Test the main function with successful processing."""
    # Insert files into database
    with psycopg.connect(**db_connection) as conn:
        with conn.cursor() as cursor:
            for filename, gcs_path in uploaded_test_files.items():
                cursor.execute(
                    """
                    INSERT INTO files (gcs_path, status) VALUES (%s, %s)
                """,
                    (gcs_path, "ingested"),
                )
            conn.commit()

    # Run main function
    exit_code = main()

    # Should return 0 for success
    assert exit_code == 0


def test_process_nonexistent_file(
    db_connection: Dict[str, str],
    test_bucket: storage.Bucket,
    gcs_client: storage.Client,
    env_vars: Dict[str, str],
):
    """Test processing a file that doesn't exist in GCS."""
    # Insert a non-existent file into database
    fake_gcs_path = f"gs://{test_bucket.name}/nonexistent/fake.html.gz"

    with psycopg.connect(**db_connection) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO files (gcs_path, status) VALUES (%s, %s)
                RETURNING id
            """,
                (fake_gcs_path, "ingested"),
            )
            file_id = cursor.fetchone()[0]
            conn.commit()

    # Try to process
    converter = HTMLToMarkdownConverter(gcs_client=gcs_client)
    successful, total = converter.process_all_files()

    # Should have 0 successful, 1 total
    assert successful == 0
    assert total == 1

    # File status should be updated to 'failed'
    with psycopg.connect(**db_connection) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT status FROM files WHERE id = %s", (file_id,))
            status = cursor.fetchone()[0]
            assert status == "failed"


def test_process_corrupted_gzip_file(
    db_connection: Dict[str, str],
    test_bucket: storage.Bucket,
    gcs_client: storage.Client,
    env_vars: Dict[str, str],
):
    """Test processing a corrupted gzip file."""
    # Upload a corrupted gzip file
    blob_name = "docs/corrupted.html.gz"
    blob = test_bucket.blob(blob_name)
    blob.upload_from_string(b"not valid gzip data", content_type="application/gzip")

    gcs_path = f"gs://{test_bucket.name}/{blob_name}"

    # Insert into database
    with psycopg.connect(**db_connection) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO files (gcs_path, status) VALUES (%s, %s)
                RETURNING id
            """,
                (gcs_path, "ingested"),
            )
            file_id = cursor.fetchone()[0]
            conn.commit()

    # Try to process
    converter = HTMLToMarkdownConverter(gcs_client=gcs_client)
    successful, total = converter.process_all_files()

    # Should fail
    assert successful == 0
    assert total == 1

    # Status should be 'failed'
    with psycopg.connect(**db_connection) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT status FROM files WHERE id = %s", (file_id,))
            status = cursor.fetchone()[0]
            assert status == "failed"


def test_empty_database(
    db_connection: Dict[str, str], gcs_client: storage.Client, env_vars: Dict[str, str]
):
    """Test behavior when database has no ingested files."""
    converter = HTMLToMarkdownConverter(gcs_client=gcs_client)
    successful, total = converter.process_all_files()

    # Should have no files to process
    assert successful == 0
    assert total == 0


def test_mixed_file_statuses(
    db_connection: Dict[str, str],
    test_bucket: storage.Bucket,
    uploaded_test_files: Dict[str, str],
    gcs_client: storage.Client,
    env_vars: Dict[str, str],
):
    """Test that only files with 'ingested' status are processed."""
    gcs_paths = list(uploaded_test_files.values())

    # Insert files with different statuses
    with psycopg.connect(**db_connection) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO files (gcs_path, status) VALUES 
                (%s, 'ingested'),
                (%s, 'pending'),
                (%s, 'converted'),
                ('gs://fake-bucket/fake.html.gz', 'ingested')
            """,
                (gcs_paths[0], gcs_paths[1], gcs_paths[0]),
            )
            conn.commit()

    # Process files
    converter = HTMLToMarkdownConverter(gcs_client=gcs_client)
    successful, total = converter.process_all_files()

    # Should process 2 files (one valid, one invalid)
    # Only 1 should succeed (the valid one)
    assert total == 2
    assert successful == 1


def test_specific_html_features(
    db_connection: Dict[str, str],
    test_bucket: storage.Bucket,
    sample_html_files: Dict[str, str],
    gcs_client: storage.Client,
    env_vars: Dict[str, str],
):
    """Test specific HTML feature handling."""
    # Test Unicode handling specifically
    unicode_html = sample_html_files["unicode.html"]
    compressed_content = gzip.compress(unicode_html.encode("utf-8"))

    blob_name = "unicode-test/unicode.html.gz"
    blob = test_bucket.blob(blob_name)
    blob.upload_from_string(compressed_content, content_type="application/gzip")

    gcs_path = f"gs://{test_bucket.name}/{blob_name}"

    # Insert into database
    with psycopg.connect(**db_connection) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO files (gcs_path, status) VALUES (%s, %s)
                RETURNING id
            """,
                (gcs_path, "ingested"),
            )
            file_id = cursor.fetchone()[0]
            conn.commit()

    # Process the file
    converter = HTMLToMarkdownConverter(gcs_client=gcs_client)
    successful, total = converter.process_all_files()

    assert successful == 1
    assert total == 1

    # Check the converted file
    markdown_blob = test_bucket.blob("unicode-test/markdown/unicode.md")
    assert markdown_blob.exists()

    markdown_content = markdown_blob.download_as_text()

    # Verify Unicode characters are preserved
    unicode_chars = [
        "你好世界",
        "🚀",
        "🌟",
        "✨",
        "café",
        "naïve",
        "résumé",
        "α + β = γ",
    ]
    for char in unicode_chars:
        assert char in markdown_content, (
            f"Unicode character '{char}' not found in markdown"
        )

    # Verify database status
    with psycopg.connect(**db_connection) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT status FROM files WHERE id = %s", (file_id,))
            status = cursor.fetchone()[0]
            assert status == "converted"
