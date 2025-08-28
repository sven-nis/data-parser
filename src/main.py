#!/usr/bin/env python3
"""
HTML to Markdown converter for Google Cloud Storage files.

This script queries a PostgreSQL database for files with 'ingested' status, reads the corresponding
gzipped HTML files from GCS, converts them to Markdown while filtering out navigation and footer
elements, and saves the results to a sibling directory in the same bucket.
"""

import gzip
import logging
import os
import re
from io import BytesIO
from pathlib import Path
from typing import List, Dict, Any
from urllib.parse import urlparse

import psycopg
from psycopg.rows import dict_row
from bs4 import BeautifulSoup, NavigableString
from google.cloud import storage
from markitdown import MarkItDown, StreamInfo
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class HTMLToMarkdownConverter:
    """Converts HTML files to Markdown with content filtering."""

    def __init__(self, gcs_client: storage.Client):
        """Initialize the converter with database and GCS connections."""
        self.gcs_client = gcs_client

        # Database connection parameters
        self.db_params = {
            "host": os.getenv("DB_HOST"),
            "port": os.getenv("DB_PORT", "5432"),
            "dbname": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
        }

        # Validate required database parameters
        required_params = ["host", "dbname", "user", "password"]
        missing_params = [
            param for param in required_params if not self.db_params[param]
        ]
        if missing_params:
            raise ValueError(f"Missing required database parameters: {missing_params}")

        # Elements to remove (navigation, footer, etc.)
        self.remove_selectors = {
            "nav",
            "footer",
            "header",
            "aside",
            ".nav",
            ".navigation",
            ".navbar",
            ".menu",
            ".footer",
            ".site-footer",
            ".page-footer",
            ".sidebar",
            ".breadcrumb",
            ".breadcrumbs",
            ".social",
            ".share",
            ".sharing",
            ".advertisement",
            ".ads",
            ".ad",
            ".cookie-notice",
            ".cookie-banner",
            "#nav",
            "#navigation",
            "#navbar",
            "#menu",
            "#footer",
            "#site-footer",
            "#page-footer",
            "#sidebar",
            "#breadcrumb",
            "#breadcrumbs",
        }

    def get_ingested_files(self) -> List[Dict[str, Any]]:
        """
        Query PostgreSQL database for files with 'ingested' status.

        Returns:
            List of dictionaries containing file information from database
        """
        query = """
        SELECT id, gcs_path, status, created_at, updated_at
        FROM files 
        WHERE status = 'ingested'
        ORDER BY updated_at DESC
        """

        try:
            with psycopg.connect(**self.db_params) as conn:
                with conn.cursor(row_factory=dict_row) as cursor:
                    cursor.execute(query)
                    results = cursor.fetchall()

            logger.info(f"Found {len(results)} files with 'ingested' status")
            return [dict(row) for row in results]

        except psycopg.Error as e:
            logger.error(f"Database error: {e}")
            raise
        except Exception as e:
            logger.error(f"Error querying database: {e}")
            raise

    def parse_gcs_path(self, gcs_path: str) -> tuple[str, str]:
        """
        Parse GCS path to extract bucket and object name.

        Args:
            gcs_path: Full GCS path (e.g., 'gs://bucket-name/path/to/file.html.gz')

        Returns:
            Tuple of (bucket_name, object_name)
        """
        if not gcs_path.startswith("gs://"):
            raise ValueError(f"Invalid GCS path format: {gcs_path}")

        parsed = urlparse(gcs_path)
        bucket_name = parsed.netloc
        object_name = parsed.path.lstrip("/")

        return bucket_name, object_name

    def get_sibling_markdown_path(self, original_path: str) -> str:
        """
        Generate sibling directory path for markdown output.

        Args:
            original_path: Original GCS object path

        Returns:
            Path for markdown file in sibling directory
        """
        path = Path(original_path)

        # Get the parent directory
        parent = path.parent

        # Create sibling directory name
        sibling_dir = parent / "markdown"

        # Change file extension to .md
        if path.suffix.lower() == ".gz":
            # Handle .html.gz files
            stem = path.stem
            if stem.endswith(".html") or stem.endswith(".htm"):
                filename = Path(stem).with_suffix(".md").name
            else:
                filename = f"{stem}.md"
        elif path.suffix.lower() in [".html", ".htm"]:
            filename = path.with_suffix(".md").name
        else:
            filename = f"{path.stem}.md"

        return str(sibling_dir / filename)

    def clean_html(self, html_content: str) -> str:
        """
        Clean HTML by removing navigation, footer, and other unwanted elements.

        Args:
            html_content: Raw HTML content

        Returns:
            Cleaned HTML content
        """
        soup = BeautifulSoup(html_content, "lxml")

        # Remove unwanted elements
        for selector in self.remove_selectors:
            if selector.startswith((".", "#")):
                # CSS selector
                elements = soup.select(selector)
            else:
                # Tag name
                elements = soup.find_all(selector)

            for element in elements:
                element.decompose()

        # Remove script and style tags
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()

        # Remove comments
        for comment in soup.find_all(
            string=lambda text: isinstance(text, NavigableString)
            and str(text).strip().startswith("<!--")
        ):
            comment.extract()

        # Clean up empty elements
        self._remove_empty_elements(soup)

        return str(soup)

    def _remove_empty_elements(self, soup: BeautifulSoup) -> None:
        """Remove empty elements that don't contribute to content."""
        # Elements that should be removed if empty
        removable_if_empty = ["p", "div", "span", "section", "article"]

        for tag_name in removable_if_empty:
            for tag in soup.find_all(tag_name):
                if not tag.get_text(strip=True) and not tag.find(["img", "br", "hr"]):
                    tag.decompose()

    def html_to_markdown(self, html_content: str) -> str:
        """
        Convert HTML to Markdown with proper table handling.

        Args:
            html_content: HTML content to convert

        Returns:
            Markdown content
        """
        # Clean the HTML first
        cleaned_html = self.clean_html(html_content)

        # Convert to Markdown using markitdown
        # Create a BytesIO stream from the HTML content
        html_bytes = cleaned_html.encode("utf-8")
        html_stream = BytesIO(html_bytes)

        # Use convert_stream with proper stream info for HTML
        md_converter = MarkItDown()
        stream_info = StreamInfo(extension=".html", mimetype="text/html")
        result = md_converter.convert_stream(html_stream, stream_info=stream_info)
        markdown = result.text_content

        # Clean up the markdown
        markdown = self._clean_markdown(markdown)

        return markdown

    def _clean_markdown(self, markdown: str) -> str:
        """Clean up the generated Markdown."""
        # Clean up list formatting first
        markdown = re.sub(r"\n\s*\n(\s*[-*+])", r"\n\1", markdown)

        # Ensure proper spacing around headers
        markdown = re.sub(r"\n(#{1,6}\s)", r"\n\n\1", markdown)
        markdown = re.sub(r"(#{1,6}.*)\n([^\n#])", r"\1\n\n\2", markdown)

        # Remove excessive blank lines AFTER header spacing (3 or more consecutive newlines become 2)
        # Use a loop to handle multiple consecutive blank lines
        while re.search(r"\n\s*\n\s*\n", markdown):
            markdown = re.sub(r"\n\s*\n\s*\n+", "\n\n", markdown)

        # Clean up table formatting
        markdown = re.sub(r"\|\s*\|\s*\|", "| |", markdown)

        # Remove leading/trailing whitespace
        markdown = markdown.strip()

        return markdown

    def process_file(self, file_info: Dict[str, Any]) -> bool:
        """
        Process a single HTML file from GCS based on database record.

        Args:
            file_info: Dictionary containing file information from database

        Returns:
            True if successful, False otherwise
        """
        gcs_path = file_info["gcs_path"]
        file_id = file_info["id"]

        try:
            logger.info(f"Processing file ID {file_id}: {gcs_path}")

            # Parse GCS path
            bucket_name, object_name = self.parse_gcs_path(gcs_path)

            # Get bucket and blob
            bucket = self.gcs_client.bucket(bucket_name)
            blob = bucket.blob(object_name)

            # Check if file exists
            if not blob.exists():
                logger.error(f"File not found in GCS: {gcs_path}")
                return False

            # Download and decompress the file
            compressed_data = blob.download_as_bytes()
            html_content = gzip.decompress(compressed_data).decode("utf-8")

            # Convert to Markdown
            markdown_content = self.html_to_markdown(html_content)

            # Generate destination path (sibling directory)
            dest_object_name = self.get_sibling_markdown_path(object_name)

            # Upload to same bucket in sibling directory
            dest_blob = bucket.blob(dest_object_name)
            dest_blob.upload_from_string(markdown_content, content_type="text/markdown")

            logger.info(
                f"Successfully converted {gcs_path} to gs://{bucket_name}/{dest_object_name}"
            )
            return True

        except Exception as e:
            logger.error(f"Error processing file ID {file_id} ({gcs_path}): {str(e)}")
            return False

    def update_file_status(self, file_id: int, status: str) -> bool:
        """
        Update file status in the database.

        Args:
            file_id: Database record ID
            status: New status to set

        Returns:
            True if successful, False otherwise
        """
        query = """
        UPDATE files 
        SET status = %s, updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
        """

        try:
            with psycopg.connect(**self.db_params) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (status, file_id))
                    conn.commit()
            return True

        except psycopg.Error as e:
            logger.error(f"Database error updating file {file_id}: {e}")
            return False
        except Exception as e:
            logger.error(f"Error updating file status {file_id}: {e}")
            return False

    def process_all_files(self) -> tuple[int, int]:
        """
        Process all files with 'ingested' status from the database.

        Returns:
            Tuple of (successful_count, total_count)
        """
        logger.info("Starting batch processing from database query")

        # Get files from database
        files = self.get_ingested_files()

        if not files:
            logger.warning("No files found with 'ingested' status")
            return 0, 0

        logger.info(f"Found {len(files)} files to process")

        successful_count = 0
        for file_info in files:
            file_id = file_info["id"]

            if self.process_file(file_info):
                successful_count += 1
                # Update status to 'converted' on success
                self.update_file_status(file_id, "converted")
            else:
                # Update status to 'failed' on error
                self.update_file_status(file_id, "failed")

        logger.info(
            f"Processing complete: {successful_count}/{len(files)} files converted successfully"
        )
        return successful_count, len(files)


def main():
    """Main entry point."""
    # Check for required database environment variables
    required_env_vars = ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD"]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]

    if missing_vars:
        logger.error(f"Missing required environment variables: {missing_vars}")
        logger.info("Please set the following environment variables:")
        logger.info("  DB_HOST=your-database-host")
        logger.info("  DB_NAME=your-database-name")
        logger.info("  DB_USER=your-database-user")
        logger.info("  DB_PASSWORD=your-database-password")
        logger.info("  DB_PORT=5432 (optional, defaults to 5432)")
        return 1

    gcs_client = storage.Client()
    try:
        # Initialize converter
        converter = HTMLToMarkdownConverter(gcs_client)

        # Process all files
        successful, total = converter.process_all_files()

        if successful == total:
            logger.info("All files processed successfully!")
            return 0
        else:
            logger.warning(
                f"Some files failed to process: {total - successful} failures"
            )
            return 1

    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        return 1


if __name__ == "__main__":
    exit(main())
