"""
Tests for HTML to Markdown conversion functionality.
"""

import re
from typing import Dict
from unittest.mock import MagicMock
from src.main import HTMLToMarkdownConverter


def test_clean_html_removes_navigation(env_vars: Dict[str, str]):
    """Test that navigation elements are removed from HTML."""
    converter = HTMLToMarkdownConverter(gcs_client=MagicMock())

    html = """
    <html>
    <body>
        <nav>Navigation menu</nav>
        <div class="navbar">Top navbar</div>
        <div id="navigation">Side nav</div>
        <main>
            <h1>Main Content</h1>
            <p>This should remain.</p>
        </main>
        <footer>Footer content</footer>
    </body>
    </html>
    """

    cleaned = converter.clean_html(html)

    # Navigation elements should be removed
    assert "Navigation menu" not in cleaned
    assert "Top navbar" not in cleaned
    assert "Side nav" not in cleaned
    assert "Footer content" not in cleaned

    # Main content should remain
    assert "Main Content" in cleaned
    assert "This should remain" in cleaned


def test_clean_html_removes_scripts_and_styles(env_vars: Dict[str, str]):
    """Test that script and style tags are removed."""
    converter = HTMLToMarkdownConverter(gcs_client=MagicMock())

    html = """
    <html>
    <head>
        <style>body { margin: 0; }</style>
        <script>console.log('test');</script>
    </head>
    <body>
        <h1>Title</h1>
        <p>Content</p>
        <script>alert('popup');</script>
    </body>
    </html>
    """

    cleaned = converter.clean_html(html)

    # Scripts and styles should be removed
    assert "console.log" not in cleaned
    assert "alert" not in cleaned
    assert "margin: 0" not in cleaned
    assert "<script>" not in cleaned
    assert "<style>" not in cleaned

    # Content should remain
    assert "Title" in cleaned
    assert "Content" in cleaned


def test_html_to_markdown_basic_conversion(env_vars: Dict[str, str]):
    """Test basic HTML to Markdown conversion."""
    converter = HTMLToMarkdownConverter(gcs_client=MagicMock())

    html = """
    <html>
    <body>
        <h1>Main Title</h1>
        <h2>Subtitle</h2>
        <p>A paragraph with <strong>bold</strong> and <em>italic</em> text.</p>
        <ul>
            <li>First item</li>
            <li>Second item</li>
        </ul>
    </body>
    </html>
    """

    markdown = converter.html_to_markdown(html)

    # Check for proper markdown formatting
    assert "# Main Title" in markdown
    assert "## Subtitle" in markdown
    assert "**bold**" in markdown or "*bold*" in markdown
    assert "*italic*" in markdown or "_italic_" in markdown
    assert "- First item" in markdown or "* First item" in markdown
    assert "- Second item" in markdown or "* Second item" in markdown


def test_html_to_markdown_table_conversion(env_vars: Dict[str, str]):
    """Test that HTML tables are properly converted to Markdown."""
    converter = HTMLToMarkdownConverter(gcs_client=MagicMock())

    html = """
    <html>
    <body>
        <table>
            <thead>
                <tr>
                    <th>Header 1</th>
                    <th>Header 2</th>
                    <th>Header 3</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td>Cell 1</td>
                    <td>Cell 2</td>
                    <td>Cell 3</td>
                </tr>
                <tr>
                    <td>Row 2, Col 1</td>
                    <td>Row 2, Col 2</td>
                    <td>Row 2, Col 3</td>
                </tr>
            </tbody>
        </table>
    </body>
    </html>
    """

    markdown = converter.html_to_markdown(html)

    # Check for table formatting
    assert "Header 1" in markdown
    assert "Header 2" in markdown
    assert "Header 3" in markdown
    assert "Cell 1" in markdown
    assert "Cell 2" in markdown
    assert "Cell 3" in markdown
    assert "Row 2, Col 1" in markdown

    # Should contain table separators (pipes)
    assert "|" in markdown


def test_html_to_markdown_complex_content(
    env_vars: Dict[str, str], sample_html_files: Dict[str, str]
):
    """Test conversion of complex HTML content."""
    converter = HTMLToMarkdownConverter(gcs_client=MagicMock())

    # Use the complex sample HTML
    html = sample_html_files["complex.html"]
    markdown = converter.html_to_markdown(html)

    # Main content should be preserved
    assert "Article Title" in markdown
    assert "First paragraph" in markdown
    assert "bold text" in markdown
    assert "Subsection" in markdown
    assert "List item 1" in markdown
    assert "List item 2" in markdown

    # Table content should be preserved
    assert "Name" in markdown
    assert "Value" in markdown
    assert "Description" in markdown
    assert "Item A" in markdown
    assert "Item B" in markdown

    # Navigation and unwanted elements should be removed
    assert "Header" not in markdown  # from header tag
    assert "Home" not in markdown  # from nav
    assert "About" not in markdown  # from nav
    assert "Sidebar content" not in markdown
    assert "Ad content" not in markdown
    assert "Social links" not in markdown
    assert "console.log" not in markdown


def test_comprehensive_html_samples(
    env_vars: Dict[str, str],
    sample_html_files,
    expected_markdown_outputs: Dict[str, Dict],
):
    """Test conversion using comprehensive HTML samples."""
    converter = HTMLToMarkdownConverter(gcs_client=MagicMock())

    # Test minimal HTML
    minimal_html = sample_html_files["minimal.html"]
    minimal_md = converter.html_to_markdown(minimal_html)
    expected = expected_markdown_outputs["minimal"]

    for content in expected["contains"]:
        assert content in minimal_md, f"Expected '{content}' in minimal markdown"
    for content in expected["not_contains"]:
        assert content not in minimal_md, (
            f"Did not expect '{content}' in minimal markdown"
        )

    # Test navigation-heavy HTML
    nav_html = sample_html_files["navigation.html"]
    nav_md = converter.html_to_markdown(nav_html)
    expected = expected_markdown_outputs["navigation"]

    for content in expected["contains"]:
        assert content in nav_md, f"Expected '{content}' in navigation markdown"
    for content in expected["not_contains"]:
        assert content not in nav_md, (
            f"Did not expect '{content}' in navigation markdown"
        )

    # Test table-heavy HTML
    table_html = sample_html_files["tables.html"]
    table_md = converter.html_to_markdown(table_html)
    expected = expected_markdown_outputs["tables"]

    for content in expected["contains"]:
        assert content in table_md, f"Expected '{content}' in table markdown"
    for content in expected["not_contains"]:
        assert content not in table_md, f"Did not expect '{content}' in table markdown"

    # Test script-heavy HTML
    script_html = sample_html_files["scripts.html"]
    script_md = converter.html_to_markdown(script_html)
    expected = expected_markdown_outputs["scripts"]

    for content in expected["contains"]:
        assert content in script_md, f"Expected '{content}' in script markdown"
    for content in expected["not_contains"]:
        assert content not in script_md, (
            f"Did not expect '{content}' in script markdown"
        )

    # Test Unicode HTML
    unicode_html = sample_html_files["unicode.html"]
    unicode_md = converter.html_to_markdown(unicode_html)
    expected = expected_markdown_outputs["unicode"]

    for content in expected["contains"]:
        assert content in unicode_md, f"Expected '{content}' in unicode markdown"
    for content in expected["not_contains"]:
        assert content not in unicode_md, (
            f"Did not expect '{content}' in unicode markdown"
        )


def test_malformed_html_handling(env_vars: Dict[str, str], sample_html_files):
    """Test that malformed HTML is handled gracefully."""
    converter = HTMLToMarkdownConverter(gcs_client=MagicMock())

    malformed_html = sample_html_files["malformed.html"]

    # Should not crash, even with malformed HTML
    try:
        markdown = converter.html_to_markdown(malformed_html)
        # Should produce some output
        assert len(markdown) > 0
        # Should contain at least the header content
        assert "Unclosed header" in markdown
    except Exception as e:
        # If it does fail, make sure it's a reasonable failure
        assert "malformed" in str(e).lower() or "parse" in str(e).lower()


def test_clean_markdown_formatting(env_vars: Dict[str, str]):
    """Test that markdown cleanup works properly."""
    converter = HTMLToMarkdownConverter(gcs_client=MagicMock())

    # Test markdown with excessive whitespace
    messy_markdown = """

# Title


This is a paragraph.



- List item 1

- List item 2




## Another Section



More content here.


"""

    cleaned = converter._clean_markdown(messy_markdown)

    # Should have reduced excessive blank lines
    assert not re.search(r"\n\s*\n\s*\n", cleaned)

    # Should start and end cleanly
    assert not cleaned.startswith("\n")
    assert not cleaned.endswith("\n\n")

    # Content should still be there
    assert "# Title" in cleaned
    assert "This is a paragraph." in cleaned
    assert "- List item 1" in cleaned
    assert "## Another Section" in cleaned
