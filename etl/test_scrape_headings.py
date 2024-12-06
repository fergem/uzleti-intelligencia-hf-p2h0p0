import pytest
from unittest.mock import patch, mock_open
from scrape_headings import fetch_page, parse_headings, save_headings_to_csv

@patch('scrape_headings.requests.get')
def test_fetch_page_success(mock_get):
    mock_get.return_value.status_code = 200
    mock_get.return_value.content = b"<html></html>"
    content = fetch_page("http://example.com", {})
    assert content == b"<html></html>"

@patch('scrape_headings.requests.get')
def test_fetch_page_failure(mock_get):
    mock_get.return_value.status_code = 404
    with pytest.raises(Exception):
        fetch_page("http://example.com", {})

def test_parse_headings():
    html_content = b"""
    <html>
        <div class="article-list__heading">Heading 1</div>
        <div class="article-list__heading">Heading 2</div>
    </html>
    """
    headings = parse_headings(html_content)
    assert headings == ["Heading 1", "Heading 2"]

@patch("builtins.open", new_callable=mock_open)
@patch("scrape_headings.csv.writer")
def test_save_headings_to_csv(mock_csv_writer, mock_open):
    headings = ["Heading 1", "Heading 2"]
    save_headings_to_csv(headings, "test.csv")
    mock_open.assert_called_once_with("test.csv", 'w', newline='', encoding='utf-8')
    mock_csv_writer().writerow.assert_any_call(["Title"])
    mock_csv_writer().writerow.assert_any_call(["Heading 1"])
    mock_csv_writer().writerow.assert_any_call(["Heading 2"])