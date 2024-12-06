# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = None

# -

from bs4 import BeautifulSoup
import csv
import requests

URL = "https://decider.com/list/canceled-netflix-original-shows/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
}

def fetch_page(url, headers):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.content
    else:
        raise Exception("Failed to retrieve the page")

def parse_headings(html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    headings = soup.find_all(class_="article-list__heading")
    return [heading.get_text(strip=True).strip("'") for heading in headings]

def save_headings_to_csv(headings, file_path):
    with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Title"])
        for heading in headings:
            writer.writerow([heading])

def main():
    try:
        html_content = fetch_page(URL, HEADERS)
        headings = parse_headings(html_content)
        print(headings)
        save_headings_to_csv(headings, 'etl/datasets/netflix_cancelled_shows.csv')
        print("All headings were successfully retrieved and written to 'netflix_cancelled_shows.csv'.")
    except Exception as e:
        print(e)

if __name__ == "__main__":
    main()
