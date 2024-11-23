# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = None

import requests
from bs4 import BeautifulSoup
import csv

url = "https://decider.com/list/canceled-netflix-original-shows/"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
}

# +
response = requests.get(url, headers=headers)
if response.status_code == 200:
    soup = BeautifulSoup(response.content, "html.parser")
    headings = soup.find_all(class_="article-list__heading")
    
    all_headings = []
    for heading in headings:
        all_headings.append(heading.get_text(strip=True).strip("'"))
    print(all_headings)
    with open('etl/datasets/netflix_cancelled_shows.csv', 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Title"])
        for heading in all_headings:
            writer.writerow([heading])
    
    print("All headings were successfully retrieved and written to 'netflix_cancelled_shows.csv'.")
else:
    print("Failed to retrieve the page")

