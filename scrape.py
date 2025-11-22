import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
import logging
import os
import time
import random
from datetime import datetime
from fake_useragent import UserAgent
from urllib.parse import urlencode

# ==================== SETUP LOGGING ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)

# Create data directory
DATA_DIR = "raw"
os.makedirs(DATA_DIR, exist_ok=True)
log.info(f"Data will be saved in: {os.path.abspath(DATA_DIR)}")

# ==================== SCRAPER SETUP ====================
ua = UserAgent()
scraper = cloudscraper.create_scraper(
    browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False},
    delay=None
)

# Your exact category (Men's Clothing - All)
BASE_URL = "https://www.amazon.com/s?i=specialty-aps&rh=n%3A7141123011%2Cn%3A1040658"

def get_page_url(page: int) -> str:
    params = {'page': page}
    return f"{BASE_URL}&{urlencode(params)}"

def scrape_single_page(url: str, page_num: int):
    headers = {'User-Agent': ua.random}
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    log.info(f"Scraping page {page_num} → {url}")
    
    try:
        response = scraper.get(url, headers=headers, timeout=30)
        
        if response.status_code != 200:
            log.error(f"HTTP {response.status_code} on page {page_num}")
            return []
        
        if "captcha" in response.text.lower() or "api-services-support@amazon.com" in response.text:
            log.warning("CAPTCHA or block detected! Sleeping 60s...")
            time.sleep(60)
            return scrape_single_page(url, page_num)  # retry once
        
        # Save raw HTML (very useful for debugging layout changes)
        html_path = os.path.join(DATA_DIR, f"raw_page_{page_num:04d}_{timestamp}.html")
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(response.text)
        log.info(f"Raw HTML saved → {html_path}")

        soup = BeautifulSoup(response.text, 'lxml')
        items = soup.find_all('div', {'data-component-type': 's-search-result'})
        
        if not items:
            log.warning(f"No products found on page {page_num} → probably last page")
            return None  # signal end

        log.info(f"Found {len(items)} products on page {page_num}")

        products = []
        for idx, item in enumerate(items, 1):
            try:
                title_tag = item.h2.a if item.h2 else None
                title = title_tag.get_text(strip=True) if title_tag else "N/A"
                link = "https://www.amazon.com" + title_tag['href'].split('/ref')[0] if title_tag else "N/A"

                img = item.find('img', {'class': 's-image'})
                image_url = img['src'] if img else "N/A"

                # Price
                whole = item.find('span', {'class': 'a-price-whole'})
                frac = item.find('span', {'class': 'a-price-fraction'})
                sym = item.find('span', {'class': 'a-price-symbol'})
                price = f"{sym.text if sym else ''}{whole.text if whole else ''}{frac.text if frac else ''}".strip('.')
                if not price: price = "N/A"

                rating = item.find('span', {'class': 'a-icon-alt'})
                rating_text = rating.get_text(strip=True).split(' out')[0] if rating else "N/A"

                review_count = "0"
                review_tag = item.find('span', {'class': 'a-size-base'})
                if review_tag and review_tag.text.replace(',', '').isdigit():
                    review_count = review_tag.text.replace(',', '')

                brand = "Unknown"
                brand_tag = item.find('h5') or item.find('span', text=lambda t: t and len(t) < 40)
                if brand_tag:
                    brand = brand_tag.get_text(strip=True)

                prime = bool(item.find('i', {'aria-label': 'Amazon Prime'}))
                sponsored = bool(item.select_one('.s-sponsored-label-text'))

                products.append({
                    'rank_on_page': idx,
                    'page': page_num,
                    'title': title,
                    'brand': brand,
                    'price': price,
                    'rating': rating_text,
                    'reviews': int(review_count),
                    'image_url': image_url,
                    'product_url': link,
                    'prime': prime,
                    'sponsored': sponsored,
                    'scraped_at': datetime.now().isoformat()
                })
            except Exception as e:
                log.debug(f"Failed to parse one item: {e}")
                continue

        return products

    except Exception as e:
        log.error(f"Request failed: {e}")
        time.sleep(20)
        return []

# ==================== MAIN SCRAPER LOOP ====================
def main():
    all_products = []
    pages_to_scrape = 50  # Change to 1–1000 as needed

    for page in range(1, pages_to_scrape + 1):
        url = get_page_url(page)
        page_data = scrape_single_page(url, page)
        
        if page_data is None:  # no more products
            log.info(f"Reached last page at {page}")
            break
        if not page_data:
            log.warning("Empty page data → stopping")
            break

        all_products.extend(page_data)
        log.info(f"Total collected so far: {len(all_products)} items")

        # Save incremental backup every 10 pages
        if page % 10 == 0:
            temp_df = pd.DataFrame(all_products)
            temp_df.to_csv(os.path.join(DATA_DIR, f"backup_up_to_page_{page}.csv"), index=False)
            log.info(f"Backup saved after page {page}")

        # Be respectful
        sleep_time = random.uniform(4, 9)
        log.info(f"Sleeping {sleep_time:.1f}s...")
        time.sleep(sleep_time)

    # Final save
    if all_products:
        df = pd.DataFrame(all_products)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        df.to_csv(os.path.join(DATA_DIR, f"amazon_mens_clothing_final_{timestamp}_{len(df)}_items.csv"), index=False)
        df.to_json(os.path.join(DATA_DIR, f"amazon_mens_clothing_final_{timestamp}_{len(df)}_items.json"), orient="records", indent=2)
        
        log.info(f"DONE! Scraped {len(df)} products")
        log.info(f"Files saved in → {os.path.abspath(DATA_DIR)}")
        print(df[['title', 'brand', 'price', 'rating']].head(10))
    else:
        log.error("No products scraped!")

if __name__ == "__main__":
    main()
