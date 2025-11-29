import os
import time
import hashlib
import requests
import json
import random
import csv
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urljoin, urlencode
from datetime import datetime
from utils.logging import setup_logger

# ========================= CONFIG (loaded from config/scrape_config.json) =========================
def load_scrape_config(path=os.path.join('config', 'scrape_config.json')):
    """Load scrape configuration from JSON and validate required keys.

    Raises FileNotFoundError if the file does not exist.
    Raises ValueError if required configuration keys are missing.
    Returns the parsed config dict when valid.
    """
    required_keys = [
        "BASE_DIR",
        "IMAGES_DIR",
        "TEXT_DIR",
        "HEADLESS",
        "MAX_PER_CATEGORY",
        "DELAY_RANGE",
        "BASE_SEARCH_URL",
        "CATEGORIES",
    ]

    if not os.path.exists(path):
        raise FileNotFoundError(f"Scrape configuration file not found: {path}")

    with open(path, 'r', encoding='utf-8') as f:
        cfg = json.load(f)

    if not isinstance(cfg, dict):
        raise ValueError(f"Scrape configuration must be a JSON object (dict) in {path}")

    missing = [k for k in required_keys if k not in cfg]
    if missing:
        raise ValueError(f"Scrape configuration is missing required keys: {', '.join(missing)}")

    return cfg


try:
    _CFG = load_scrape_config()
except Exception as e:
    import sys
    print(f"Configuration error: {e}", file=sys.stderr)
    raise

# Allow environment overrides for some config values (useful in containers)
try:
    if os.getenv('HEADLESS') is not None:
        _CFG['HEADLESS'] = os.getenv('HEADLESS').lower() in ('1', 'true', 'yes')
    if os.getenv('MAX_PER_CATEGORY') is not None:
        try:
            _CFG['MAX_PER_CATEGORY'] = int(os.getenv('MAX_PER_CATEGORY'))
        except Exception:
            pass
    if os.getenv('BASE_SEARCH_URL') is not None:
        _CFG['BASE_SEARCH_URL'] = os.getenv('BASE_SEARCH_URL')
except Exception:
    # best-effort; continue with file config
    pass

# Resolve directories and runtime settings from loaded config
BASE_DIR = _CFG['BASE_DIR']
IMAGES_DIR = os.path.join(BASE_DIR, _CFG['IMAGES_DIR'])
TEXT_DIR = os.path.join(BASE_DIR, _CFG['TEXT_DIR'])
os.makedirs(IMAGES_DIR, exist_ok=True)
os.makedirs(TEXT_DIR, exist_ok=True)

# initialize logger now that BASE_DIR is known
logger = setup_logger(name='amazon_scraper', base_dir=BASE_DIR, config_path=os.path.join('config','logging_config.json'), caller_file=__file__)
# Record script start time for total run duration logging
SCRIPT_START = datetime.now()

# Scrape runtime settings
HEADLESS = bool(_CFG['HEADLESS'])
MAX_PER_CATEGORY = int(_CFG['MAX_PER_CATEGORY'])
DELAY_RANGE = tuple(_CFG['DELAY_RANGE'])
CATEGORIES = _CFG['CATEGORIES']
BASE_SEARCH_URL = _CFG['BASE_SEARCH_URL']

# ========================= DRIVER & HELPERS =========================
def get_driver():
    options = Options()
    if HEADLESS:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    driver = webdriver.Chrome(options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
    return driver

def generate_id(url):
    return hashlib.md5(url.encode()).hexdigest()

def random_delay():
    time.sleep(random.uniform(*DELAY_RANGE))

def scroll_randomly(driver):
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight * Math.random());")
    time.sleep(0.8)

def click_continue_shopping_if_present(driver):
    """If a 'Continue shopping' button appears, click it and return True. Otherwise return False.

    Looks for the exact button the user reported and a few tolerant XPaths. Logs actions.
    """
    try:
        # Exact XPath provided by user (full path)
        exact_xpath = "/html/body/div/div[1]/div[3]/div/div/form/div/div/span/span/button"
        try:
            btns = driver.find_elements(By.XPATH, exact_xpath)
            for b in btns:
                if b.is_displayed():
                    logger.info('Detected exact Continue shopping button (XPath). Clicking it...')
                    try:
                        b.click()
                    except Exception:
                        driver.execute_script('arguments[0].click();', b)
                    time.sleep(0.6 + random.random() * 0.4)
                    logger.info('Clicked Continue shopping (exact)')
                    return True
        except Exception:
            pass

        # Tolerant XPath: button with class a-button-text or alt/text containing 'Continue shopping'
        tolerant_xp = "//button[contains(@class,'a-button-text') and (contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'continue shopping') or contains(translate(@alt,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'continue shopping'))]"
        try:
            els = driver.find_elements(By.XPATH, tolerant_xp)
            for e in els:
                if e.is_displayed():
                    logger.info('Detected Continue shopping button (tolerant). Clicking it...')
                    try:
                        e.click()
                    except Exception:
                        driver.execute_script('arguments[0].click();', e)
                    time.sleep(0.6 + random.random() * 0.4)
                    logger.info('Clicked Continue shopping (tolerant)')
                    return True
        except Exception:
            pass

    except Exception:
        pass
    return False


def handle_sorry_page(driver, product_url=None, max_retries=3):
    """Detect Amazon error/sorry page (dog image) and try to recover.

    Returns True if page appears OK after recovery, False if still bad.
    """
    for attempt in range(1, max_retries + 1):
        src = (driver.page_source or '').lower()
        # Detect the well-known dog/error image or textual indicators
        if 'error/500_503.png' not in src and 'sorry! something went wrong' not in src and 'we\'re sorry' not in src:
            return True

        logger.warning(f"Detected Amazon error page (attempt {attempt}/{max_retries}). Trying recovery...")

        # 1) click continue shopping if present
        clicked = click_continue_shopping_if_present(driver)
        if clicked:
            logger.info('Clicked Continue shopping while recovering from error')

        # 2) small backoff and refresh
        backoff = 1.5 * attempt + random.random()
        logger.info(f'Waiting {backoff:.1f}s before refresh')
        time.sleep(backoff)
        try:
            driver.refresh()
        except Exception:
            try:
                # fallback: navigate to amazon home then back
                driver.get('https://www.amazon.com')
                time.sleep(1.5 + random.random())
                if product_url:
                    driver.get(product_url)
            except Exception:
                pass

        time.sleep(1.8 + random.random())

        # 3) dismiss overlays if any
        try:
            # best-effort only
            click_continue_shopping_if_present(driver)
        except Exception:
            pass

        # 4) after retries, try clearing cookies/localStorage once on second attempt
        if attempt == 2:
            try:
                driver.execute_script('window.localStorage.clear(); window.sessionStorage.clear();')
                driver.delete_all_cookies()
                logger.info('Cleared local/session storage and cookies as part of recovery')
            except Exception:
                pass

    # If we exit the loop, final check
    final_src = (driver.page_source or '').lower()
    if 'error/500_503.png' in final_src or 'sorry! something went wrong' in final_src:
        logger.error('Recovery failed — still seeing Amazon error page')
        return False
    logger.info('Recovery succeeded — page appears normal')
    return True

# ========================= EXTRACTION FUNCTIONS (Modular) =========================
def extract_title(soup): return soup.select_one("#productTitle").get_text(strip=True) if soup.select_one("#productTitle") else "N/A"


def extract_price(soup):
    try:
        whole = soup.select_one(".a-price-whole")
        fraction = soup.select_one(".a-price-fraction")
        symbol = soup.select_one(".a-price-symbol")

        if not whole or not symbol:
            return None

        whole_text = whole.get_text(strip=True)
        fraction_text = fraction.get_text(strip=True) if fraction else ""
        symbol_text = symbol.get_text(strip=True)

        if fraction_text:
            return f"{whole_text}{fraction_text} {symbol_text}"
        else:
            return f"{whole_text} {symbol_text}"

    except:
        return None

def extract_rating(soup):
    el = soup.select_one('span.a-icon-alt')
    return el.get_text(strip=True).split()[0] if el else "N/A"

def extract_brand(soup):
    el = soup.select_one('#bylineInfo, a#bylineInfo')
    return el.get_text(strip=True).replace("Visit the", "").replace("Store", "").replace("Brand:", "").strip() if el else "N/A"

def extract_color(soup):
    el = soup.select_one('span.selection, #inline-twister-expanded-dimension-text-color_name')
    return el.get_text(strip=True) if el else "N/A"

def extract_product_details(soup):
    details = {}
    for row in soup.select('.product-facts-detail'):
        label = row.select_one('.a-col-left span.a-color-base')
        value = row.select_one('.a-col-right span.a-color-base')
        if label and value:
            key = label.get_text(strip=True).rstrip(":").lower().replace(" ", "_").replace("-", "_")
            details[key] = value.get_text(strip=True)
    return details

def extract_about_text(soup):
    ul = soup.select_one('h3:-soup-contains("About this item") ~ ul') or \
         soup.select_one('#feature-bullets ul')
    if not ul: return ""
    return "\n".join(b.get_text(strip=True) for b in ul.find_all('span', class_='a-list-item') if b.get_text(strip=True))

def extract_image_url(soup):
    img = soup.select_one('#landingImage, #imgTag')
    if not img: return None
    url = img.get('data-old-hires') or img.get('src')
    if not url and img.get('data-a-dynamic-image'):
        try: url = list(json.loads(img['data-a-dynamic-image']).keys())[0]
        except: pass
    return url

def download_image(url, path):
    if not url: return False
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=12)
        if r.status_code == 200:
            with open(path, "wb") as f:
                f.write(r.content)
            return True
    except: pass
    return False

def expand_details(driver):
    try:
        btn = driver.find_element(By.XPATH, "//a[contains(@class,'a-expander-header')]//h3[contains(text(),'Product details')]//parent::a")
        if "a-expander-collapsed" in btn.get_attribute("class"):
            driver.execute_script("arguments[0].click();", btn)
            time.sleep(1.5)
    except: pass

# ========================= MAIN SCRAPER =========================
def scrape_category(driver, category_name, node_id):
    search_url = f"{BASE_SEARCH_URL}{node_id}"
    logger.info(f"Starting → {category_name.upper()} | {search_url}")
    cat_start = datetime.now()
    driver.get(search_url)
    time.sleep(3 + random.random()*1.5)
    # If a 'Continue shopping' popup appears on the search page, click it
    if click_continue_shopping_if_present(driver):
        logger.info('Continue shopping popup dismissed on search page')
    # If the search page itself is an error/sorry page, try to recover
    if not handle_sorry_page(driver, product_url=None, max_retries=2):
        logger.warning('Search page recovery failed; continuing but results may be incomplete')

    csv_file = os.path.join(BASE_DIR, f"{category_name}_bronze.csv")
    scraped_ids = set()
    if os.path.exists(csv_file):
        with open(csv_file, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            scraped_ids = {row["id"] for row in reader}

    total_scraped = 0
    page = 1

    while total_scraped < MAX_PER_CATEGORY:
        logger.info(f"Page {page} | Already scraped: {total_scraped}/{MAX_PER_CATEGORY}")
        scroll_randomly(driver)
        time.sleep(1.5)

        soup = BeautifulSoup(driver.page_source, "html.parser")
        items = soup.select("a.a-link-normal.s-no-outline")

        if not items:
            logger.info("No items found → end of results")
            break

        for item in items:
            if total_scraped >= MAX_PER_CATEGORY:
                break
            href = item.get("href")
            if not href or "/dp/" not in href:
                continue
            product_url = urljoin("https://www.amazon.com", href.split("/ref")[0])
            pid = generate_id(product_url)
            if pid in scraped_ids:
                continue

            logger.info(f"→ [{total_scraped+1}] {product_url}")
            driver.get(product_url)
            # Gentle wait and attempt to dismiss any 'Continue shopping' before checking page content
            time.sleep(1.0 + random.random()*1.5)
            if click_continue_shopping_if_present(driver):
                logger.info('Continue shopping popup dismissed on product page')

            # If a sorry/error page appears (dog image), attempt recovery before scraping
            if not handle_sorry_page(driver, product_url=product_url, max_retries=3):
                logger.warning('Product page recovery failed — skipping this product')
                random_delay()
                continue

            try:
                WebDriverWait(driver, 12).until(EC.presence_of_element_located((By.ID, "productTitle")))
            except:
                random_delay()
                continue

            expand_details(driver)
            time.sleep(1.8)
            soup = BeautifulSoup(driver.page_source, "html.parser")

            data = {
                "id": pid,
                "url": product_url,
                "title": extract_title(soup),
                "price": extract_price(soup),
                "rating": extract_rating(soup),
                "brand": extract_brand(soup),
                "color": extract_color(soup),
            }
            data.update(extract_product_details(soup))

            # Save about text
            about = extract_about_text(soup)
            try:
                with open(os.path.join(TEXT_DIR, f"{pid}.txt"), "w", encoding="utf-8") as f:
                    f.write(about)
            except Exception:
                logger.exception(f"Failed to save about text for {pid}")

            # Save image
            img_url = extract_image_url(soup)
            if img_url:
                ok = download_image(img_url, os.path.join(IMAGES_DIR, f"{pid}.jpg"))
                if not ok:
                    logger.warning(f"Failed to download image for {pid}: {img_url}")

            # Append to CSV
            file_exists = os.path.isfile(csv_file)
            with open(csv_file, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=data.keys())
                if not file_exists:
                    writer.writeheader()
                writer.writerow(data)

            scraped_ids.add(pid)
            total_scraped += 1
            logger.info(f"Saved item {pid} — total saved for category: {total_scraped}")
            random_delay()

        # Next page?
        try:
            next_btn = driver.find_element(By.CSS_SELECTOR, "a.s-pagination-next:not(.s-pagination-disabled)")
            driver.execute_script("arguments[0].click();", next_btn)
            time.sleep(3.5)
            page += 1
        except:
            logger.info(f"No more pages for {category_name}")
            break

    logger.info(f"Finished {category_name} → {total_scraped} items saved")
    try:
        cat_elapsed = datetime.now() - cat_start
        logger.info(f"Category {category_name} duration: {str(cat_elapsed)}")
    except Exception:
        pass

# ========================= RUN ALL CATEGORIES =========================
def main():
    driver = get_driver()
    try:
        for name, node in CATEGORIES.items():
            scrape_category(driver, name, node)
            time.sleep(5)  # Pause between categories
        logger.info("ALL CATEGORIES COMPLETED!")
        logger.info("Check bronze/ folder → CSV files + images + about_item texts")
    except Exception as e:
        logger.exception("Unhandled exception in main loop")
    finally:
        try:
            total_elapsed = datetime.now() - SCRIPT_START
            logger.info(f"Script total duration: {str(total_elapsed)}")
        except Exception:
            pass
        driver.quit()

if __name__ == "__main__":
    main()