from selenium import webdriver
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.firefox import GeckoDriverManager
from bs4 import BeautifulSoup
from jdatetime import date as jdate, timedelta
import requests
import mysql.connector
import logging
import re
import time
from datetime import datetime

# log setting
logging.basicConfig(filename='scraper.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# header setting
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,/;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1"
}

def check_internet():
    try:
        requests.get("https://www.google.com", timeout=5)
        return True
    except:
        return False

def get_text_or_dash(element):
    return element.text.strip() if element else "-"

def parse_ad_date(date_text):
    if not date_text or date_text == "-":
        # jalali date
        today = jdate.today()
        return f"{today.year}/{today.month:02d}/{today.day:02d}"

    date_text = date_text.strip()

    shamsi_pattern = r'(\d{4})/(\d{2})/(\d{2})'
    match = re.match(shamsi_pattern, date_text)
    if match:
        year, month, day = match.groups()
        return f"{year}/{month}/{day}"

    current_date = jdate.today()

    if "ساعت پیش" in date_text:
        hours = int(re.search(r'(\d+)', date_text).group(1))
        date_obj = current_date - timedelta(hours=hours)
        return f"{date_obj.year}/{date_obj.month:02d}/{date_obj.day:02d}"
    elif "دیروز" in date_text:
        date_obj = current_date - timedelta(days=1)
        return f"{date_obj.year}/{date_obj.month:02d}/{date_obj.day:02d}"
    elif "روز پیش" in date_text:
        days = int(re.search(r'(\d+)', date_text).group(1))
        date_obj = current_date - timedelta(days=days)
        return f"{date_obj.year}/{date_obj.month:02d}/{date_obj.day:02d}"

    #unknown date format
    return f"{current_date.year}/{current_date.month:02d}/{current_date.day:02d}"

def scrape(response, url):
    try:
        print(f"Processing URL: {url}")
        car_features = {}
        soup = BeautifulSoup(response, "html.parser")
        details = soup.find('div', class_="info-wrapper")

        car_features['name'] = get_text_or_dash(details.find('h1', class_="bama-ad-detail-title__title")) if details else "-"
        car_features['mileage'] = get_text_or_dash(soup.find(string="کارکرد").find_next('p')) if soup.find(string="کارکرد") else "-"
        car_features['fuel_type'] = get_text_or_dash(soup.find(string="نوع سوخت").find_next('p')) if soup.find(string="نوع سوخت") else "-"
        car_features['exterior_color'] = get_text_or_dash(soup.find(string="رنگ بدنه").find_next('p')) if soup.find(string="رنگ بدنه") else "-"
        car_features['gearbox'] = get_text_or_dash(soup.find(string="گیربکس").find_next('p')) if soup.find(string="گیربکس") else "-"
        car_features['body_status'] = get_text_or_dash(soup.find(string="وضعیت بدنه").find_next('p')) if soup.find(string="وضعیت بدنه") else "-"
        car_features['interior_color'] = get_text_or_dash(soup.find(string="رنگ داخلی").find_next('p')) if soup.find(string="رنگ داخلی") else "-"
        car_features['description'] = get_text_or_dash(soup.find('div', class_="desc")) if soup.find('div', class_="desc") else "-"

        price_element = soup.find('div', class_="bama-ad-detail-price__section")
        if price_element:
            price_text = price_element.text.strip()
            car_features['price'] = "توافقی" if "توافقی" in price_text else price_text
        else:
            car_features['price'] = "-"

        car_features['year'] = get_text_or_dash(details.find('span', class_="bama-ad-detail-title__subtitle")) if details else "-"
        car_features['trim'] = get_text_or_dash(details.find('span', class_="bama-ad-detail-title__subtitle-dot").find_next('span')) if details else "-"

        for detail in soup.find_all('div', class_="bama-vehicle-detail-with-link__row"):
            text = detail.text.strip()
            if 'حجم موتور' in text:
                car_features['engine_size'] = text.split('\n')[2].strip() if len(text.split('\n')) > 2 else "-"
            elif 'پیشرانه' in text:
                car_features['engine_type'] = text.split('\n')[2].strip() if len(text.split('\n')) > 2 else "-"
            elif 'شتاب' in text:
                car_features['acceleration'] = text.split('\n')[1].strip() if len(text.split('\n')) > 1 else "-"
            elif 'مصرف ترکیبی' in text:
                car_features['fuel_consumption'] = text.split('\n')[2].strip() if len(text.split('\n')) > 2 else "-"

        for key in ['engine_size', 'engine_type', 'acceleration', 'fuel_consumption']:
            car_features.setdefault(key, "-")

        # ad date
        date_element = soup.find('span', class_="bama-ad-detail-title__ad-time")
        date_text = get_text_or_dash(date_element)
        car_features['date'] = parse_ad_date(date_text) 

        car_features['url'] = url

        mydb = mysql.connector.connect(
            host='localhost',
            user='root',
            password='your password here',
            database='bama',
            auth_plugin='mysql_native_password'
        )
        myCursor = mydb.cursor()

        # table settings
        myCursor.execute("""
            CREATE TABLE IF NOT EXISTS bama (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255),
                mileage VARCHAR(50),
                fuel_type VARCHAR(50),
                exterior_color VARCHAR(50),
                gearbox VARCHAR(50),
                body_status VARCHAR(100),
                interior_color VARCHAR(50),
                description TEXT,
                price VARCHAR(100),
                year VARCHAR(50),
                trim VARCHAR(100),
                engine_size VARCHAR(50),
                engine_type VARCHAR(50),
                acceleration VARCHAR(50),
                fuel_consumption VARCHAR(50),
                date VARCHAR(10),
                url VARCHAR(255) UNIQUE
            )
        """)

        myCursor.execute("SELECT COUNT(*) FROM bama WHERE url = %s", (url,))
        url_count = myCursor.fetchone()[0]

        if url_count > 0:
            print(f"URL already exists in database: {url}. Skipping...")
            logging.info(f"URL already exists in database: {url}. Skipping...")
            myCursor.close()
            mydb.close()
            return

        sql = """
        INSERT INTO bama (name, mileage, fuel_type, exterior_color, gearbox, body_status, interior_color, description, price, year, trim, engine_size, engine_type, acceleration, fuel_consumption, date, url)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = tuple(car_features.get(key) for key in ['name', 'mileage', 'fuel_type', 'exterior_color', 'gearbox', 'body_status', 'interior_color', 'description', 'price', 'year', 'trim', 'engine_size', 'engine_type', 'acceleration', 'fuel_consumption', 'date', 'url'])

        myCursor.execute(sql, values)
        mydb.commit()

        print(f"Data inserted successfully for {url}")
        logging.info(f"Data inserted successfully for {url}: {car_features}")

        myCursor.execute("SELECT COUNT(*) FROM bama")
        row_count = myCursor.fetchone()[0]
        print(f"Total rows in bama table: {row_count}")

    except Exception as e:
        print(f"Error processing {url}: {e}")
        logging.error(f"Error processing {url}: {e}")

    finally:
        myCursor.close()
        mydb.close()

def get_recent_ads():
    urls = []
    base_url = "https://bama.ir/car"

    if not check_internet():
        print("No internet connection. Please check your network.")
        logging.error("No internet connection. Please check your network.")
        return []

    options = FirefoxOptions()
    options.add_argument("--headless")
    # browser extra settings
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")

    try:
        driver = webdriver.Firefox(
            service=FirefoxService(GeckoDriverManager().install()),
            options=options
        )
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        for attempt in range(3):
            try:
                driver.set_page_load_timeout(120)
                driver.delete_all_cookies()
                driver.get(base_url)
                print("Page loaded successfully.")
                logging.info("Page loaded successfully.")

                try:
                    WebDriverWait(driver, 30).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "bama-ad-holder"))
                    )
                    print("At least one ad loaded.")
                    logging.info("At least one ad loaded.")
                except:
                    print("No ads loaded within 30 seconds.")
                    logging.warning("No ads loaded within 30 seconds.")

                time.sleep(7)

                last_height = driver.execute_script("return document.body.scrollHeight")
                for i in range(5):
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(7)
                    new_height = driver.execute_script("return document.body.scrollHeight")
                    print(f"Scroll {i+1}: Last height = {last_height}, New height = {new_height}")
                    logging.info(f"Scroll {i+1}: Last height = {last_height}, New height = {new_height}")
                    if new_height == last_height:
                        break
                    last_height = new_height

                with open("bama_page.html", "w", encoding="utf-8") as f:
                    f.write(driver.page_source)
                print("Saved page source to bama_page.html")

                soup = BeautifulSoup(driver.page_source, "html.parser")
                ads = soup.find_all('div', class_="bama-ad-holder")
                print(f"Found {len(ads)} ads on the page.")
                if not ads:
                    print("No ads found on the page. The structure of the page might have changed.")
                    logging.error("No ads found on the page. The structure of the page might have changed.")
                    print("First 1000 chars of page source:", driver.page_source[:1000])
                    driver.quit()
                    return []

                for ad in ads:
                    link = ad.find('a', class_="bama-ad listing")
                    if not link:
                        print("No link found for an ad!")
                        continue
                    full_url = link['href']
                    if not full_url.startswith("https://bama.ir"):
                        full_url = f"https://bama.ir{full_url}"
                    urls.append(full_url)

                break

            except Exception as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                logging.error(f"Attempt {attempt + 1} failed: {e}")
                time.sleep(10)
                if attempt == 2:
                    print("Failed to load page after 3 attempts.")
                    logging.error("Failed to load page after 3 attempts.")
                    driver.quit()
                    return []
        driver.quit()
    except Exception as e:
        print(f"Firefox Webdriver Error: {e}")
        logging.error(f"Firefox Webdriver Error: {e}")

    print(f"Total URLs collected: {len(urls)}")
    return urls

urls = get_recent_ads()
print(f"Starting to process {len(urls)} URLs.")
for url in urls:
    if not check_internet():
        print("No internet connection. Please check your network.")
        logging.error("No internet connection. Please check your network.")
        break

    for attempt in range(3):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code == 200:
                scrape(response.text, url)
                break
            else:
                print(f"Failed to retrieve {url}. Status code: {response.status_code}")
                logging.error(f"Failed to retrieve {url}. Status code: {response.status_code}")
                break
        except requests.exceptions.ReadTimeout:
            print(f"Timeout error on {url}. Retrying...")
            logging.warning(f"Timeout error on {url}. Retrying...")
            time.sleep(5)
        except Exception as e:
            print(f"Error processing {url}: {e}")
            logging.error(f"Error processing {url}: {e}")
            break
    time.sleep(2)