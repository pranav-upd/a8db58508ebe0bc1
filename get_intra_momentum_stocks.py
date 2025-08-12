import os
import re
import csv
import time
import pytz
import shutil
import random
import tempfile
import logging
import traceback
from dotenv import load_dotenv
from datetime import datetime
import math
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from algo_scripts.algotrade.scripts.trading_style.intraday.sg_momentum_stock_alerts import SgIntradayMomentumAlertsRepository, get_data_by_screener_date, get_data_by_screener_run_id


# ---------------- Load Env ---------------- #
load_dotenv()
# ---------------- Setup Logging ---------------- #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("intra-momentum")
INTRADAY_SCREENER_EMAIL = os.getenv("INTRADAY_SCREENER_EMAIL")
INTRADAY_SCREENER_PWD = os.getenv("INTRADAY_SCREENER_PWD")



#--------------------------------------------
def get_screener_run_id():
    # Generate screener_run_id and screener_date
    # ist_now is already defined in the function: ist_zone = pytz.timezone("Asia/Kolkata"); time_now = datetime.now(); ist_now = time_now.astimezone(ist_zone)
    ist_zone = pytz.timezone("Asia/Kolkata")
    time_now = datetime.now()
    ist_now = time_now.astimezone(ist_zone)
    current_minute = ist_now.minute
    # Round down to the nearest 5th minute (0, 5, 10, ..., 55)
    # For example, 9:23 -> 9:20, 9:04 -> 9:00, 9:59 -> 9:55
    #rounded_minute = math.floor(current_minute / 5) * 5

    # Round down to the nearest 10th minute (0, 5, 10, ..., 55)
    # For example, 9:23 -> 9:20, 9:04 -> 9:00, 9:59 -> 9:50
    rounded_minute = math.floor(ist_now.minute / 10) * 10

    # Handle case where original time is exactly on the hour, ensuring it's not the *previous* hour's 55th minute if that logic is too simple.
    # A direct replacement of minute should be fine.
    screener_run_time_dt = ist_now.replace(minute=rounded_minute, second=0, microsecond=0)

    screener_run_time = screener_run_time_dt.strftime("%Y-%m-%d") # Format: DD-MM-YYYY


    return screener_run_time

def read_csv_and_delete(download_dir,file_name, logger):
    csv_file = os.path.join(download_dir, file_name)
    #csv_file = f"C:\\trad-fin\\algo_platform_01_2025\\algo_scripts\\algotrade\scripts\\trading_style\\intraday\\strategies\\intraday_screener\\bwis\\{file_name}"

    logger.info(f"Reading {csv_file}..")
    try:
        with open(csv_file, mode='r', newline='', encoding='utf-8') as file:
            reader, csv_data = csv.reader(file), []
            for row in reader:
                csv_data.append(row)
        logger.info(f"Reading completed")
        logger.info(f"Deleting {csv_file}..")
        os.remove(csv_file)

    except FileNotFoundError:
        logger.error(f"Error: File not found at {file_name}")
    return csv_data

def wait_for_file(dir, filename, logger):
    try:
        time.sleep(10)
        path = os.path.join(dir, filename)
        if os.path.exists(path):
            logger.info("Successfully found csv file")
        else:
            raise FileNotFoundError
    except Exception as e:
        logger.error(f"Error: {e}")
        raise Exception
#--------------------------------------------
def write_to_db(data_towrite, logger):
    #Read the list of dictionaries containing the data.
    i = 1
    run_dt = get_screener_run_id()
    sg_momentum = SgIntradayMomentumAlertsRepository()
    try:
        for data_i in data_towrite[1:]:
            print(data_i)
            run_id = str(abs(hash(str(i))))
            re_string = data_i[4]
            new_re_string = re.sub("[^\w.\s]",'', re_string)
            sg_momentum.insert([i, run_id, run_dt, "Momentum", data_i[0], data_i[0], "Momentum", data_i[1], data_i[3], data_i[4], data_i[5], data_i[6], data_i[7], data_i[8], data_i[9], data_i[10], data_i[11], i])
            i += 1
        logger.info("Logged all rows successfully")
    except Exception as e:
        logger.error(f"Error: {e}")
        raise Exception


# ---------------- Scraper ---------------- #
def run_scraper(logger):
    logger.info("🚀 Launching browser...")
    user_data_dir = tempfile.mkdtemp(prefix="chrome-profile")
    download_dir = os.getcwd()
    file_name = "intraday_momentum_stocks.csv"
    options = webdriver.ChromeOptions()
    options.add_experimental_option("prefs", {"download.default_directory": download_dir})
    options.add_argument(f"--user-data-dir={user_data_dir}")
    driver = webdriver.Chrome(options=options)
    driver.set_window_size(1920, 1080)

    try:
        run_id = random.randint(1000, 9999)
        run_history = f"Intraday scrape on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

        # Login
        driver.get("https://intradayscreener.com/login")
        logger.info("🌐 Opened login page.")
        email_input = WebDriverWait(driver, 20).until(
            EC.visibility_of_element_located((By.XPATH, '//input[@type="email"]'))
        )
        email_input.clear()
        email_input.send_keys(INTRADAY_SCREENER_EMAIL)

        password_input = driver.find_element(By.XPATH, '//input[@type="password"]')
        password_input.clear()
        password_input.send_keys(INTRADAY_SCREENER_PWD)

        signin_btn = driver.find_element(By.XPATH, '//form//button')
        driver.execute_script("arguments[0].scrollIntoView(true);", signin_btn)
        time.sleep(1)
        driver.execute_script("arguments[0].click();", signin_btn)
        logger.info("🔐 Login submitted.")
        time.sleep(6)

        # Go to alerts page
        driver.get("https://intradayscreener.com/intraday-momentum")
        logger.info("📌 Opened Intraday Stock Alerts page.")
        time.sleep(5)

        # Try clicking CSV export
        elements = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.XPATH, "//button | //a"))
        )
        csv_clicked = False
        try:
            for el in elements:
                if "csv" in el.text.strip().lower():
                    el.click()
                    logger.info(f"📥 Clicked export element: '{el.text.strip()}'")
                    csv_clicked = True
                    break
        except Exception as click_err:
            logger.warning(f"⚠️ Could not click: {el.text.strip()} - {click_err}")

        if not csv_clicked:
            raise Exception("❌ CSV download button not found or clickable.")

        # Wait for download
        logger.info("⏳ Waiting for CSV to download...")
        wait_for_file(download_dir,file_name, logger)
        logger.info("✅ CSV downloaded successfully.")

    except Exception as e:
        logger.error(f"❌ Script failed: {e}")
        traceback.print_exc()
        driver.save_screenshot("error.png")

    finally:
        driver.quit()
        shutil.rmtree(user_data_dir, ignore_errors=True)
        logger.info("🧹 Browser closed and temp data cleaned up.")


# ---------------- Runner ---------------- #
if __name__ == "__main__":
    run_scraper(logger)
    download_dir = os.getcwd()
    file_name = "intraday_momentum_stocks.csv"
    data_final = read_csv_and_delete(download_dir, file_name, logger)
    write_to_db(data_final, logger)

