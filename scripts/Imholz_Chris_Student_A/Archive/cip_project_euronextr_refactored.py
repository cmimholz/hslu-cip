import sys
import time
import pandas as pd
import numpy as np
import logging
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def configure_logging():
    """Configure the logging settings."""
    log_filename = datetime.now().strftime('cip_project_script_log_%Y-%m-%d_%H-%M-%S.txt')
    logging.basicConfig(filename=log_filename, filemode='w', level=logging.INFO)
    logging.info('Script started at ' + str(datetime.now()))

def read_excel_data(filepath):
    """Read an Excel file into a DataFrame."""
    return pd.read_excel(filepath)

def initialize_webdriver():
    """Initialize and return a Chrome WebDriver."""
    return webdriver.Chrome()

def get_table_data(elements, relevant_keys):
    """Extract data from table rows based on relevant keys."""
    data = []
    for element in elements:
        cells = element.find_elements(By.TAG_NAME, 'td')
        if cells and cells[0].text.strip() in relevant_keys:
            data.append((cells[0].text.strip(), cells[1].text.strip()))
    return data

def scrape_share_information(driver, url, isin_mic):
    """Scrape general share information from the webpage."""
    full_url = url + isin_mic
    driver.get(full_url)
    share = []
    try:
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "main-wrapper")))
        name = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "header-instrument-name"))).text
        share.append(('Name', name))

        elements = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table.table-responsive tr")))
        share += get_table_data(elements, ["Currency", "Market Cap"])
    except Exception as e:
        logging.error(f"Failed to scrape general info for {isin_mic}: {str(e)}")
    return share

def scrape_esg_information(driver):
    """Scrape ESG information from the ESG tab."""
    esg_info = []
    try:
        esg_tab = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CLASS_NAME, "esg-nav-link")))
        esg_tab.click()
        time.sleep(2)  # Wait for page to load

        esg_elements = driver.find_elements(By.CSS_SELECTOR, "div.esg-content tr")
        esg_info += get_table_data(esg_elements, ["CDP", "FTSE4Good", "MSCI ESG Ratings", "Moody's ESG Solution", "Sustainalytics"])
    except Exception as e:
        logging.error(f"Failed to scrape ESG info: {str(e)}")
    return esg_info

def scrape_characteristics(driver):
    """Scrape characteristics from the Characteristics tab."""
    characteristics_info = []
    try:
        characteristics_tab = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.LINK_TEXT, "CHARACTERISTICS")))
        characteristics_tab.click()
        time.sleep(2)  # Ensure the page has loaded

        characteristics_elements = driver.find_elements(By.CSS_SELECTOR, "div.characteristics-content tr")
        characteristics_info += get_table_data(characteristics_elements, ["Type", "Sub type", "Market", "ISIN Code", "Industry", "SuperSector", "Sector", "Subsector"])
    except Exception as e:
        logging.error(f"Failed to scrape characteristics: {str(e)}")
    return characteristics_info

def save_to_csv(df, filepath):
    """Save DataFrame to a CSV file."""
    df.to_csv(filepath, sep=';', index=False)
    logging.info('Data saved to ' + filepath)

def main():
    configure_logging()
    driver = initialize_webdriver()
    try:
        df_excel = read_excel_data('/home/student/Cloud/Owncloud/SyncVM/CIP/hslu-cip/data/raw_data/indexes_to_scrap_stage1.xlsx')
        base_url = 'https://live.euronext.com/en/product/equities/'
        headers = ["Name", "Currency", "Market Cap", "CDP", "FTSE4Good", "MSCI ESG Ratings", "Moody's ESG Solution", "Sustainalytics", "Type", "Sub type", "Market", "ISIN Code", "Industry", "SuperSector", "Sector", "Subsector"]
        df = pd.DataFrame(columns=headers)

        df_excel = df_excel.head(1)

        for isin_mic in df_excel['ISIN'] + '-' + df_excel['MIC']:
            share_info = scrape_share_information(driver, base_url, isin_mic)
            esg_info = scrape_esg_information(driver)
            characteristics_info = scrape_characteristics(driver)

            all_info = dict(share_info + esg_info + characteristics_info)
            df = pd.concat([df, pd.DataFrame([all_info])], ignore_index=True)

        save_to_csv(df, '/home/student/Cloud/Owncloud/SyncVM/CIP/hslu-cip/data/clean_data/scraped_shares_data2.csv')
    finally:
        driver.quit()
        logging.info('Script ended at ' + str(datetime.now()))

if __name__ == "__main__":
    main()
