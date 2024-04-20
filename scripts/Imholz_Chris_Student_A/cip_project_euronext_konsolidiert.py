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
    """Read an Excel file into a DataFrame.

    Args:
    Filepath (str): The path to the Excel file.

    Returns:
        pd.DataFrame: DataFrame containing the Excel data.
    """
    return pd.read_excel(filepath)

def initialize_webdriver():
    """Initialize and return a Chrome WebDriver."""
    return webdriver.Chrome()

def save_to_csv(df, filepath):
    """Save DataFrame to a CSV file."""
    df.to_csv(filepath, mode = "w",sep=';', index=False)
    logging.info('Data saved to ' + filepath)


def process_table_data(elements, required_fields):
    """
    General purpose function to process HTML table rows.

    Args:
        elements (list): A list of Selenium WebElement representing table rows.
        required_fields (list): A list of field names to extract from the table.

    Returns:
        list: A list of key-value pairs (tuples) extracted based on required fields.
    """
    data = []
    for element in elements:
        cells = element.find_elements(By.TAG_NAME, 'td')
        if cells and cells[0].text.strip() in required_fields:
            data.append((cells[0].text.strip(), cells[1].text.strip()))
    return data


def process_table_data(elements, required_fields):
    """
    General purpose function to process HTML table rows.

    Args:
        elements (list): A list of Selenium WebElement representing table rows.
        required_fields (list): A list of field names to extract from the table.

    Returns:
        list: A list of key-value pairs (tuples) extracted based on required fields.
    """
    #TBD
    # data = []
    # for element in elements:
    #     cells = element.find_elements(By.TAG_NAME, 'td')
    #     if cells and cells[0].text.strip() in required_fields:
    #         data.append((cells[0].text.strip(), cells[1].text.strip()))
    # return data


def scrape_share_information(driver, url, isin_mic):
    """
    Scrape general share information from the webpage.

    Args:
        driver (webdriver): The Selenium WebDriver.
        url (str): The base URL to which ISIN_MIC will be appended.
        isin_mic (str): Concatenated ISIN and MIC for the URL.

    Returns:
        list: A list of key-value pairs of share data.
    """
    complet_url = url + isin_mic
    driver.get(complet_url)
    share = []

    try:
        element_header_main_wrapper = WebDriverWait(driver,60).until( #waits max 10 until the page is loaded
            EC.presence_of_element_located((By.ID, "main-wrapper")))

        element_header_name = WebDriverWait(driver,60).until( #waits max 10 until the page is loaded
            EC.presence_of_element_located((By.ID, "header-instrument-name")))

        share.append(["Name",element_header_name.text])

        element_table_responsive = element_header_main_wrapper.find_element(By.CLASS_NAME, "table-responsive")
        for element in element_table_responsive.find_elements(By.CSS_SELECTOR,"tr"):
            td_element_list = element.find_elements(By.CSS_SELECTOR, "td")
            match td_element_list[0].text:
                case "Currency":
                    share.append(["Currency", td_element_list[1].text])
                case "Market Cap":
                    share.append(["Market Cap", td_element_list[1].text])
            continue

    except:
       print("Didn't find the element in quotes:", complet_url)
       print(sys.exc_info()[0])
    #
    # try:
    #     WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "main-wrapper")))
    #     name = WebDriverWait(driver, 10).until(
    #         EC.presence_of_element_located((By.ID, "header-instrument-name"))).text
    #     share.append(('Name', name))
    #
    #     elements = WebDriverWait(driver, 10).until(
    #         EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table.table-responsive tr")))
    #     share += get_table_data(elements, ["Currency", "Market Cap"])
    # except Exception as e:
    #     logging.error(f"Failed to scrape general info for {isin_mic}: {str(e)}")
    return share

def scrape_esg_information(driver, isin_mic):
    """
    Scrape ESG information from the ESG tab.

    Args:
        driver (webdriver): The Selenium WebDriver.

    Returns:
        list: A list of key-value pairs of ESG data.
    """
    share = []
    try:
        # go to page ESG
        esg_button = WebDriverWait(driver, 10).until(  # waits max 10seconds until the page is loaded
            EC.presence_of_element_located((By.CLASS_NAME, "nav-item.nav-link.esg-nav-link")))
        esg_button.click()
        # print(driver.page_source)
        time.sleep(3)

        # esg_ratings_block_description=driver.find_element(By.ID, "EsgRatingsBlockDescription")
        # print(esg_ratings_block_description.text)
        # your code to find and interact with the element

        esg_rating_fields = ["CDP", "FTSE4Good", "MSCI ESG Ratings", "Moody's ESG Solution", "Sustainalytics"]
        other_esg_information = ["Carbon footprint (total GHG emissions / enterprise value)",
                                 "Share of women in total workforce", "Rate of resignation","Share of women in management bodies","Gender pay gap", "Professional equality index", "Rate of employees with disabilities", "Average training hours per employee", "Board gender diversity (female board members / total board members)", "Number of female board members","Number of board members", "Total energy consumption", " Ratio of non-recycled waste"]

        for element in driver.find_elements(By.TAG_NAME, "tr"):
            td_element_list = element.find_elements(By.TAG_NAME, "td")

            if td_element_list:  # check if there is an element
                field = td_element_list[0].text.strip()
                if field in esg_rating_fields:
                    # Extract the text from the first two cells for each required row
                    # Add the Ratings of the ESG Rating row to the dictionary
                    share.append([field, td_element_list[1].text.strip()])
                elif field in other_esg_information:
                    share.append([field, td_element_list[2].text.strip() + ' ' + td_element_list[
                        1].text.strip()])  # Values that do not contain an entry were deliberately taken into account. As a result, these are adjusted in data cleaning.
    except:
        print("ESG Page not found unexpected error from ISIN ", isin_mic)
        print(sys.exc_info()[0])
    # try:
    #     esg_tab = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CLASS_NAME, "esg-nav-link")))
    #     esg_tab.click()
    #     time.sleep(2)  # Wait for page to load
    #
    #     esg_elements = driver.find_elements(By.CSS_SELECTOR, "div.esg-content tr")
    #     esg_info += get_table_data(esg_elements, ["CDP", "FTSE4Good", "MSCI ESG Ratings", "Moody's ESG Solution", "Sustainalytics"])
    # except Exception as e:
    #     logging.error(f"Failed to scrape ESG info: {str(e)}")
    # return esg_info
    return share

def scrape_characteristics(driver, isin_mic):
    """
    Scrape characteristics from the Characteristics tab.

    Args:
        driver (webdriver): The Selenium WebDriver.

    Returns:
        list: A list of key-value pairs of characteristic data.
    """
    share = []
    #go to page Characteristics
    try:
        character_button = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.LINK_TEXT, "CHARACTERISTICS"))
        )

        character_button.click()
        characteristics_list = []
        characteristics = ["Type", "Sub type", "Market", "ISIN Code", "Industry","SuperSector", "Sector", "Subsector"]
        time.sleep(3)
        # print(driver.page_source)

        # With this approach it iterates trough many useless "td" labels. but because of the inexistens of id or unique classe names it is not possible otherwise
        for element in driver.find_elements(By.TAG_NAME, "tr"):
            element_list = element.find_elements(By.TAG_NAME, "td")

            if element_list: # check if there is an element
                field = element_list[0].text.strip()
                if field in characteristics:
                    # Extract the text from the first two cells for each required row
                    # Add the Ratings of the ESG Rating row to the dictionary
                    share.append([field, element_list[1].text.strip()])
    except:
        print("Characteristics Page not found from ISIN ", isin_mic ,": ", sys.exc_info()[0])

    finally:
        print(share)

    return share

    # try:
    #     characteristics_tab = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.LINK_TEXT, "CHARACTERISTICS")))
    #     characteristics_tab.click()
    #     time.sleep(2)  # Ensure the page has loaded
    #
    #     characteristics_elements = driver.find_elements(By.CSS_SELECTOR, "div.characteristics-content tr")
    #     characteristics_info += get_table_data(characteristics_elements, ["Type", "Sub type", "Market", "ISIN Code", "Industry", "SuperSector", "Sector", "Subsector"])
    # except Exception as e:
    #     logging.error(f"Failed to scrape characteristics: {str(e)}")
    # return characteristics_info

def load_data_in_df(share, headers):
    # Fill the dictionary with the scraped data
    # Initialize a dictionary with all headers set to None
    row_data = {header: None for header in headers}

    for item in share:
        if len(item) == 2:
            key, value = item
            if key in row_data:
                row_data[key] = value
        elif len(item) == 1:
            # If there is only one element in the item, it means no value was scraped for that field
            row_data[item[0]] = None
    return pd.DataFrame([row_data])


def main():
    configure_logging()
    driver = initialize_webdriver()

    try:
        df_excel = read_excel_data('/home/student/Cloud/Owncloud/SyncVM/CIP/hslu-cip/data/clean_data/indexes_to_scrap_stage1.xlsx')
        # Concatenating the 'ISIN' and 'TRADING LOCATION' columns into a new column in the DataFrame
        #df_excel = df_excel.head(1)

        ISIN_MIC = df_excel['ISIN'] + '-' + df_excel['MIC']
        base_url = 'https://live.euronext.com/en/product/equities/'
        headers = ["Name", "Currency", "Market Cap", "CDP", "FTSE4Good", "MSCI ESG Ratings", "Moody's ESG Solution",
                   "Sustainalytics", "Carbon footprint (total GHG emissions / enterprise value)",
                   "Share of women in total workforce", "Rate of resignation", "Type", "Sub type", "Market",
                   "ISIN Code", "Industry", "SuperSector", "Sector", "Subsector","Share of women in management bodies","Gender pay gap", "Professional equality index", "Rate of employees with disabilities", "Average training hours per employee", "Board gender diversity (female board members / total board members)", "Number of female board members","Number of board members", "Total energy consumption", " Ratio of non-recycled waste"]

        df = pd.DataFrame(columns=headers)

        for isin_mic in ISIN_MIC.tolist():

            share_info = scrape_share_information(driver, base_url, isin_mic)
            esg_info = scrape_esg_information(driver, isin_mic)
            characteristics_info = scrape_characteristics(driver, isin_mic)

            all_info = share_info + esg_info + characteristics_info
            # Convert the row dictionary to a DataFrame and concatenate it to the main DataFrame
            row_df = load_data_in_df(all_info, headers)
            df = pd.concat([df, row_df], ignore_index=True)

        save_to_csv(df, '/home/student/Cloud/Owncloud/SyncVM/CIP/hslu-cip/data/clean_data/scraped_shares_extendedESG_data2.csv')
    finally:
        driver.quit()
        logging.info('Script ended at ' + str(datetime.now()))

if __name__ == "__main__":
    main()




