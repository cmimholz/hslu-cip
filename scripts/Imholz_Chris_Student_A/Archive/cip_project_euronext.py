#Tech with tim: https://www.youtube.com/watch?v=b5jt2bhSeXs&list=PLzMcBGfZo4-n40rB1XaJ0ak1bemvlqumQ&index=2
#Check: https://www.youtube.com/watch?v=OISEEL5eBqg&list=PLzMcBGfZo4-n40rB1XaJ0ak1bemvlqumQ&index=4
import sys

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException,TimeoutException, StaleElementReferenceException
from selenium.webdriver.common.keys import Keys # used to send keys
from selenium.webdriver.common.by import By # used to send keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC # used for exception handling
import sys
import time
import pandas as pd
import os
import numpy as np
import logging
from datetime import datetime

# @TODO: DANONE does not have a ESG button, how to handle that?
# @TODO: Name does not properly work to load.


# Configure logging
log_filename = datetime.now().strftime('script_log_%Y-%m-%d_%H-%M-%S.txt')
#logging.basicConfig(filename=log_filename, level=logging.INFO)
logging.basicConfig(filename=log_filename, filemode='w', level=logging.INFO)

# Log when the script starts
logging.info('Script started' + str(datetime.now()))

# Reading the Excel file into a DataFrame
df_excel = pd.read_excel('/home/student/Cloud/Owncloud/SyncVM/CIP/hslu-cip/data/raw_data/indexes_to_scrap_stage1.xlsx')

#For testing purpose I am only loading 3 shares of the list
#df_excel = df_excel.head(1)

# Concatenating the 'ISIN' and 'TRADING LOCATION' columns into a new column in the DataFrame
ISIN_MIC = df_excel['ISIN'] + '-' + df_excel['MIC']

#for windows
driver = webdriver.Chrome()

url = 'https://live.euronext.com/en/product/equities/'

headers = ["Name","Currency", "Market Cap", "CDP", "FTSE4Good", "MSCI ESG Ratings", "Moody's ESG Solution", "Sustainalytics", "Carbon footprint (total GHG emissions / enterprise value)",
      "Share of women in total workforce", "Rate of resignation", "Type", "Sub type", "Market", "ISIN Code", "Industry","SuperSector", "Sector", "Subsector"]

df = pd.DataFrame(columns=headers)

for i in ISIN_MIC.tolist():
    comple_url = url + i
    #//Öffnet die webseite
    #ISIN FR0000121014
    #Exchange XPAR
    driver.get(comple_url)
    #print(driver.title)

    #Returns the source code
    #print(driver.page_source)

    #inserts in search bar ISIN and presses RETURN
    #search = driver.find_element(By.ID,"edit-search-input-quote--2")
    #search.clear()
    #search.send_keys("FR0000121014")
    #search.send_keys(Keys.RETURN)

    #Wants to read the Quotes page and waits max. 10 seconds to load
    #Loads currency & market cap into a dictionary.
    share = []

    try:
        element_header_main_wrapper = WebDriverWait(driver,60).until( #waits max 10 until the page is loaded
            EC.presence_of_element_located((By.ID, "main-wrapper")))

        element_header_name = WebDriverWait(driver,60).until( #waits max 10 until the page is loaded
            EC.presence_of_element_located((By.ID, "header-instrument-name")))

        share.append(["Name",element_header_name.text])

        #share.append(["Name",element_header_main_wrapper.find_element(By.ID,"header-instrument-name").text])

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
       print("Didn't find the element in quotes:", comple_url)
       print(sys.exc_info()[0])

    try:
        #go to page ESG
        esg_button = WebDriverWait(driver,10).until( #waits max 10seconds until the page is loaded
                 EC.presence_of_element_located((By.CLASS_NAME, "nav-item.nav-link.esg-nav-link")))
        esg_button.click()
        # print(driver.page_source)
        time.sleep(3)

        # esg_ratings_block_description=driver.find_element(By.ID, "EsgRatingsBlockDescription")
        # print(esg_ratings_block_description.text)
        # your code to find and interact with the element

        esg_rating_fields = ["CDP", "FTSE4Good", "MSCI ESG Ratings", "Moody's ESG Solution", "Sustainalytics"]
        other_esg_information = ["Carbon footprint (total GHG emissions / enterprise value)", "Share of women in total workforce", "Rate of resignation"]


        for element in driver.find_elements(By.TAG_NAME, "tr"):
            td_element_list = element.find_elements(By.TAG_NAME, "td")

            if td_element_list: # check if there is an element
                field = td_element_list[0].text.strip()
                if field in esg_rating_fields:
                    # Extract the text from the first two cells for each required row
                    # Add the Ratings of the ESG Rating row to the dictionary
                    share.append([field, td_element_list[1].text.strip()])
                elif field in other_esg_information:
                    share.append([field, td_element_list[2].text.strip() +' '+ td_element_list[1].text.strip()]) # Values that do not contain an entry were deliberately taken into account. As a result, these are adjusted in data cleaning.
    except:
        print("ESG Page not found unexpected error from ISIN ", ISIN_MIC)
        print(sys.exc_info()[0])



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
        print("Characteristics Page not found from ISIN ", ISIN_MIC ,": ", sys.exc_info())

    finally:
        print(share)

    # Initialize a dictionary with all headers set to None
    row_data = {header: None for header in headers}

    # Fill the dictionary with the scraped data
    for item in share:
        if len(item) == 2:
            key, value = item
            if key in row_data:
                row_data[key] = value
        elif len(item) == 1:
            # If there is only one element in the item, it means no value was scraped for that field
            row_data[item[0]] = None

    # Convert the row dictionary to a DataFrame and concatenate it to the main DataFrame
    row_df = pd.DataFrame([row_data])
    df = pd.concat([df, row_df], ignore_index=True)

# Once you have the final DataFrame 'df' ready, you can save it to a CSV file as follows:
csv_file_path = '/home/student/Cloud/Owncloud/SyncVM/CIP/hslu-cip/data/clean_data/scraped_shares_data.csv'  # You can change this to your preferred path
df.to_csv(csv_file_path, mode='w',sep=";", index=False)  # Set index=False to exclude the index from the CSV, existing files are overwritten


# Log when the script ends
logging.info('Script ended' + str(datetime.now()))

driver.quit()

print(df)
