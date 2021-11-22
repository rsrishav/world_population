import os
import time
import shutil
import pandas as pd

from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver

from kaggle import KaggleApi as kag_api

__version__ = "4.0.0"
MAIN_PAGE_URL = "https://worldpopulationreview.com/"
CURRENT_YEAR = 0000
FILE_NAME_LIVE = ""
DATA_FOLDER = "dataset"
DATASET_NAME = "world-population"
HISTORIC_DATA_HEADERS = ["iso_code", "country", "current_population", "updated_datetime"]
FILE_NAME_HISTORIC = os.path.join(DATA_FOLDER, f"timeseries_population_count.csv")


def kaggle_authenticate():
    api = kag_api()
    kag_api.authenticate(api)
    print("\n[INFO] Kaggle api authenticated.")
    return api


def get_live_pop(url):
    url = MAIN_PAGE_URL + url
    html_doc = get_html_doc(url)
    parse_data = BeautifulSoup(html_doc, 'html.parser')
    span_data = parse_data.find("span", {"style": "font-size: 36px;"})
    return span_data.getText()


def save_df_csv(df, path, append):
    if append:
        df.to_csv(path, index=False, mode='a', header=not os.path.exists(path))
    else:
        df.to_csv(path, index=False)
    print(f"[INFO] Data saved to {path}.")


def clear_dir(folder):
    """ Remove all the files in a folder. """
    for filename in os.listdir(folder):
        if filename == 'dataset-metadata.json':
            pass
        else:
            file_path = os.path.join(folder, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
                print(f"[INFO] File removed: {filename}")
            except Exception as e:
                print('[ERROR] Failed to delete %s. Reason: %s' % (file_path, e))


def get_html_doc(url):
    """ Get HTML page source code in STR format. """
    print()
    options = webdriver.ChromeOptions()
    options.add_argument('headless')
    driver = webdriver.Chrome(options=options)
    driver.get(MAIN_PAGE_URL)
    time.sleep(3)
    response = driver.page_source
    return response


def get_data():
    """ Scrap data from website. """
    global CURRENT_YEAR, FILE_NAME_LIVE

    final_data = list()
    data_headers = list()
    matched_country_data = pd.read_csv("country_list.csv")
    matched_countries = matched_country_data["location"].tolist()

    html_document_1 = get_html_doc(MAIN_PAGE_URL)
    table_1 = BeautifulSoup(html_document_1, 'html.parser')
    headers = table_1.find("thead", {"class": "jsx-2636433790"})
    table_body = table_1.find("tbody", {"class": "jsx-2636433790"})

    for th in headers.find("tr"):
        th_text = th.text
        th_text = th_text.strip().lower().replace(" ", "_").replace("(", "").replace(")", "")
        if th_text == "flag":
            th_text = "iso_code"
        if th_text.__contains__("live"):
            th_text = th_text.replace("live", "last_updated")
            CURRENT_YEAR = th_text[:4]
        if th_text.__contains__("km²"): th_text = th_text[:len(th_text) - 3] + "sq_km"
        data_headers.append(th_text)

    for trow in table_body.find_all("tr"):
        row_data = list()
        country_name_tag = trow.find(href=True)
        country_name = country_name_tag.text
        if country_name in matched_countries:
            for td in trow:
                text = td.get_text()
                if text == "":
                    iso_code = matched_country_data[matched_country_data["location"] == country_name]["iso_code"].iloc[
                        0]
                    text = iso_code
                if text.__contains__("km²"): text = text[:len(text) - 3] + "sq_km"
                row_data.append(text)
            final_data.append(row_data)

    FILE_NAME_LIVE = os.path.join(DATA_FOLDER, f"{CURRENT_YEAR}_population.csv")
    df = pd.DataFrame(data=final_data, columns=data_headers)
    df_historic = df[df.columns[0:3]]
    df_historic = df_historic.assign(updated_date=datetime.utcnow())
    df_historic.columns = HISTORIC_DATA_HEADERS
    print(df)
    save_df_csv(df, FILE_NAME_LIVE, False)
    save_df_csv(df_historic, FILE_NAME_HISTORIC, True)


def publish_data(api, path):
    response = kag_api.dataset_create_version(api, path, f"Dataset updated till (UTC): {datetime.utcnow()}",
                                              convert_to_csv=True, delete_old_versions=False)
    print(f"[INFO] Kaggle Dataset uploaded.")
    clear_dir(path)


def kaggle_dataset_download(api, dataset_name, path):
    kag_api.dataset_download_files(api, dataset_name, unzip=True, path=path)
    print("[INFO] Dataset downloaded.")


if __name__ == '__main__':
    api = kaggle_authenticate()
    kaggle_dataset_download(api, DATASET_NAME, DATA_FOLDER)
    get_data()
    publish_data(api, DATA_FOLDER)
