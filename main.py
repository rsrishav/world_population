import os
import shutil
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from kaggle import KaggleApi as kag_api

__version__ = "3.0.1"
MAIN_PAGE_URL = "https://worldpopulationreview.com/"
LIVE_PAGE_URL = "https://countrymeters.info/en/list/List_of_countries_and_dependent_territories_of_the_World_by_population"
CURRENT_YEAR = 0000
FILE_NAME = ""
DATA_FOLDER = "dataset"


def get_live_pop(url):
    url = MAIN_PAGE_URL + url
    html_doc = get_html_doc(url)
    parse_data = BeautifulSoup(html_doc, 'html.parser')
    span_data = parse_data.find("span", {"style": "font-size: 36px;"})
    return span_data.getText()


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
    response = requests.get(url)
    return response.text


def get_data():
    """ Scrap data from website. """
    global CURRENT_YEAR, FILE_NAME

    final_data = list()
    key_dict = list()
    matched_country_data = pd.read_csv("country_list.csv")
    matched_countries = matched_country_data["location"].tolist()

    html_document_1 = get_html_doc(MAIN_PAGE_URL)
    table_1 = BeautifulSoup(html_document_1, 'html.parser')
    headers = table_1.find("thead", {"class": "jsx-2642336383"})
    table_body = table_1.find("tbody", {"class": "jsx-2642336383"})

    html_document_2 = get_html_doc(LIVE_PAGE_URL)
    table_2 = BeautifulSoup(html_document_2, 'html.parser')
    table = table_2.find("table", {"class": "facts"})

    live_pop = dict()
    for tr in table.find_all("tr")[1:]:
        td_tags = tr.find_all("td")
        live_pop[td_tags[2].get_text()] = f"{td_tags[3].get_text()}+{td_tags[4].get_text()}"

    for th in headers.find("tr"):
        th_text = th.text
        th_text = th_text.strip().lower().replace(" ", "_").replace("(", "").replace(")", "")
        if th_text == "flag":
            th_text = "iso_code"
        if th_text.__contains__("live"):
            th_text = th_text.replace("live", "last_updated")
            CURRENT_YEAR = th_text[:4]
        if th_text.__contains__("km²"): th_text = th_text[:len(th_text) - 3] + "sq_km"
        key_dict.append(th_text)

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
            row_data[2] = live_pop[country_name].split("+")[0]
            row_data[7] = live_pop[country_name].split("+")[1]
            final_data.append(row_data)

    FILE_NAME = os.path.join(DATA_FOLDER, f"{CURRENT_YEAR}_population.csv")
    df = pd.DataFrame(data=final_data, columns=key_dict)
    df.to_csv(FILE_NAME, index=False)
    print(f"Total population: {live_pop['Total population'].split('+')[0]}")
    print(f"[INFO] Data saved to {FILE_NAME}.")


def publish_data():
    api = kag_api()
    kag_api.authenticate(api)
    print("[INFO] Kaggle credentials authenticated.")
    response = kag_api.dataset_create_version(api, DATA_FOLDER, f"Dataset updated till (UTC): {datetime.utcnow()}",
                                              convert_to_csv=True, delete_old_versions=False)
    print(f"[INFO] Kaggle Dataset uploaded.")
    clear_dir(DATA_FOLDER)


if __name__ == '__main__':
    get_data()
    publish_data()
