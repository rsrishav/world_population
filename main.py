from bs4 import BeautifulSoup
import requests
import pandas as pd

MAIN_PAGE_URL = "https://worldpopulationreview.com/"


def get_html_doc(url):
    """ Get HTML page source code in STR format. """
    response = requests.get(url)
    return response.text


def get_country():
    final_data = list()
    key_dict = list()
    matched_country_data = pd.read_csv("matched_country_list.csv")
    matched_countries = matched_country_data["location"].tolist()

    html_document = get_html_doc(MAIN_PAGE_URL)
    soup = BeautifulSoup(html_document, 'html.parser')
    headers = soup.find("thead", {"class": "jsx-2642336383"})
    table_body = soup.find("tbody", {"class": "jsx-2642336383"})

    for th in headers.find("tr"):
        th_text = th.text
        th_text = th_text.strip().lower().replace(" ", "_").replace("(", "").replace(")", "")
        if th_text == "flag":
            th_text = "iso_code"
        key_dict.append(th_text)

    for trow in table_body.find_all("tr"):
        row_data = list()
        country_name_tag = trow.find(href=True)
        country_name = country_name_tag.text

        if country_name in matched_countries:
            for td in trow:
                text = td.get_text()
                if text == "":
                    iso_code = matched_country_data[matched_country_data["location"] == country_name]["iso_code"].iloc[0]
                    text = iso_code
                row_data.append(text)
            final_data.append(row_data)

    df = pd.DataFrame(data=final_data, columns=key_dict)
    df.to_csv('final.csv')


if __name__ == '__main__':
    get_country()
