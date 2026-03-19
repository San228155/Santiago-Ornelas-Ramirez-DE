from bs4 import BeautifulSoup
import requests

def zip_web_scrapper():
    headers = {
        'User-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }

    url = 'https://www.zip-codes.com/county/tx-harris.asp'

    page = requests.get(url, headers= headers)

    soup = BeautifulSoup(page.text, 'html.parser')

    zip_table = soup.find('table', class_ = 'table table-striped table-hover table-bordered small border rounded-3 overflow-hidden sortableTbl')

    zip_table_titles = zip_table.find('thead').find_all('th')

    zip_table_titles_list = [titles.get_text(strip=True) for titles in zip_table_titles]

    columns_zip_table = zip_table.find('tbody').find_all('tr')

    rows_table = []
    for row in columns_zip_table:
        row_data = row.find_all('td')
        individual_row_data = [data.get_text(strip=True) for data in row_data]
        rows_table.append(individual_row_data)

    df_zip = spark.createDataFrame(rows_table).toDF('ZIP_Code', 'Classification', 'City', 'Population', 'Percentage_of_Population')
    return df_zip

if __name__ == "__main__":
    df = zip_web_scrapper()
    df.write\
    .mode("overwrite")\
    .saveAsTable(f"harris_county_catalog.bronze.zip")