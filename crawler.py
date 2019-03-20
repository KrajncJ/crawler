from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from urllib.parse import urlparse

chrome_path = 'C:/Program Files (x86)/Google/Chrome/Application/chrome.exe'


def extract_links(driver):

    elems = driver.find_elements_by_xpath("//a[@href]")

    #@TODO currently only A hrefs are included - we may add some additional
    extr_links = [link.get_attribute("href") for link in elems]

    return [urlparse(url).geturl() for url in extr_links]
    # normalized_urls = []
    # for u in extr_links:
    #     normalized = urlparse(u)
    #     normalized_urls.append(normalized.geturl())
    #
    # return normalized_urls


def fetch_url(url, headless = True):
    options = Options()
    options.add_argument("--headless")
    driver = webdriver.Chrome(options=options)

    try:
        driver.get(url)
        print(driver.current_url)
        links = extract_links(driver)

        print(links)
        print(len(links))

     #   print(driver.page_source)
        driver.quit()

    except TimeoutException:
        driver.quit()
        print('MOVIE NOT YET AVAILABLE!')
        pass
    except:
        driver.quit()
        raise


def extract_data(data):
    return 'not implemented'


def is_duplicate(page):
    return 'not implemented'


def get_next_url():
    return  'not implemented'


def store_new_links():
    return 'not implemented'


def store_page():
    return 'not implemented'


def initialize_crawler(settings):
    return 'not implemented'




def connect():
    # Specifying the ODBC driver, server name, database, etc. directly
    driver = 'Devart ODBC Driver for PostgreSQL'
    cnxn = pypyodbc.connect('DRIVER={' + driver + '};SERVER=127.0.0.1;PORT=5432;DATABASE=postgresDB;uid=userDB;pwd=postgres;')
    # cnxn = pypyodbc.connect('DSN=postgres;SERVER=127.0.0.1;PORT=5432;DATABASE=postgresDB;uid=userDB;pwd=postgres;')
    # Create a cursor from the connection
    return cnxn.cursor()


def read_create_db_sql(file_name,cursor):
    with open(file_name, "r") as file:
        sqlScript = file.read()
        for statement in sqlScript.split(';'):
            #ugly af but it works
            try:
                cursor.execute(statement+';')
                cursor.commit()
                print("SUCCESS: " + statement+';')
            except:
                print("FAILED: " + statement+';')



def insert_site(cursor,domain,robots_content,sitemap_content):
    cursor.execute("insert into site(domain,robots_content,sitemap_content) values (?, ?, ?)", [domain,robots_content,sitemap_content])
    cursor.commit()



if __name__ == '__main__':
    print('start')
    fetch_url("http://www.e-prostor.gov.si/")
    # cursor = connect()
    # # read_create_db_sql("crawldb.sql",cursor)
    # insert_site(cursor,"www.nagebabe.com","XXX","PORN")
    # cursor.close()





