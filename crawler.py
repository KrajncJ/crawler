
import os
import pypyodbc

# from selenium import webdriver
# from selenium.webdriver.common.keys import Keys
# from selenium.webdriver.chrome.options import Options

chrome_path = 'C:/Program Files (x86)/Google/Chrome/Application/chrome.exe'

# def download_and_render_link(url):
#     options = Options()
#     options.add_argument("--headless")
#     options.binary_location = chrome_path
#
#     driver = webdriver.Chrome(executable_path=os.path.abspath('chromedriver'), options=options)
#     driver.get("http://fri.uni-lj.si")


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
    cursor = connect()
    # read_create_db_sql("crawldb.sql",cursor)
    insert_site(cursor,"www.nagebabe.com","XXX","PORN")
    cursor.close()





