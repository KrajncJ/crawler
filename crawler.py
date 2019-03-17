
import os
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options

chrome_path = 'C:/Program Files (x86)/Google/Chrome/Application/chrome.exe'

def download_and_render_link(url):
    options = Options()
    options.add_argument("--headless")
    options.binary_location = chrome_path

    driver = webdriver.Chrome(executable_path=os.path.abspath('chromedriver'), options=options)
    driver.get("http://fri.uni-lj.si")


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



if __name__ == '__main__':
    download_and_render_link('ds')