

import os
import dbHelper
import datetime




from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from urllib.parse import urlparse

from multiprocessing import Process, Pool, Queue


# CONSTANTS
MAX_URL_LEN = 255
ENDING_DOMAIN = 'gov.si'


STATUS_OK = 0
STATUS_TIMEOUT = 1
STATUS_UN_EXCEPTION = 2
# SOME GLOBAL VARIABLES

# This dictionary holds all urls which were added to processing or may have been explored
handled_urls = {}


class Node:
    max_tries = 5
    page_types = ["HTML","BINARY","DUPLICATE","FRONTIER"]

    def __init__(self, site, targetUrl):
        self.site = site
        self.targetUrl = targetUrl
        self.tries = 0
        self.fetched = False

        #@TODO: to discuss with partner
        self.domain = ""
        self.robots_content = ""
        self.sitemap_content = ""
        self.page_type_code = self.page_types[0]


        # Holds fetched data
        self.pageData = None
        self.links = []
        self.images = []

    # Mark node fetch was not successful
    def mark_failed(self):
        self.tries += 1

    def mark_fetched(self):
        self.fetched = True

    # Indicates if node was fetched too many times
    # If node fetch fail, we should put the node back to queue and try again, but only if Node is still valid
    def is_valid(self):
        return self.max_tries > self.tries

    def print_self(self):
        print("NODE STATUS: \nFetched: {0}\nLinks: {1}\nImages: {2}".format(self.fetched,len(self.links), len(self.images)))


def get_next_urls(driver):

    # @TODO currently only A hrefs are included - we may add some additional??
    url_parsed_links = [urlparse(element.get_attribute("href")) for element in driver.find_elements_by_xpath("//a[@href]")]

    # Remove links which are not from .gov.si
    url_parsed_links = [link.geturl() for link in url_parsed_links if link.netloc.endswith(ENDING_DOMAIN)]

    to_investigate_urls = []

    # Use only urls which were not handled till now
    for url in url_parsed_links:
        if url not in handled_urls and len(url) < MAX_URL_LEN:
            to_investigate_urls.append(url)
            handled_urls[url] = True

    return to_investigate_urls

#@TODO currenly this is useless
def extract_images(driver):
    elems = driver.find_elements_by_xpath("//img[@src]")
    images = []
    for i in elems:
        print('image..')
        print(i.get_attribute("src"))
        images.append(i.get_attribute("src"))
    return []


# Fetches one node and populate attributes to it
def fetch_node(node):
    try:
        result = fetch_url(node.targetUrl)
        if result['status'] == STATUS_OK:
            node.links = result['links']
            node.images = result['images']
            node.pageData = result['page_data']
            node.mark_fetched()
        else:
            # If fetch was NOK, mark node as failed
            node.mark_failed()
            return node
    except:
        # If exception occur, mark node as failed
        node.mark_failed()
    finally:
        return node


# Fetch one url and return dict with info
def fetch_url(url, headless = True):
    print("Fetching: {0} ...".format(url))
    options = Options()
    options.add_argument("--headless")
    driver = webdriver.Chrome(options=options)

    try:
        driver.get(url)
        links = get_next_urls(driver)
        images = extract_images(driver)

        return {
            'status': STATUS_OK,
            'links': links,
            'images': images,
            'page_data': driver.page_source
        }
        driver.quit()

    except TimeoutException:
        driver.quit()
        return {
            'status': STATUS_TIMEOUT,  # Timeout
            'message': 'Timeout'
        }
    except:
        driver.quit()
        return {
            'status': STATUS_UN_EXCEPTION,  # Unknown exception
            'message': 'Timeout'
        }

#@TODO deprecated
# def extract_data(data):
#     return 'not implemented'

#
#
# def is_duplicate(page):
#     return 'not implemented'
#
#
# def get_next_url():
#     return  'not implemented'
#
#
# def store_new_links():
#     return 'not implemented'
#
#
# def store_page():
#     return 'not implemented'


def initialize_crawler(process_number):
    p = Pool(5)


# This method should store node info to database (with all related data)
# Some attributes might be added on Node in future class if needed
def store_node(n):
    site_id = dbHelper.insert_site(cursor,n.site,n.robots_content,n.sitemap_content)
    page_id = dbHelper.insert_page(cursor,site_id,n.page_type_code,n.targetUrl,n.pageData)
    for link in n.links:
        ...
        #@TODO:helper method to query all page_ids by given url; to discuss
        # dbHelper.insert_link(cursor,page_id,)
    for image in n.images:
        #@TODO: what is content_type and how to get filename
        dbHelper.insert_image(cursor,page_id,image.name,".png",image,datetime.datetime.now())







if __name__ == '__main__':
    cursor = dbHelper.connect()
    cursor.close()

    print('start')

    # SAMPLE...
    #@TODO implement queue and take nodes from it.
    n = Node("http://www.e-prostor.gov.si","http://www.e-prostor.gov.si/")
    fetch_node(n)
    n.print_self()

    if n.fetched:
        store_node(n)







