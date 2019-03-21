

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
import time
from multiprocessing import  Manager, Process


# CONSTANTS
MAX_URL_LEN = 255
ENDING_DOMAIN = 'gov.si'
WORKERS = 5
INITIAL_URLS = ['http://www.e-prostor.gov.si']

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
        print("NODE STATUS: \nFetched: {0}\nLinks: {1}\nImages: {2}\nTries: {3}".format(self.fetched,len(self.links), len(self.images), self.tries))


class Worker(Process):

    def __init__(self, name, frontier_q, done_q):
        super(Worker, self).__init__()
        self.name = name
        self.frontier_q = frontier_q
        self.done_q = done_q

    def run(self):
        while True:

            # Get node to work on it.
            work_node = self.frontier_q.get()

            print("{} got to fetch: {}".format(self.name, work_node.targetUrl))

            # Do work
            fetch_node(work_node)

            # Check status and place it to correct queue
            if work_node.fetched:
                self.done_q.put(work_node)
            elif work_node.is_valid:
                # add some timeout @TODO we should add some lower priority to wait a few seconds before next fetch
                self.frontier_q.put(work_node)


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
    except Exception as e:
        print('Fetch Node Exception:')
        print(e)
        # If exception occur, mark node as failed
        node.mark_failed()
    finally:
        return node


# Fetch one url and return dict with info
def fetch_url(url, headless = True):
    print("Fetching: {0} ...".format(url))

    options = Options()
    options.add_argument('--headless')

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
        print('Fetch Url: Timeout exception')
        driver.quit()
        return {
            'status': STATUS_TIMEOUT,  # Timeout
            'message': 'Timeout'
        }
    except Exception as e:
        print('Fetch Url Exception occured:')
        print(e)
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



def initialize_crawler(process_number):
    p = Pool(5)


def get_initial_nodes():
    return [Node(url,url) for url in INITIAL_URLS]


if __name__ == '__main__':
    # cursor = dbHelper.connect()
    # cursor.close()

    print('Initializing ...')
    # Create queues
    manager = Manager()
    frontier_q = manager.Queue()
    done_q = manager.Queue()

    # Create workers
    workers = []
    for i in range(WORKERS):
        workers.append(Worker("Worker {0}".format(i), frontier_q, done_q))

    # Start workers
    for w in workers:
        w.start()

    time.sleep(3)

    # Put initial nodes into queue
    nodes = get_initial_nodes()
    for n in nodes:
        frontier_q.put(n)

    print('Staring crawler with {0} nodes in frontier'.format(frontier_q.qsize()))

    time.sleep(3)

    # Temp solution @TODO when to stop crawling. We need to stop workers somehow
    while done_q.qsize() != len(INITIAL_URLS):
        time.sleep(1)
        print('waiting..')

    print('Stopping crawler...')
    time.sleep(3)

    # Terminate al workers
    for w in workers:
        w.terminate()
    






