

import os
import dbHelper
import datetime
import urllib3
import hashlib
import requests


from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from urllib.parse import urlparse
from urllib import robotparser
from bs4 import BeautifulSoup

import time
from multiprocessing import  Manager, Process



# CONSTANTS
MAX_URL_LEN = 255
ENDING_DOMAIN = 'gov.si'
WORKERS = 4
DEFAULT_REQ_RATE = 4
INITIAL_URLS = ['http://www.e-prostor.gov.si']
VALID_DOCS = ['pdf', 'doc', 'ppt', 'docx', 'pptx']
PAGE_TYPES = ["HTML", "BINARY", "DUPLICATE", "FRONTIER"]

#INITIAL_URLS = ['http://www.e-prostor.gov.si', 'https://evem.gov.si/']

STATUS_OK = 0
STATUS_TIMEOUT = 1
STATUS_UN_EXCEPTION = 2
# SOME GLOBAL VARIABLES

# This dictionary holds all urls which were added to processing or may have been explored
handled_urls = {}
# Dict of robotParsers
site_info = {}
#
sites_dict = {}
html_dict = {}

sites_last_visited = {}


class Node:
    max_tries = 5

    def __init__(self, parsed_url):
        self.site = parsed_url.netloc
        self.targetUrl = parsed_url.geturl()
        self.parsedUrl = parsed_url
        self.tries = 0
        self.fetched = False
        self.page_type_code = PAGE_TYPES[0]

        page_type = parsed_url.path.split('.')[-1]


        if (page_type.lower() in VALID_DOCS):
            print("PAGE IN VALID DOCS type({})".format(str(page_type))+ ' url: '+self.targetUrl)
            self.page_type_code = PAGE_TYPES[1]
            self.ending = page_type

        self.status_code = ""


        #@TODO: to discuss with partner
        self.robots_content = ""
        self.sitemap_content = ""
        self.sitemap_list = []
        self.access_time = ""
        self.request_rate = 3 #@TODO

        # Holds fetched data
        self.pageData = None
        self.links = []
        self.images = []

        self.init_fetch()

    # Returns true if given node is site.
    def init_fetch(self):
        if self.parsedUrl.netloc not in site_info:
            print(str(self.parsedUrl.netloc) +  " not in site_info")
            site_info[self.parsedUrl.netloc] = False
            # robotsLink = "{0}://{1}/robots.txt".format(self.parsedUrl.scheme,self.parsedUrl.netloc)
            basicUrl = "{0}://{1}".format(self.parsedUrl.scheme,self.parsedUrl.netloc)

            try:
                rs = parse_robots_and_sitemap(basicUrl)
                site_info[self.parsedUrl.netloc] = rs
                rp = rs['robots']['robots_parser']
                url_to_check = self.targetUrl
                cf = rp.can_fetch('*',url_to_check)
                print('init fetch: can fetch (try):  ' + url_to_check + " ? " + str(cf))
                self.tries = 6 if not cf else self.tries

                self.robots_content = str(rs['robots']['robots_text'])
                self.sitemap_content= str(rs['sitemap']['sitemap_text'])
                self.sitemap_list   = rs['sitemap']['sitemap_parser']
                self.request_rate = rp.crawl_delay("*")



            except Exception as e:
                # Something go wrong.
                print('Robots content fetch error... ')
                print(e)
                return False
        else:
            if site_info[self.parsedUrl.netloc] == False:

                print("site {} already in site_info but has no robots".format(self.parsedUrl.netloc))
                print(site_info)
                return False
            else:
                print('already got robots and sitemap for this site ({})'.format(self.parsedUrl.netloc))
                rp = site_info[self.parsedUrl.netloc]['robots']['robots_parser']
                cf = rp.can_fetch('*',self.targetUrl)
                print('init fetch: can fetch:  ' + self.targetUrl + " ? " + str(cf))
                self.tries = 6 if not cf else self.tries
                return cf

    # Mark node fetch was not successful
    def mark_failed(self):
        self.tries += 1

    def mark_fetched(self):
        self.fetched = True

    # Indicates if node was fetched too many times
    # If node fetch fail, we should put the node back to queue and try again, but only if Node is still valid
    def is_valid(self):
        return self.max_tries > self.tries

    def is_document(self):
        # If this is a document, we should store it and complete with investigation
        # in this node.
        for ending in VALID_DOCS:
            if self.targetUrl.endswith(ending):
                return ending
        return False

    def print_self(self):
        print("NODE STATUS: \nFetched: {0}\nLinks: {1}\nImages: {2}\nTries: {3}".format(self.fetched,len(self.links), len(self.images), self.tries))


class Worker(Process):

    def __init__(self, name, frontier_q, done_q):
        super(Worker, self).__init__()
        self.name = name
        self.frontier_q = frontier_q
        self.done_q = done_q
        self.db = None

    def run(self):
        while True:

            if self.db is None:
                self.db = dbHelper.New_dbHelper()
            self.process_node()

    def process_node(self):
        # Get node to work on it.
        try:

            work_node = self.frontier_q.get(timeout=10)
            time_between_requests = work_node.request_rate if work_node.request_rate is not None else DEFAULT_REQ_RATE

            if work_node.parsedUrl.netloc in sites_last_visited:
                time_elapsed = time.time() - sites_last_visited[work_node.parsedUrl.netloc]

                if time_elapsed < time_between_requests:
                    print('should wait some time putting node back to queue , elapsed: ' + str(
                        time_elapsed) + ", " + work_node.targetUrl)
                    self.frontier_q.put(work_node)
                    return;

            else:
                print('site not visited at all: ' + work_node.parsedUrl.netloc)

        except Exception as e:
            # TODO terminate process
            print("Worker ({0}) has no more data in frontier.\n".format(self.name))
            super(Worker, self).terminate()

        print("{} got node: {}".format(self.name, work_node.targetUrl))

        if work_node.is_valid() and not work_node.fetched:
            fetch_node(work_node)

        # Check status and place it to correct queue
        if work_node.is_valid() and work_node.fetched:
            self.done_q.put(work_node)
            # Put all links into frontier

            for sm in work_node.sitemap_list:
                parsed_link = urlparse(sm['url'])
                print('adding node from sitemap: ' + parsed_link.geturl())
                sm_node = Node(parsed_link)
                self.frontier_q.put(sm_node)

            for i in range(len(work_node.links)):

                link = work_node.links[i]
                new_node = Node(link)
                if new_node.is_valid():
                    self.frontier_q.put(new_node)
                else:
                    work_node.links[i] = None

            store_node(work_node, self.db)

        # Puts the node back to frontier
        if work_node.is_valid() and not work_node.fetched:
            self.frontier_q.put(work_node)

        self.frontier_q.task_done()


# Fetches one node and populate attributes to it
def fetch_node(node):

    try:
        if node.page_type_code == PAGE_TYPES[0]:
            result = fetch_url(node.targetUrl)
            sites_last_visited[node.parsedUrl.netloc] = time.time()
            if result['status'] == STATUS_OK:
                node.status_code = result['status_code']
                node.access_time = result['access_time']

                node.links = result['links']
                node.images = result['images']
                node.pageData = result['page_data']
                node.mark_fetched()
            else:
                # If fetch was NOK, mark node as failed
                node.mark_failed()
                return node
        elif node.page_type_code == PAGE_TYPES[1]:
            try:
                result = url_file_to_bytes(node.targetUrl)
                r = requests.get(node.targetUrl)
                node.status_code = r.status_code
                node.access_time = datetime.datetime.now()
                node.pageData = result
                node.mark_fetched()
            except:
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

    options = webdriver.ChromeOptions()
    options.add_argument('--headless')

    driver = webdriver.Chrome(options=options)

    try:
        driver.get(url)

        r = requests.get(url)
        status_code = r.status_code
        access_time = datetime.datetime.now()



        links = get_next_urls(driver)
        images = extract_images(driver)

        return {
            'status': STATUS_OK,
            'links': links,
            'images': images,
            'page_data': driver.page_source,
            'status_code':status_code,
            'access_time' : access_time,
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


def get_next_urls(driver):

    # @TODO currently only A hrefs are included - we may add some additional??
    url_parsed_links = [urlparse(element.get_attribute("href")) for element in driver.find_elements_by_xpath("//a[@href]")]
    url_parsed_links_btns = [urlparse(element.get_attribute("href")) for element in driver.find_elements_by_xpath("//button[@onclick]")]
    print('buttons click: '+str(len(url_parsed_links_btns)))
    # Remove links which are not from .gov.si
    url_parsed_links = [link for link in url_parsed_links if link.netloc.endswith(ENDING_DOMAIN)]

    to_investigate_urls = []

    # Use only urls which were not handled till now
    for url in url_parsed_links:
        # Get original url
        target_url = url.geturl()
        if target_url not in handled_urls and len(target_url) < MAX_URL_LEN:
            to_investigate_urls.append(url)
            handled_urls[target_url] = True

    return to_investigate_urls


def save_image_locally(url):
    http = urllib3.PoolManager()
    image_name = url.split("/")[-1]
    r = http.request('GET', url, preload_content=False)
    with open("images/"+image_name, 'wb') as out:
        while True:
            data = r.read()
            if not data:
                break
            out.write(data)


def url_file_to_bytes(url):
    http = urllib3.PoolManager()
    r = http.request('GET', url, preload_content=False)
    data = r.read()
    ba = bytearray(data)
    by = bytes(ba)
    return by


def extract_images(driver):
    elems = driver.find_elements_by_xpath("//img[@src]")
    images = []
    for i in elems:
        url = i.get_attribute("src")
        image_name = url.split("/")[-1]
        image_type = image_name.split(".")[-1]
        image_bytes = url_file_to_bytes(url)
        images.append({'name':image_name,
                       'data':image_bytes,
                       'type':image_type,
                       'time_stamp': datetime.datetime.now()
                       })
    return images


def store_node(n,db):
    if n.site not in sites_dict:
        site_id = db.insert_site(n.site,n.robots_content,n.sitemap_content)
        sites_dict[n.site] = site_id

    if n.page_type_code == PAGE_TYPES[0]:
        page_html = n.pageData.encode('utf-8')
        page_html_hash = hashlib.md5(page_html)

        if page_html_hash not in html_dict:
            page_id = db.insert_page(sites_dict[n.site],n.page_type_code,n.targetUrl,n.pageData,n.status_code,n.access_time)
            html_dict[page_html_hash] = page_id
            for image in n.images:
                db.insert_image(page_id, image['name'], image['type'], image['data'], image['time_stamp'])
            for link in n.links:
                if link is not None: #ugly af but it is what it is
                    target_url = link.geturl()
                    # we insert link's page to page table, but we dont set any other rows. We do that to get link's page id
                    # we will update other rows in link's page when the page is vistited through frontier
                    to_page_id = db.insert_page(sites_dict[n.site], "FRONTIER", target_url, None, None, None)
                    db.insert_link(page_id, to_page_id)
        else:
            n.page_type_code = "DUPLICATE"
            db.set_duplicate_page(html_dict[page_html_hash])
            page_id = db.insert_page(sites_dict[n.site],n.page_type_code,n.targetUrl,n.pageData,n.status_code,n.access_time)

    else:
        page_id = db.insert_page(sites_dict[n.site], n.page_type_code, n.targetUrl, None, n.status_code,n.access_time)
        page_data_id = db.insert_page_data(page_id,n.ending,n.pageData)


def get_initial_nodes():
    return [Node(urlparse(url)) for url in INITIAL_URLS]


def at_least_one_worker_active(workers):
    for w in workers:
        if w.is_alive():
            # Do not terminate yet
            return True
    return False


def parse_sitemap(url):
    xmlArray = []
    r = requests.get(url,timeout=3)
    xml = ""
    soup = BeautifulSoup(r.text)
    sitemapTags = soup.find_all("sitemap")
    if len(sitemapTags) == 0:
        sitemapTags = soup.find_all("url")
    for sitemap in sitemapTags:
        xmlArray.append({'url':sitemap.findNext("loc").text,
                         'lastmod':sitemap.findNext("lastmod").text,
                         'changefreq':sitemap.findNext("changefreq").text,
                         'priority':sitemap.findNext("priority").text})
        xml = r.text
    return {'sitemap_parser':xmlArray,'sitemap_text':xml}


def extract_sitemap_url(robots_txt):
    robots_array = robots_txt.split('\n')
    sitemap_url = None
    for line in robots_array:
        if line.lower().startswith("sitemap"):
            line_array = line.split(":",1)
            if len(line_array) > 0:
                line_array[1] = line_array[1].replace(" ", "")
                sitemap_url = line_array[1]
    return sitemap_url


def parse_robots_and_sitemap(url_in):
    url = url_in + "/robots.txt"
    return_dict = {}
    r = requests.get(url,timeout=3)
    robots_all_text = r.text
    rp = robotparser.RobotFileParser()
    rp.parse(robots_all_text.split("\r"))

    if rp.default_entry == None:
        robots_all_text = "NO_ROBOTS"

    sitemap_url = extract_sitemap_url(robots_all_text)
    if sitemap_url is not None:
        sitemap = parse_sitemap(sitemap_url)
    else:
        sitemap = parse_sitemap(url + "/sitemap.xml")

    return_dict["sitemap"] = sitemap
    return_dict["robots"]  = {"robots_parser":rp,
                              "robots_text"  :robots_all_text}
    return return_dict




if __name__ == '__main1__':
    rs = parse_robots_and_sitemap("http://www.e-prostor.gov.si")
    print(rs)


if __name__ == '__main__':
 
    print('Initializing ...')
    # Create queues
    manager = Manager()
    frontier_q = manager.Queue()
    done_q = manager.Queue()
    site_info = manager.dict()

    # Create workers
    workers = []
    for i in range(WORKERS):
        workers.append(Worker("Worker {0}".format(i), frontier_q, done_q))

    # Put initial nodes into queue
    nodes = get_initial_nodes()
    for n in nodes:
        frontier_q.put(n)
    print('Staring crawler with {0} nodes in frontier ...'.format(frontier_q.qsize()))

    time.sleep(1)

    # Start workers
    for w in workers:
        w.start()

    while at_least_one_worker_active(workers) or frontier_q.qsize() > 0:
        print('**** Qsize: {0} ****'.format(str(frontier_q.qsize())))
        time.sleep(3)

    # Wait for last pages to fetch, which are currently in worker.
    time.sleep(10)

    # Terminate al workers
    print('Terminating all workers')
    for w in workers:
        w.terminate()

    print('Stopping crawler...')
    time.sleep(10)





