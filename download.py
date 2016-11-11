import re
import os.path

import pandas as pd

from urllib.request import urlretrieve
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from multiprocessing.pool import ThreadPool
from timeit import default_timer as timer

def crawl_url(urlFolder):
    url = urlFolder[0]
    folder = urlFolder[1]
    
    filename = folder + os.path.basename(urlparse(url).path) # use filename in url as filename in data/
    
    if (os.path.isfile(filename)):
        return url, filename, None
    
    try:
        fh, http_message = urlretrieve(url, filename)
        return url, fh, None
    except Exception as e:
        return url, None, e

def crawl_urls(file_names):
    print("Downloading", len(file_names), "documents")
    
    start = timer()
    results = ThreadPool(32).imap_unordered(crawl_url, file_names)

    for url, fh, error in results:
        if error is None:
            print("%r ✔️ %.2fs" % (fh, timer() - start))
        else:
            print("Error fetching %r: %s" % (url, error))

    print("Elapsed Time: %s" % (timer() - start,))

# Define a Date Range:
dateRange = pd.date_range(start='2015-09-01',end='2015-09-30', freq='1D').tolist()
print("Crawling from", dateRange[0].strftime('%Y-%m-%d'), "to", dateRange[-1].strftime('%Y-%m-%d'))

# DBpedia Events
file_name = "http://events.dbpedia.org/dataset/%s.ttl"
file_names = [((file_name % ts.strftime('%Y/%m/%d')), "dbpedia/events/") for ts in dateRange]
crawl_urls(file_names)

# Phoenix Data:
file_name = "https://s3.amazonaws.com/oeda/data/current/events.full.%s.txt.zip"
file_names = [((file_name % ts.strftime('%Y%m%d')), "phoenixdata/events/") for ts in dateRange]
crawl_urls(file_names)

# GDELT v1 Events:
file_name = "http://data.gdeltproject.org/events/%s.export.CSV.zip"
file_names = [((file_name % ts.strftime('%Y%m%d')), "GDELT/v1/") for ts in dateRange]
crawl_urls(file_names)

# GDELT v1 GKG:
file_name = "http://data.gdeltproject.org/gkg/%s.gkg.csv.zip"
file_names = [((file_name % ts.strftime('%Y%m%d')), "GDELT/v1/") for ts in dateRange]
crawl_urls(file_names)

# GDELT v1 GKG Counts:
file_name = "http://data.gdeltproject.org/gkg/%s.gkgcounts.csv.zip"
file_names = [((file_name % ts.strftime('%Y%m%d')), "GDELT/v1/") for ts in dateRange]
crawl_urls(file_names)

# GDELT V2: WARNING: 17,265 items, totalling 63.4 GB!

#http://data.gdeltproject.org/gdeltv2/masterfilelist.txt
#http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt

gdelt2_eng = pd.read_csv("GDELT/v2_201509-masterfilelist.txt", sep=' ', header=None)
file_names = [(name, "GDELT/v2/") for name in gdelt2_eng[2]]
crawl_urls(file_names)

gdelt2_trans = pd.read_csv("GDELT/v2_201509-masterfilelist-translation.txt", sep=' ', header=None)
file_names = [(name, "GDELT/v2/") for name in gdelt2_trans[2]]
crawl_urls(file_names)

print("Finished!")