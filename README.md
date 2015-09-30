## About
This is the git repository for EIII web-crawler. EIII Web-crawler is the crawler
which has been developed as part of EIII project @ http://eiii.eu .

## Installation

EIII Web-crawler is written in Python and can be setup by using pip.

After you check out the source code,

To install dependencies

$ pip install -r requirements.txt

To setup

To a virtualenv,

$  python setup.py install

To the system Python,

$ sudo python setup.py install

### sgmlop

Note that the sgmlop requirement cannot be installed by pip. This has to be done
manually.

1. Download sgmlop from  http://effbot.org/media/downloads/sgmlop-1.1.1-20040207.zip
2. unzip the file
3. sudo python setup.py install



## Running the crawler

The crawler can be run in 2 modes - standalone and as part of a crawler server.

Stand-alone mode is useful if you want to crawl a site directly using the default rules
and save its files or dump its URLs.

$ cd eiii_crawler

$ python crawler.py -h (for options)

Crawl a site, say example.com

$ python crawler.py http://example.com

Crawler server mode can be used to run many crawlers in parallel.

First run the crawler server passing number of crawlers to be spawend using the --nprocs
parameter.

$ eiii_crawler_server --nprocs=5

...

Use the eiii_crawler_client program to run a crawl for a site.

$ python eiii_crawler_client.py http://mysite.com

Using this, you can run multiple crawls in parallel.

NOTE: eiii_crawler running in server mode has a dependency on the ttrpc project.

## Configuration

The configuration is in the file config.json. The crawler first looks for it in the
current folder, then in the $HOME folder and finally in .eiii/crawler/ folder.

## Logs

The crawler always creates a local folder from where it is run called "logs" to save the crawl logs.

A random, unique task-id is created for every crawl and the logs are saved in that name with the ".log" extension.

Also a crawl.log file is created which would append logs for every subsequent crawl.

## Stats

Crawler statistics are written to .eiii/crawler/stats folder using <task-id>.json format.


## Data store

All crawl data is stored to .eiii/crawler/store in folders arranged in cached folders
format. Headers are compressed and stored as .hdr files . If data caching is enabled
data is also stored in a similar way and used for HTTP 304 comparisons using
if-modified-since and etag HTTP header options.

## Further information

For any further information write to eiii@tingtun.no .
