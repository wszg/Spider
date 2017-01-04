__author__ = 'ZG'
import re
import urllib2
import yaml
import getopt
import sys
import Queue
import time
from threadpool import ThreadPool
import logging
from urlparse import urlparse

logger = logging.getLogger()


class Spider(object):
    def __init__(self, seedurls, output, pattern, thread_num, max_depth, crawl_timeout, crawl_interval):
        self.urlQueue = Queue.Queue()
        self.output = output
        self.pattern = pattern
        self.thread_num = thread_num
        self.max_depth = max_depth
        self.threadPool = ThreadPool(self.thread_num)
        self.crawl_timeout = crawl_timeout
        self.crawl_interval = crawl_interval
        self.current_depth = 1
        self.finishurls = set()
        self.newurls = set()

        for seed in seedurls:
            self.urlQueue.put(seed)

    def crawljob(self, url):
        try:
            content = urllib2.urlopen(url, timeout=self.crawl_timeout).read()
            print(url)
            print(self.finishurls)
            if re.compile(self.pattern).match(url):
                print('--------------')
                file = open(self.output + "/" + time.strftime("%Y%m%d%H%M%S", time.localtime()) + ".html", 'w')
                file.write(content)
                logging.info("save page with url %s", url)
            pattern = re.compile('<a href="(.*?)"', re.S)
            allurl = re.findall(pattern, content)
            for url in allurl:
                url = urlparse(url).geturl()
                if url.startswith("http") and url not in self.finishurls:
                    self.newurls.add(url)
            print 'length of newurl',len( self.newurls)
        except Exception, e:
            logger.warning(e)
        time.sleep(self.crawl_interval)

    def start(self):
        self.threadPool.startThreads()
        while self.current_depth <= self.max_depth:
            while not self.urlQueue.empty():
                url = self.urlQueue.get()
                self.threadPool.addJob(self.crawljob, url)
                self.finishurls.add(url)
                self.threadPool.workJoin()
            for url in self.newurls:
                if url not in self.finishurls:
                    self.urlQueue.put(url)
            #self.newurls = set()
            self.current_depth += 1
        self.stop()

    def stop(self):
        self.threadPool.stopThreads()


def logconfig(logfile, loglevel):
    # if os.path.isfile(logfile):
    #     os.remove(logfile)
    LEVELS = {
        1: logging.CRITICAL,
        2: logging.ERROR,
        3: logging.WARNING,
        4: logging.INFO,
        5: logging.DEBUG
    }
    level = LEVELS[loglevel]
    logging.basicConfig(filename=logfile, level=level, format='%(asctime)s %(levelname)s [line:%(lineno)d] %(message)s')


def usage():
    print "add -c to use the config"


if __name__ == '__main__':
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hc:", ["help", "config="])
    except getopt.GetoptError as err:
        print str(err)
        usage()
        sys.exit(2)
    configfile = None
    for o, a in opts:
        if o == "-c":
            configfile = a
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        else:
            assert False, "unhandled option"
    c = yaml.load(open(configfile))
    logconfig(c["logfile"], c["loglevel"])
    seedurls = []
    for url in open(c["urls"]):
        seedurls.append(url)
    spider = Spider(seedurls, c["output"], c["pattern"], c["thread"], c["max_depth"], c["crawl_timeout"],c["crawl_interval"])
    spider.start()

