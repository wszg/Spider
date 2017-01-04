###############################################
#   Spider for crawling urls in the webpage   #
#      author : zhuge  time: 2017/1/4         #
###############################################
import os
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

    def crawlJob(self, url):
        try:
            print "Try to crawls url in", url,
            content = urllib2.urlopen(url, timeout=self.crawl_timeout).read()
            if re.compile(self.pattern).match(url):
                file = open(self.output + "/" + time.strftime("%Y%m%d%H%M%S", time.localtime()) + ".html", 'w')
                file.write(content)
                logging.info("save page with url %s", url)
            pattern = re.compile('<a href="(.*?)"', re.S)
            allurl = re.findall(pattern, content)
            print "==>Get",len(allurl), "new urls"
            for url in allurl:
                url = urlparse(url).geturl()
                if url.startswith("http") and url not in self.finishurls:
                    self.newurls.add(url)
        except Exception, e:
            logger.warning(e)
        time.sleep(self.crawl_interval)

    def start(self):
        self.threadPool.startThreads()
        while self.current_depth <= self.max_depth:
            while not self.urlQueue.empty():
                url = self.urlQueue.get()
                self.threadPool.addJob(self.crawlJob, url)
                self.finishurls.add(url)
                self.threadPool.workJoin()
            for url in self.newurls:
                if url not in self.finishurls:
                    self.urlQueue.put(url)
            self.newurls = set()
            self.current_depth += 1
        self.stop()

    def stop(self):
        self.threadPool.stopThreads()


def logConfig(logfile, loglevel):
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
    logging.basicConfig(filename=logfile, level=level,
                        format='%(asctime)s %(levelname)s [line:%(lineno)d] %(message)s')


def usage():
    print "add -c file or --config=file to use the config"


def verifyConfigInfo(output, pattern, pList):
    if not os.path.isdir(output):
        print "The output directory \"", output, "\"is not exist.\n Press Y for create N for reconfiguration"
        create = raw_input()
        if create == "Y":
            os.mkdir(output)
        else:
            sys.exit()
    if not isinstance(pattern, str):
        print "The config parameter < pattern > should be string"
        sys.exit()
    verifyInt(pList)


def verifyInt(pList):
    for item in pList:
        if not isinstance(item, int):
            for key in c.keys():
                if c[key] == item:
                    print "The config parameter value<", key, ">should be int"
            sys.exit()


if __name__ == '__main__':
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hc:", ["help", "config="])
    except getopt.GetoptError as err:
        print str(err)
        usage()
        sys.exit(2)
    configfile = None
    for o, a in opts:
        if o in ("-c", "--config"):
            configfile = a

        elif o in ("-h", "-help", "--h", "--help"):
            usage()
            sys.exit()
        else:
            assert False, "unhandled option"
    try:
        c = yaml.load(open(configfile))
        verifyConfigInfo(c["output"], c["pattern"], (c["thread"], c["max_depth"], c["crawl_timeout"],
                                                     c["crawl_interval"], c["loglevel"]))
        logConfig(c["logfile"], c["loglevel"])
        seedurls = []
        for url in open(c["urls"]):
            seedurls.append(url)
        spider = Spider(seedurls, c["output"], c["pattern"], c["thread"], c["max_depth"],
                        c["crawl_timeout"], c["crawl_interval"])
        spider.start()
    except Exception, e:
        print e
