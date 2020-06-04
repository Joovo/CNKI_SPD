from scrapy import cmdline
import sys
import os
sys.path.append(os.path.abspath('.'))

if __name__ == '__main__':
    cmdline.execute(
        'scrapy crawl sample'.split()
    )