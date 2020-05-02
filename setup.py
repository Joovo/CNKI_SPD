from scrapy import cmdline

if __name__ == '__main__':
    cmdline.execute(
        'scrapy crawl sample -o sample.csv'.split()
    )