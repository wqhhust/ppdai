import scrapy
import psycopg2
import time
import re

class PpaiSpider(scrapy.Spider):
    name = "ppdai"
    allowed_domains = ["www.ppdai.com","loan.ppdai.com"]
    start_urls = [
        "http://loan.ppdai.com/blacklist/2015"
    ]

    def parse(self, response):
        def isBlackList(x):
            return re.search("blacklist",x)
        def getPage(x):
            return x.split("/")[-1].split("_")[-1].replace('p','')
        def getPageURL(x):
            return response.url+"_m0_p"+str(x)
        pageList = response.xpath('//tr/td/a/@href').extract()
        pageNumberList = list(map(int,map(getPage,filter(isBlackList,pageList))))
        maxPageNumber = pageNumberList[-1]
        print pageNumberList
        print maxPageNumber
        print pageList
        for url in map(getPageURL,range(1,maxPageNumber)):
        #for url in list(map(getPageURL,range(1,5))):
            print url
            yield scrapy.Request(url,callback=self.parse_page_contents)

    def parse_page_contents(self,response):
        time.sleep(5)
        userList = response.xpath('//td/img/@alt').extract()
        print userList
        conn=None
        try:
            conn=psycopg2.connect(dbname="test", user="test", password="test", host="localhost")
            cursor = conn.cursor()
            for user in userList:
                sql = "insert into blackuser (user_name) values ('"+ user+ "')"
                cursor.execute(sql)
            conn.commit()
        except psycopg2.DatabaseError, e:
            print "error"
            print e
        finally:
            if conn:
                conn.close()