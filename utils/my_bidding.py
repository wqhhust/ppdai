import re
import psycopg2
import utils
from lxml import html

ppdai_username,session_requests = utils.login()
pages_count = utils.get_pages(session_requests,"http://www.ppdai.com/moneyhistory?Type=3&Time=180")
print(pages_count)

def get_url(page_count):
    "http://www.ppdai.com/moneyhistory?Type=3&Time=180&page=".format(page_count)

def get_my_biddings(url):
    print("processing {} page of ".format(url))
    result = session_requests.get(url)
    tree = html.fromstring(result.text)
    trs = tree.xpath("//table/tr")[1:]
    for tr in trs:
        tds = tr.xpath(".//td")
        if len(tds) > 0:
            td_time = tds[0].text.replace(",","")
            print(td_time)

get_my_biddings("http://www.ppdai.com/moneyhistory?Type=3&Time=180")