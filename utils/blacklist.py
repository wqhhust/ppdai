import re
import psycopg2
import utils

from lxml import html
ppdai_username,session_requests = utils.login()

conn = psycopg2.connect("dbname=test user=test password=test port=5434 host=127.0.0.1")
pg_cursor = conn.cursor()


def get_delayed_bidding(url):
    print("processing {} page of ".format(url))
    result = session_requests.get(url)
    tree = html.fromstring(result.text)
    trs = tree.xpath("//table/tr")[1::2]
    for tr in trs:
        tds = tr.xpath(".//td")
        if len(tds) > 0:
            td_money = tds[1].text.replace(",","")
            money = [float(x) for x in (re.compile("[0-9.]+").findall(td_money))]
            #print(td_money,money)
            td_delay_days=tds[3].text.replace(",","")
            delay_days = [int(x) for x in (re.compile("\d+").findall(td_delay_days))]
            #print(td_delay_days,delay_days)
            bidding_id = tds[0].findall("./span")[1].attrib["listingid"]
            #print(bidding_id)
            yield [bidding_id]+money+delay_days


def get_page_url(pagecount):
    return "http://invest.ppdai.com/account/blacklist?PageIndex={}&IsCalendarRequest=0".format(pagecount)


pages_count = utils.get_pages(session_requests,"http://invest.ppdai.com/account/blacklist")
pg_cursor.execute("delete from ppdai_blacklist where user_name=''".format(ppdai_username))
for x in range(pages_count):
    url = get_page_url(x+1)
    for row in get_delayed_bidding(url):
        print(row)
        row.insert(0, ppdai_username)
        row_tuple = tuple(row)
        sql = """insert into ppdai_blacklist (user_name,bidding_id,overdue,returned,total,overdue_days,max_overdue_days)
         values ({},{},{},{},{},{},{})""".format(*row_tuple)
        print(sql)
        pg_cursor.execute(sql)

conn.commit()
conn.close()


