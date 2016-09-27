import re
import psycopg2
import utils
import time
import random
from lxml import html

ppdai_username,session_requests = utils.login()
pages_count = utils.get_pages(session_requests,"http://www.ppdai.com/moneyhistory?Type=3&Time=180")
print(pages_count)

conn = psycopg2.connect("dbname=test user=test password=test port=5434 host=127.0.0.1")
pg_cursor = conn.cursor()

def get_url(page_count):
    return "http://www.ppdai.com/moneyhistory?Type=3&Time=180&page={}".format(page_count)

def get_my_biddings(url):
    print("processing {} page of ".format(url))
    result = session_requests.get(url)
    tree = html.fromstring(result.text)
    trs = tree.xpath("//table/tr")[1:]
    for tr in trs:
        tds = tr.xpath(".//td")
        if len(tds) > 0:
            td_time = tds[0].text.replace(",","")
            td_money = tds[2].text.replace(",", "")
            money = int(re.compile("\d+").findall(td_money)[0])
            td_bidding_id = tds[5].find("./a").attrib["href"]
            bidding_id=re.compile("\d+").findall(td_bidding_id)[0]
            result = [td_time,money,bidding_id]
            #print(result)
            yield  result


pg_cursor.execute("truncate table my_biddings_stage")
get_my_biddings("http://www.ppdai.com/moneyhistory?Type=3&Time=180&page=")
sql_template = "insert into my_biddings_stage (user_name,bidding_time,money,bidding_id) values ('{}','{}',{},'{}')"
sql_to_check = """
select count(*) cnt from
(select min(bidding_time)  bidding_time from my_biddings_stage where user_name='{0}') s,
(select max(bidding_time)  bidding_time from my_biddings where user_name='{0}')  d where s.bidding_time<d.bidding_time
""".format(ppdai_username)

for x in range(pages_count):
    url = get_url(x+1)
    print("processing url of "+url)
    for x in get_my_biddings(url):
        x.insert(0, ppdai_username)
        sql = sql_template.format(*x)
        print(sql)
        pg_cursor.execute(sql)
    pg_cursor.execute(sql_to_check)
    result = pg_cursor.fetchone()
    print(result[0])
    conn.commit()
    #time.sleep(random.randint(3))
    if result[0]==1:
        break

sql_insert = """
insert into my_biddings select * from my_biddings_stage where bidding_time >
(select coalesce(max(bidding_time),'2000/3/31 8:42:14') from my_biddings where user_name = '{}')
""".format(ppdai_username)
print(sql_insert)
pg_cursor.execute(sql_insert)

conn.commit()
conn.close()
