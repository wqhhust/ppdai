from selenium import webdriver
import time
import pickle
import requests
import pika
import re
from lxml import html
headers = {
"User-Agent":
    "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36"
}
s = requests.session()
s.headers.update(headers)

ppdai_url = "http://www.ppdai.com"
file = "/data/ppdai/181.dmp"


def dump_cookie(file):
    fp = webdriver.FirefoxProfile()
    fp.set_preference("http.response.timeout", 1)
    fp.set_preference("dom.max_script_run_time", 1)
    driver = webdriver.Firefox(firefox_profile=fp)
    driver.set_page_load_timeout(10)
    driver.get(ppdai_url)
    time.sleep(30)
    pickle.dump(driver.get_cookies(), open(file,"wb"))
    return driver


def load_cookie_to_requests(session,file):
    cookies = pickle.load(open(file,'rb'))
    for cookie in cookies:
        cookie_name=cookie['name']
        cookie_value=cookie['value']
        print(cookie_name)
        print(cookie_value)
        c = {cookie_name:cookie_value}
        session.cookies.update(c)
    print(len(session.cookies))


def load_cookie_to_webdriver(driver,file):
    driver.get(ppdai_url)
    cookies = pickle.load(open(file,'rb'))
    for cookie in cookies:
        print("adding cookie")
        print(cookie)
        driver.add_cookie(cookie)


def get_bidding_list_after_loggin(session,url):
    result = session.get(url)
    encoding = result.encoding
    page_text = result.text.encode(encoding)
    tree = html.fromstring(result.text.encode(encoding))
    bidding_elements = tree.findall('.//a[@class="title ell"]')
    bidding_id_list = [x.attrib["href"].split("=")[1] for x in bidding_elements]
    print(bidding_id_list)


def get_bidding_details(session,bidding_id):
    url = "http://www.ppdai.com/list/{}".format(bidding_id)
    print(url)
    result = session.get(url)
    encoding = result.encoding
    page_text = result.text
    # remove some noisy elements
    page_text = re.sub("<em>.*?</em>","",page_text)
    #print(page_text)
    tree = html.fromstring(page_text)
    elements = tree.findall('.//div[@class="newLendDetailMoneyLeft"]/dl')
    elements_text = [x.find("./dd").text.replace(",","") for x in elements]
    first_table = tree.find('.//table[@class="lendDetailTab_tabContent_table1"]')
    table_text= [re.sub("\s","",x.text) for x in first_table.findall(".//td")]
    xueli = tree.find('.//i[@class="xueli"]')
    xueji = tree.find('.//i[@class="xueji"]')
    education_list = [None,None,None,None]
    xueli_or_xueji = [x for x in [xueji, xueli] if x is not None]
    if xueli_or_xueji != []:
        xueli_or_xueji = xueli_or_xueji[0]
        education_full = xueli_or_xueji.getparent().text_content().strip()
        education_info = re.sub(".*（|）.*", "", education_full).split("，")
        education_info = [i.split("：")[1] for i in education_info]
        education_list = [education_full] + education_info
    bank_credit = tree.find('.//i[@class="renbankcredit"]')
    renbankcredit = None
    if bank_credit is not None:
        renbankcredit = bank_credit.getparent().text_content().strip()
    return_history = [None,None,None]
    try:
        return_str = re.compile(".*正常还清.*").findall(page_text)[0]
        return_str = re.sub(".*<p>|</p>.*|次| ","",return_str)
        return_history = [x.split("：")[1] for x in return_str.split("，")]
    except ignore:
        pass
    borrow_history = [None,None,None]
    try:
        borrow = tree.findall('.//span[@class="orange"]')
        borrow_history = [re.sub("¥|,|\s","",x.text) for x in borrow]
    except ignore:
        pass

    print(elements_text)
    print(table_text)
    print(education_list)
    print(renbankcredit)
    print(return_history)
    print(borrow_history)
    return tree


def test():
    # dump_cookie(file)
    # load_cookie_to_webdriver(driver,file)
    result = requests.get(ppdai_url)
    load_cookie_to_requests(s, file)
    #print(len(s.cookies))
    result = s.get("http://www.ppdai.com/account/lend")
    encoding = result.encoding
    #print(encoding)
    #print(result.text.encode(encoding))
    #[print(cookie) for cookie in s.cookies]
    #print(len(s.cookies))
    tree = get_bidding_details(s,21112787)
    return tree

def consume_queue():
    def callback(ch, method, properties, body):
        print(" [x] Received %r" % body)
    url_params = "amqp://ppdai:ppdai2016@123.206.203.97"

    parameters = pika.URLParameters(url_params)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    queue_name = 'middle'

    channel.exchange_declare(exchange='pp',
                             type="topic",
                             durable=True,
                             auto_delete=False)

    channel.queue_declare(queue=queue_name,
                          durable=True,
                          exclusive=False,
                          auto_delete=False,
                          arguments={"x-max-length": 100}
                          )

    channel.queue_bind(queue=queue_name,
                       exchange='pp')
    print(queue_name)

    channel.basic_consume(callback,
                          queue=queue_name,
                          no_ack=False)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    connection.close()

page = test()
