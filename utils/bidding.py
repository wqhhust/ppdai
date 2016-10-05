from selenium import webdriver
import threading
import time
import pickle
import requests
import pika
import json
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
        #print(cookie_name)
        #print(cookie_value)
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
    print(url)
    result = session.get(url)
    encoding = result.encoding
    page_text = result.text.encode(encoding)
    tree = html.fromstring(result.text.encode(encoding))
    bidding_elements = tree.findall('.//a[@class="title ell"]')
    bidding_id_list = [{"bidding_id":x.attrib["href"].split("=")[1]} for x in bidding_elements]
    print("++++++++++++++++++")
    print(bidding_id_list)
    return bidding_id_list


def merge_dicts(*dict_args):
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result


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

    def make_map(keys,values):
        return dict(zip(keys,values))
    m1 = make_map(["amount","rate","time_span"],elements_text)
    m2 = make_map(["purpose","sex","age","marry_status","education_no_proof","house_info","car_info"],table_text)
    m3 = make_map([" education_detail","school","education_level","education_method"],education_list)
    renbankcredit = False if renbankcredit is None else True
    m4 = {"ren_bank_credit":renbankcredit}
    m5 = make_map(["cnt_return_on_time","cnt_return_less_than_15","cnt_return_great_than_15"],return_history)
    m6 = make_map(["total_load_in_history","waiting_to_pay","waiting_to_get_back"],borrow_history)
    result = merge_dicts(m1,m2,m3,m4,m5,m6)
    print(result)
    return result


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


def generate_bidding_list_from_message(msg):
    page = re.sub(".*:page-number|,|:timestamp.*", "", str(msg)).strip()
    url_template = "http://invest.ppdai.com/loan/listnew?LoanCategoryId=4&SortType=2&PageIndex={}&MinAmount=0&MaxAmount=0"
    url = url_template.format(page)
    print(url)
    return_msg = get_bidding_list_after_loggin(s, url)
    if return_msg == []:
        return []
    else:
        return return_msg


def generate_bidding_detail_from_message(msg):
    json_msg = json.loads(str(msg, encoding='UTF-8'))
    bidding_id = json_msg["bidding_id"]
    print("processing bidding id of {}".format(bidding_id))
    return get_bidding_details(s,bidding_id)

def consume_queue(source_queue, target_queue,convert_function):
    def callback(ch, method, properties, body):
        print(" [x] Received %r" % body)
        msg = convert_function(body)
        ch.basic_publish("pp",target_queue,json.dumps(msg))
        time.sleep(1)

        ch.basic_ack(delivery_tag=method.delivery_tag)
    print("----------------")
    url_params = "amqp://ppdai:ppdai2016@123.206.203.97"

    parameters = pika.URLParameters(url_params)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.basic_qos(prefetch_count=1)

    # channel.exchange_declare(exchange='pp',
    #                          type="topic",
    #                          durable=True,
    #                          auto_delete=False)
    #
    # channel.queue_declare(queue=queue_name,
    #                       durable=True,
    #                       exclusive=False,
    #                       auto_delete=False,
    #                       arguments={"x-max-length": 100}
    #                       )

    channel.queue_bind(queue=source_queue,
                       exchange='pp')

    channel.basic_consume(callback,
                          queue=source_queue,
                          no_ack=False)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    connection.close()


load_cookie_to_requests(s, file)
t1 = threading.Thread(target=consume_queue,args=("middle","middle_no_detail",generate_bidding_list_from_message))
t2 = threading.Thread(target=consume_queue,args=("middle_no_detail_no_duplication","middle_with_detail",generate_bidding_detail_from_message))
t1.start()
t2.start()

#page = test()
