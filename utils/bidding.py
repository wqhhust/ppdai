import threading
import time
import pickle
import glob
import requests
import pika
import json
import re
import socket
import getpass
import sqlite3
from lxml import html
from multiprocessing import Process
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException

import utils
headers = {
"User-Agent":
    "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36"
}
s = requests.session()
s.headers.update(headers)

ppdai_url = "http://www.ppdai.com"
root_directory = utils.get_root_directory()
file_pattern = root_directory +"/*.dmp"
os_user_name = getpass.getuser()
host_name = socket.gethostname()
(bidding_sql,start_firefox,url_params) = utils.get_sql()

def get_dump_files_list():
    dump_files_list = glob.glob(file_pattern)
    return dump_files_list

def dump_cookie():
    fp = webdriver.FirefoxProfile()
    fp.set_preference("http.response.timeout", 1)
    fp.set_preference("dom.max_script_run_time", 20)
    driver = webdriver.Firefox(firefox_profile=fp)
    driver.set_page_load_timeout(10)
    driver.get(ppdai_url)
    time.sleep(30)
    user_name= driver.find_element_by_class_name("hasStatusArrow").find_element_by_xpath("./a").text.strip()
    file = "{}/{}.dmp".format(root_directory,user_name)
    pickle.dump(driver.get_cookies(), open(file,"wb"))
    return driver


def load_cookie_to_requests(session,file):
    cookies = pickle.load(open(file,'rb'))
    for cookie in cookies:
        cookie_name=cookie['name']
        cookie_value=cookie['value']
        c = {cookie_name:cookie_value}
        session.cookies.update(c)
    print(len(session.cookies))


def test_dump(file):
    s.get(ppdai_url)
    load_cookie_to_requests(s,file)
    result = s.get("http://invest.ppdai.com/account/lend")
    match_count = len(re.compile("loginByPassword").findall(result.text))
    if match_count == 0:
        print("login successfully")
        tree = html.fromstring(result.text)
        total_amount = tree.find('.//span[@class="my-ac-ps-yue"]').text
        total_amount = float(re.sub("¥|,","",total_amount))
        print(total_amount)
        return (total_amount,True,file)
    else:
        print("login failed")
        return (0, False, None)


def load_cookie_to_webdriver(file):
    driver = webdriver.Firefox()
    driver.get(ppdai_url)
    cookies = pickle.load(open(file,'rb'))
    for cookie in cookies:
        driver.add_cookie(cookie)
    return driver


def get_bidding_list_after_loggin(session,url):
    result = session.get(url)
    encoding = result.encoding
    page_text = result.text.encode(encoding)
    tree = html.fromstring(result.text.encode(encoding))
    bidding_elements = tree.findall('.//a[@class="title ell"]')
    bidding_id_list = [{"bidding_id":x.attrib["href"].split("=")[1]} for x in bidding_elements]
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
    table_text = [None]*7
    if first_table is not None:
        table_text = [re.sub("\s", "", x.text) for x in first_table.findall(".//td")]

    xueli = tree.find('.//i[@class="xueli"]')
    xueji = tree.find('.//i[@class="xueji"]')
    education_list = [None,None,None,None]
    xueli_or_xueji = [x for x in [xueji, xueli] if x is not None]
    if xueli_or_xueji != []:
        xueli_or_xueji = xueli_or_xueji[0]
        education_full = xueli_or_xueji.getparent().text_content().strip()
        education_info = re.sub("^.*?（|）\b*$", "", education_full).split("，")
        print(education_info)
        if len(education_info)>1:
            education_info = [i.split("：")[1] for i in education_info]
            education_list = [education_full] + education_info
    bank_credit = tree.find('.//i[@class="renbankcredit"]')
    renbankcredit = None
    if bank_credit is not None:
        renbankcredit = bank_credit.getparent().text_content().strip()
    return_history = [None,None,None]
    return_info = re.compile(".*正常还清.*").findall(page_text)
    if len(return_info)>0:
        return_str =return_info[0]
        return_str = re.sub(".*<p>|</p>.*|次| ","",return_str)
        return_history = [int(x.split("：")[1]) for x in return_str.split("，")]

    borrow_history = [None,None,None]
    borrow = tree.findall('.//span[@class="orange"]')
    if len(borrow)>0:
        borrow_history = [float(re.sub("¥|,|\s","",x.text)) for x in borrow]

    def make_map(keys,values):
        return dict(zip(keys,values))
    m1 = make_map(["amount","rate","time_span"],[float(x) for x in elements_text])
    m2 = make_map(["purpose","sex","age","marry_status","education_no_proof","house_info","car_info"],table_text)
    if m2["age"] is not None:
        m2["age"] = int(m2["age"])
    m3 = make_map([" education_detail","school","education_level","education_method"],education_list)

    ppdai_level = tree.xpath(".//span[contains(@class, 'creditRating')]")[0].attrib["class"].replace("creditRating ", "")
    renbankcredit = False if renbankcredit is None else True
    user_name = tree.find('.//a[@class="username"]').text
    title = tree.find('.//div[@class="newLendDetailbox"]/h3/span').text
    m4 = {"ren_bank_credit":renbankcredit,"bidding_id":bidding_id,
          "ppdai_level":ppdai_level, "user_name":user_name, "title":title,
          "os_user_name":os_user_name,"hostname":host_name
          }
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

def consume_queue(source_queue, target_queue,convert_function,sleep_time):
    def callback(ch, method, properties, body):
        print(" [x] Received %r" % body)
        try:
            msg = convert_function(body)
            ch.basic_publish("pp",target_queue,json.dumps(msg))
        except Exception as e:
            print("Error from {} to {}:---------------------------------------".format(source_queue,target_queue))
            print(e)
        if sleep_time > 0:
            time.sleep(sleep_time)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    print("convert from queue of {} to queue of {}".format(source_queue,target_queue))
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
    print("close rabbitmq connection from queue {} to queue {}................".format(source_queue,target_queue))
    connection.close()



def prepare_db():
    conn = sqlite3.connect(':memory:')
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE bidding_history
    (
      ppdai_level varchar(10),
      user_name varchar(100),
      title varchar(200),
      bidding_id varchar(100),
      certificates varchar(1000),
      rate double,
      amount double,
      time_span double,
      purpose varchar(1000),
      sex varchar(10),
      marry_status varchar(100),
      education_no_proof varchar(100),
      house_info varchar(100),
      car_info varchar(100),
      education_detail varchar(500),
      school varchar(100),
      education_level varchar(100),
      education_method varchar(100),
      cnt_return_on_time integer,
      cnt_return_less_than_15 integer,
      cnt_return_great_than_15 integer,
      total_load_in_history double,
      waiting_to_pay double,
      waiting_to_get_back double,
      age double,
      cnt_successful_bidding integer,
      cnt_fail_bidding integer,
      ren_bank_credit boolean DEFAULT false,
      hukou boolean DEFAULT false,
      certificates_in_str varchar(1000),
      location varchar(100),
      category varchar(100),
      rank int,
      hostname varchar(100),
      os_user_name varchar(100),
      wsl_rank int,
      score float,
      is_211 boolean,
      is_985 boolean,
      star int
    )
    """)
    cur.execute("select count(*) from bidding_history")
    print(cur.fetchone())
    return (conn,cur)


def do_bidding(driver,bidding_id,amount):
    url = "http://invest.ppdai.com/loan/info?id={}".format(bidding_id)
    print(driver)
    driver.get(url)
    try:
        driver.find_element_by_class_name('expquickbid')
        print("bidding of {} is completed".format(bidding_id))
    except NoSuchElementException:
        total_element = driver.find_element_by_id('accountTotal')
        account_total = float(re.sub("¥|,","",total_element.text))
        bidding_left_element = driver.find_element_by_id('listRestMoney')
        bidding_left = float(re.sub("¥|,", "", bidding_left_element.text))
        element1 = driver.find_element_by_class_name('newLendDetailMoneyLeft')
        total_borrow_element = element1.find_elements_by_xpath("./dl")[0].find_element_by_xpath(".//dd")
        total_borrow = int(re.sub("¥|,", "", total_borrow_element.text))
        bidding_amount = min([int(x) for x in [bidding_left,account_total,0.3*total_borrow,amount]])
        print(bidding_left)
        print(account_total)
        print(total_borrow)
        print(bidding_amount)
        input_element = driver.find_element_by_class_name('inputAmount')
        input_element.clear()
        input_element.send_keys(str(bidding_amount))
        form_element = driver.find_element_by_class_name('inputbox').find_element_by_xpath("./input")
        form_element.click()
        bidding_element = driver.find_element_by_id("btBid")
        driver.execute_script("document.getElementById('btBid').setAttribute('visibility', 'true');");
        # driver.switch_to_alert()
        # driver.switch_to_active_element()
        time.sleep(0.5)
        for x in range(10):
            try:
                bidding_element.click()
                time.sleep(0.5)
                print("success after try {} times for bidding_id of {}".format(x+1, bidding_id))
            except Exception as e:
                print("Error: when click")
                print(e)
                time.sleep(0.5)


def get_message_from_broadcast_exchange(driver):
    (conn_sqlliet,cursor) = prepare_db()
    def callback(ch, method, properties, body):
        json_msg = json.loads(str(body, encoding='UTF-8'))
        json_msg_remove_empty_value = dict((k, v) for k, v in json_msg.items() if v)
        bidding_id = json_msg["bidding_id"]
        print(json_msg_remove_empty_value)
        placeholder = ", ".join(["?"] * len(json_msg_remove_empty_value))
        stmt = "insert into bidding_history ({columns}) values ({values});"\
            .format(columns=",".join(json_msg_remove_empty_value.keys()),values=placeholder)
        print(stmt)
        values = list(json_msg_remove_empty_value.values())
        cursor.execute(stmt, values)
        # if sex information is not available, that means the bidding is completed
        print("=====the messge from broadcast queue is: {}".format(str(json_msg_remove_empty_value)))
        if json_msg_remove_empty_value.get("sex",1) !=1:
            sql = "select {} from bidding_history where bidding_id={}".format(bidding_sql,bidding_id)
            cursor.execute(sql)
            amount = cursor.fetchone()[0]
            print("********************")
            print("the suggested amount is:{}".format(amount))
            sql = "select * from bidding_history where bidding_id={}".format(bidding_id)
            cursor.execute(sql)
            print(cursor.fetchone())
            print("********************")
            if amount>0:
                try:
                    do_bidding(driver, bidding_id, amount)
                except Exception as e:
                    print("Error: error when run do_bidding")
                    print(e)

        else:
            print("bidding of {} is completed".format(bidding_id))
        ch.basic_ack(delivery_tag=method.delivery_tag)

    parameters = pika.URLParameters(url_params)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    user_name = getpass.getuser()
    host_name = socket.gethostname()
    result = channel.queue_declare(queue='broadcast-{}-{}'.format(host_name,user_name), durable=False)
    channel.basic_qos(prefetch_count=1)
    queue_name = result.method.queue
    channel.queue_bind(queue=queue_name,
                       exchange='pp-broadcast')
    channel.basic_consume(callback,
                          queue=queue_name,
                          no_ack=False)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    connection.close()


def start_tasks(driver):
    load_cookie_to_requests(s, file)
    t1 = Process.Thread(target=consume_queue, args=("middle", "middle_no_detail", generate_bidding_list_from_message,3))
    t2 = Process.Thread(target=consume_queue, args=("middle_no_detail_no_duplication", "middle_with_detail", generate_bidding_detail_from_message,0))
    t1.start()
    t2.start()
    if start_firefox:
        print("listening on broadcast queue")
        get_message_from_broadcast_exchange(driver)


def get_cookies_file_with_max_amount():
    if len(get_dump_files_list()) > 0:
        return sorted([test_dump(x) for x in get_dump_files_list()])[-1][-1]
    else:
        dump_cookie()
        return get_cookies_file_with_max_amount()


file = get_cookies_file_with_max_amount()
print(file)
driver = None
if start_firefox:
    driver = load_cookie_to_webdriver(file)
start_tasks(driver)


#do_bidding(driver,21425176,50)
# select * from (select * from rabbitmq_bidding_history order by created_time desc limit 500) x where school is not null
#  (select * from rabbitmq_bidding_history order by created_time desc limit 500) 


# driver = dump_cookie()
