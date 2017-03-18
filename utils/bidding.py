import threading
import time
import pickle
import traceback
import logging
import glob
import requests
import pika
import json
from datetime import datetime
import re
import socket
import getpass
import sqlite3
from lxml import html
from multiprocessing import Process
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.keys import Keys
from toolz import itertoolz
import utils
headers = {
"User-Agent":
    "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36"
}
http_session1 = requests.session()
http_session1.headers.update(headers)

http_session2 = requests.session()
http_session2.headers.update(headers)

ppdai_url = "http://www.ppdai.com"
root_directory = utils.get_root_directory()
file_pattern = root_directory +"/*.dmp"
os_user_name = getpass.getuser()
host_name = socket.gethostname()
(bidding_sql,start_firefox,url_params) = utils.get_sql()

def normalize_str(s):
    return s.replace("¥","").replace(",","")

def make_map(keys,values):
    return dict(zip(keys,values))

def find_element_by_class(node,tag,className):
    return node.findall('.//{}[@class="{}"]'.format(tag,className))

def get_logger(file):
    # create logger with 'spam_application'
    logger = logging.getLogger(file)
    logger.setLevel(logging.DEBUG)
    # create file handler which logs even debug messages
    fh = logging.FileHandler('{}.log'.format(file))
    fh.setLevel(logging.DEBUG)
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

logger_to_get_detail   = get_logger("detail")
logger_to_broadcast    = get_logger("broadcast")
logger_to_bidding_list = get_logger("bidding_list")
logger_to_consumer     = get_logger("consumer")
logger_to_need_bidding = get_logger("need_bidding")

def get_dump_files_list():
    dump_files_list = glob.glob(file_pattern)
    return dump_files_list

def dump_cookie():
    fp = webdriver.FirefoxProfile()
    fp.set_preference("http.response.timeout", 1)
    fp.set_preference("dom.max_script_run_time", 20)
    driver = webdriver.Firefox(firefox_profile=fp)
    driver.set_page_load_timeout(30)
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


def test_dump(http_session,file):
    http_session.get(ppdai_url)
    load_cookie_to_requests(http_session,file)
    result = http_session.get("http://invest.ppdai.com/account/lend")
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
    fp = webdriver.FirefoxProfile()
    fp.set_preference("http.response.timeout", 1)
    fp.set_preference("dom.max_script_run_time", 20)
    driver = webdriver.Firefox(firefox_profile=fp)
    driver.get(ppdai_url)
    cookies = pickle.load(open(file,'rb'))
    for cookie in cookies:
        try:
            driver.add_cookie(cookie)
        except Exception as e:
            print("Failed to load the following cookie:")
            print(cookie)
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
    # logger_to_get_detail.info("begin url get request for {}".format(url))
    result = session.get(url, timeout=30)
    logger_to_get_detail.info("url get request completed")
    encoding = result.encoding
    page_text = result.text
    # return page_text
    # remove some noisy elements
    page_text = re.sub("<em>.*?</em>","",page_text)
    #print(page_text)
    tree = html.fromstring(page_text)
    ppdai_level = tree.xpath(".//span[contains(@class, 'creditRating')]")[0].attrib["class"].replace("creditRating ", "")
    user_name = tree.find('.//a[@class="username"]').text
    title = tree.find('.//div[@class="newLendDetailbox"]/h3/span').text
    final_dict = {"bidding_id":bidding_id, "user_name":user_name, "ppdai_level":ppdai_level, "title":title}
    logger_to_get_detail.info("get html tree from bidding_id of {}".format(url))
    elements = tree.findall('.//div[@class="newLendDetailMoneyLeft"]/dl')
    logger_to_get_detail.info("there are {} elements".format(len(elements)))
    elements_text = [x.find("./dd").text.replace(",","") for x in elements]
    a = tree.findall('.//td[@class="inn"]')
    first_table = tree.find('.//table[@class="lendDetailTab_tabContent_table1"]')
    table_text = [None]*7
    spans_result = []
    spans_text = ""

    def update_user(spans_text):
        for [k,v] in spans_text:
            if k == "文化程度":
                final_dict.update({"education_level":v})
            if k == "毕业院校":
                final_dict.update({"school":v})
            if k == "学习形式":
                final_dict.update({"education_method":v})
            if k == "年龄":
                final_dict.update({"age":float(v)})
            if k == "性别":
                final_dict.update({"sex":v})
    if first_table is not None:
        print("get info for first table")
        spans_list = [x.findall('.//span')for x in first_table.findall('.//td[@class="inn"]')]
        for spans in spans_list:
            spans_result = spans_result + spans
        spans_text = [x.text.split("：") for x in spans_result]
        print(spans_text)
        update_user(spans_text)

    leader_info_list = find_element_by_class(tree,"div","lender-info")
    if len(leader_info_list) > 0:
        print(111)
        temp1 = find_element_by_class(leader_info_list[0],"p","ex col-1")
        key =[x.text.replace("：","").strip() for x in temp1]
        value= [x.find('./span').text.replace("：","").strip() for x in temp1]
        user_info = [[k,v] for k,v in zip(key,value)]
        update_user(user_info)
    print(final_dict)
    n1 = tree.findall('.//div[@class="newLendDetailMoneyLeft"]')
    print(len(n1))
    if (len(n1) > 0):
        nn = n1[0].findall('.//dd')
        try:
            a = [float(normalize_str(x.text)) for x in nn]
            b = make_map(["amount","rate","time_span"],a)
        except Exception as e:
            logger_to_get_detail.exception(e)
        else:
            final_dict.update(b)
            print(final_dict)
    n2 = tree.findall('.//p[@class="ex col-1"]')
    if len(n2)>0:
        temp_text = [x.text.split("：") for x in n2]
        temp_text = [x for x in temp_text if len(x) == 2]
        for [k,v] in temp_text:
            if k == "待还金额":
                print("bidding id of {},waiting to pay is {}".format(bidding_id,v))
                if v.strip() == "":
                    v="0"
                final_dict.update({"waiting_to_pay": float(normalize_str(v))})
    # m3 = make_map([" education_detail","school","education_level","education_method"],education_list)
    # m5 = make_map(["cnt_return_on_time","cnt_return_less_than_15","cnt_return_great_than_15"],return_history)
    # m6 = make_map(["total_load_in_history","waiting_to_pay","waiting_to_get_back"],borrow_history)
    print(final_dict)
    vs = final_dict.values()
    for a in vs:
        print(a)
        print(type(a))
    for key in final_dict.keys():
        if isinstance(final_dict[key],str):
            final_dict[key] = final_dict[key].strip()
    return final_dict


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


def generate_bidding_list_from_message(msg, http_session):
    page = re.sub(".*:page-number|,|:timestamp.*", "", str(msg)).strip()
    url_template = "http://invest.ppdai.com/loan/listnew?LoanCategoryId=4&SortType=2&PageIndex={}&MinAmount=0&MaxAmount=0"
    url = url_template.format(page)
    print(url)
    return_msg = get_bidding_list_after_loggin(http_session, url)
    if return_msg == []:
        return []
    else:
        return return_msg


def generate_bidding_detail_from_message(msg, http_session):
    json_msg = json.loads(str(msg, encoding='UTF-8'))
    bidding_id = json_msg["bidding_id"]
    ppdai_level = json_msg.get("ppdai_level",'Not-AA')
    bidding_id = itertoolz.last(bidding_id.split("="))
    print("===============")
    print(ppdai_level)
    if ppdai_level == 'AA':
        return {"bidding_id":bidding_id}
        pass
    logger_to_get_detail.info("this bidding informaiton is {}".format(str(bidding_id)))
    print(bidding_id)
    print("processing bidding id of {}".format(bidding_id))
    try:
        data = get_bidding_details(http_session,bidding_id)
        return data
    except Exception as e:
        logger_to_get_detail.warning("error when get details for bidding_id of {}".format(bidding_id))
        logger_to_get_detail.exception(e)
        return {"bidding_id":bidding_id}

def consume_queue(source_queue, target_queue,convert_function,sleep_time, http_session):
    def callback(ch, method, properties, body):
        print("get message from queue of {}, and distribute message to queue of {}".format(source_queue,target_queue))
        print(" [x] Received %r" % body)
        try:
            msg = convert_function(body, http_session)
            ch.basic_publish("pp",target_queue,json.dumps(msg))
        except Exception as e:
            print("Error from {} to {}:---------------------------------------".format(source_queue,target_queue))
            print(e)
            logger_to_consumer.exception(e)
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
    except Exception as e:
        print("error..................................")
        print(e)
        print("error end")
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
    driver.set_page_load_timeout(30)
    logger_to_broadcast.info("start bidding for {}".format(bidding_id))
    def try_get(url):
        try:
            driver.get(url)
            return True
        except Exception:
            logger_to_broadcast.info("getting page content time out, stop loading the page and re-load the page")
            return False

    for _ in range(4):
        get_result = try_get(url)
        if get_result:
            break
    if get_result == False:
        logger_to_broadcast.info("tried to reload the page several times, give up")
        raise RuntimeError("already tried many times of loading the page, give up")

    logger_to_broadcast.info("got the page for bidding of {}".format(bidding_id))
    try:
        driver.find_element_by_class_name('expquickbid')
        logger_to_broadcast.info("the bidding {} is complete".format(bidding_id))
    except NoSuchElementException:
        logger_to_broadcast.info("the bidding is not complete, begin to bid....")
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
                msg = "success after try {} times for bidding_id of {}".format(x+1, bidding_id)
                logger_to_broadcast.info(msg)
            except Exception as e:
                logger_to_broadcast.exception(e)
    except Exception as e:
        logger_to_broadcast.exception(e)
    logger_to_broadcast.info("end bidding for {}".format(bidding_id))


def get_message_from_broadcast_exchange(driver):
    (conn_sqlliet,cursor) = prepare_db()
    def callback(ch, method, properties, body):
        json_msg = json.loads(str(body, encoding='UTF-8'))
        json_msg_remove_empty_value = dict((k, v) for k, v in json_msg.items() if v)
        bidding_id = json_msg["bidding_id"]
        logger_to_broadcast.info(json_msg_remove_empty_value)
        placeholder = ", ".join(["?"] * len(json_msg_remove_empty_value))
        stmt = "insert into bidding_history ({columns}) values ({values});"\
            .format(columns=",".join(json_msg_remove_empty_value.keys()),values=placeholder)
        # logger_to_broadcast.info(stmt)
        values = list(json_msg_remove_empty_value.values())
        cursor.execute(stmt, values)
        # if sex information is not available, that means the bidding is completed
        print("=====the messge from broadcast queue is: {}".format(str(json_msg_remove_empty_value)))
        # if json_msg_remove_empty_value.get("sex",0) !=1:
        if len(json_msg_remove_empty_value) == 1:
            logger_to_broadcast.info("ignore this message, since it's not complete: {}".format(json_msg_remove_empty_value))
        if True:
            sql = "select {} suggested_amount,a.* from bidding_history a where bidding_id={}".format(bidding_sql,bidding_id)
            try:
                cursor.execute(sql)
                result = cursor.fetchone()
            except Exception as e:
                logger_to_broadcast.exception(e)
                raise Exception(sql)
            amount = result[0]
            msg = "the suggested amount is:{}, bidding is {} ".format(amount,str(result))
            if amount > 0:
                logger_to_need_bidding.info(msg)
            sql = "select * from bidding_history where bidding_id={}".format(bidding_id)
            cursor.execute(sql)
            print(cursor.fetchone())
            print("********************")
            if amount>0:
                logger_to_need_bidding("begin the bidding...")
                start_time = datetime.now()
                try:
                    do_bidding(driver, bidding_id, amount)
                except Exception as e:
                    print("Error: error when run do_bidding")
                    print(e)
                end_time = datetime.now()
                elapse_time = end_time - start_time
                elapsed_second = elapse_time.total_seconds()
                logger_to_need_bidding("end the bidding..., used seconds is {}".format(elapsed_second))

        else:
            print("bidding of {} is completed".format(bidding_id))
        ch.basic_ack(delivery_tag=method.delivery_tag)

    parameters = pika.URLParameters(url_params)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    user_name = getpass.getuser()
    host_name = socket.gethostname()
    queue_name = 'broadcast-{}-{}'.format(host_name,user_name)
    result = channel.queue_declare(queue=queue_name, exclusive = False , auto_delete=True)
    print("resut for queue creation is:".format(result))
    channel.basic_qos(prefetch_count=1)
    print("queue of {} was declared".format(queue_name))
    result = channel.queue_bind(queue=queue_name,
                       exchange='pp-broadcast',
                       routing_key = None)
    print("resut for queue bidding is:".format(result))
    print("bidding is done ....................................")
    channel.basic_consume(callback,
                          queue=queue_name,
                          no_ack=False)
    try:
        channel.start_consuming()
    except Exception as e:
        logger_to_broadcast.exception(e)
        connection.close()


def start_tasks(driver,file):
    load_cookie_to_requests(http_session1, file)
    load_cookie_to_requests(http_session2, file)
    if start_firefox:
        print("listening on broadcast queue")
        # get_message_from_broadcast_exchange(driver)
        t0 = Process(target=get_message_from_broadcast_exchange,args= (driver,))
        t0.start()
        print("process started")
    # t1 = Process(target=consume_queue, args=("middle", "middle_no_detail", generate_bidding_list_from_message,3))
    # t2 = Process(target=consume_queue, args=("middle_no_detail_no_duplication", "middle_with_detail", generate_bidding_detail_from_message,0))
    t1 = threading.Thread(target=consume_queue, args=("middle", "middle_no_detail", generate_bidding_list_from_message,3, http_session1))
    t2 = threading.Thread(target=consume_queue, args=("middle_no_detail_no_duplication", "middle_with_detail", generate_bidding_detail_from_message,0, http_session2))
    t1.start()
    t2.start()
    print("jobs all started...")


def get_cookies_file_with_max_amount(http_session):
    if len(get_dump_files_list()) > 0:
        return sorted([test_dump(http_session ,x) for x in get_dump_files_list()])[-1][-1]
    else:
        dump_cookie()
        return get_cookies_file_with_max_amount(http_session)


def get_file_and_driver():
    driver = None
    file = get_cookies_file_with_max_amount(http_session1)
    print(file)
    if start_firefox:
        driver = load_cookie_to_webdriver(file)
    # start_tasks(driver, file)
    return driver,file

def create_session():
    driver,file = get_file_and_driver()
    load_cookie_to_requests(http_session1, file)
    return http_session1

def loop_run_periodically(minutes):
    driver,file = get_file_and_driver()
    while True:
        p1 = Process(target = start_tasks, args = (driver,file))
        p1.start()
        time.sleep(60 * minutes)
        p1.terminate()
        while True:
            print("killed the process, current status is {} waiting to exit....".format(p1.is_alive()))
            print("killed the process, current status is {} waiting to exit....".format(p1.is_alive()))
            print("killed the process, current status is {} waiting to exit....".format(p1.is_alive()))
            time.sleep(1)
            if not p1.is_alive():
                break
        print("The is_alive of the process is:".format(p1.is_alive()))

def run_once():
    driver,file = get_file_and_driver()
    start_tasks(driver,file)

run_once()
# loop_run_periodically(120)

# driver = dump_cookie()
