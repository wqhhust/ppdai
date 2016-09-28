from selenium import webdriver
import time
import pickle
import requests
import pika
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


def test():
    # dump_cookie(file)
    # load_cookie_to_webdriver(driver,file)
    result = requests.get(ppdai_url)
    load_cookie_to_requests(s, file)
    print(len(s.cookies))
    result = s.get("http://www.ppdai.com/account/lend")
    encoding = result.encoding
    print(encoding)
    print(result.text.encode(encoding))
    [print(cookie) for cookie in s.cookies]
    print(len(s.cookies))




