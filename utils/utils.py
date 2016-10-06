import re
import requests
import configparser
import os
from os.path import dirname
from lxml import html


current_file = os.path.realpath(__file__)
config_file = os.path.join(dirname(dirname(current_file)),"blacklist.config")
#config_file = "/data/ppdai/blacklist.config"

def login():
    config = configparser.ConfigParser()
    config.read(config_file)
    user_config = config["user"]

    session_requests = requests.session()
    login_url = "https://ac.ppdai.com/User/Login?redirect="
    session_requests.get(login_url)
    ppdai_username = user_config["username"]

    payload = {
        "UserName": ppdai_username,
        "Password": user_config["password"]
    }

    result = session_requests.post(
        login_url,
        data=payload,
        headers=dict(referer=login_url)
    )
    return ppdai_username,session_requests


def get_pages(session_requests,url):
    result = session_requests.get(url)
    tree = html.fromstring(result.text)
    pages_text = tree.find('.//span[@class="pagerstatus"]').text
    pages_count = int(re.compile("\d+").findall(pages_text)[0])
    return pages_count


def get_sql():
    config = configparser.ConfigParser(interpolation=None)
    config.read(config_file)
    bidding = config["bidding"]
    result = (bidding["sql"],config.getboolean("bidding","start_firefox"))
    print(result)
    return result
