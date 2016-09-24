import re
import requests
import configparser
from lxml import html

def login():
    config = configparser.ConfigParser()
    config.read("/data/ppdai/blacklist.config")
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
