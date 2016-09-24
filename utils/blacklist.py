import requests
import configparser
import re
from lxml import html
session_requests = requests.session()
login_url = "https://ac.ppdai.com/User/Login?redirect="
result = session_requests.get(login_url)
tree = html.fromstring(result.text)
config = configparser.ConfigParser()
config.read("/data/ppdai/utils/blacklist.config")
user_config = config["user"]
print(tree)
payload = {
	"UserName": user_config["username"],
	"Password": user_config["password"]
}
result = session_requests.post(
	login_url,
	data = payload,
	headers = dict(referer=login_url)
)
#result = session_requests.get("http://invest.ppdai.com/account/blacklist")


def get_delayed_bidding(url):
    print("processing {} page of ".format(url))
    result = session_requests.get(url)
    tree = html.fromstring(result.text)
    trs = tree.xpath("//table/tr")[1::2]
    for tr in trs:
        tds = tr.xpath(".//td")
        if len(tds) > 0:
            td_money = tds[1].text
            money = [float(x) for x in (re.compile("[0-9.]+").findall(td_money))]
            print(td_money,money)
            td_delay_days=tds[3].text
            delay_days = [int(x) for x in (re.compile("\d+").findall(td_delay_days))]
            print(td_delay_days,delay_days)
            print(tds[0].findall("./span")[1].attrib["listingid"])


get_delayed_bidding("http://invest.ppdai.com/account/blacklist")

#.xpath("./td")[4].text
#print(os.path.abspath(__file__))

