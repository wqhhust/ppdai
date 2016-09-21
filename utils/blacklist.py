import requests
import configparser
from lxml import html
session_requests = requests.session()
login_url = "https://ac.ppdai.com/User/Login?redirect="
result = session_requests.get(login_url)
tree = html.fromstring(result.text)

config = configparser.ConfigParser()
config.read("./../blacklist.config")
user_config = config["user"]
print(tree)
payload = {
	"UserName": user_config["username"],
	"Password": user_config["password"]
}
print(payload)
result = session_requests.post(
	login_url,
	data = payload,
	headers = dict(referer=login_url)
)
print(result.text)
result = session_requests.get("http://invest.ppdai.com/account/blacklist")
print(result.text)

