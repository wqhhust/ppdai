import requests
from lxml import html
session_request = requests.session()
login_url = "https://ac.ppdai.com/User/Login?redirect="
result = session_request.get(login_url)
tree = html.fromstring(result.text)
print(tree)
