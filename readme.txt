need to install python 3.5
after download it.
run command of:
python utils/bidding.py

For the first execution, it will pop us a firefox window for you to input username, password and verification code,
after you input successfully, it will save the cookies to a dump file. When next time run the command, it will load
the dump file and reload the cookies. Make sure make the cookie persistent: by flip the box of remember the username.

firefox required is version of 31.

packages needs to install:
selenium
lxml
pika
requests
