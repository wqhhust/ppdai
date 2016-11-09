while [ 1 -eq 1 ]
do
    echo "++++++++++++++++++++++++++++"
    python3 bidding.py &
    sleep 2400
    ps -ef|grep bidding |grep -v grep|awk {'print $2'}|xargs kill -9
    ps -ef|grep firefox|grep -v grep|awk {'print $2'}|xargs kill -9
    echo "++++++++++++++++++++++++++++"
done
