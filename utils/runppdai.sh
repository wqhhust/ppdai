while [ 1 -eq 1 ]
do
    ps -ef|grep bidding |grep -v grep|awk {'print $2'}|xargs kill -9
    ps -ef|grep firefox|grep -v grep|awk {'print $2'}|xargs kill -9
    echo "start ppdai python ......"
    cd /data/ppdai/utils
    python3 bidding.py &
    cd /home/oracle/clojure/lein/page_scan_controller
    lein controller &
    sleep 2400
    ps -ef|grep bidding |grep -v grep|awk {'print $2'}|xargs kill -9
    ps -ef|grep firefox|grep -v grep|awk {'print $2'}|xargs kill -9
    echo "killed ppdai python...."
    sleep 30
done
