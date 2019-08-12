TO RUN:

# Python > 3.6

ARGUMENTS:

(OPTIONAL) threads = the number of threads (OPTIONAL, DEFAULT=10)
(OPTIONAL) proxy_total = total number of proxies to start with (DEFAULT=50)
(OPTIONAL) proxy_refresh = the number of proxies to rotate per price interval (DEFAULT=10)
(OPTIONAL) proxy_wait = how long to wait for proxies before canceling and retrying the request (DEFAULT=30 (seconds))
(OPTIONAL) timeout = set a timeout before making any request (DEFAULT=0 (ms))

ENVIRONMENT VARIABLES:

#DATABSE PROPERTIES

USER=''
HOST=''
PASSWORD=''

RUN COMMAND:
python3.6 autotrader.py -threads -proxy_total -proxy_refresh -proxy_wait -timeout



