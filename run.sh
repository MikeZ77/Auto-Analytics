#! /bin/bash

#variables
threads=10
proxy_total=60
proxy_refresh=30
proxy_wait=50
timeout=0

#environment variables
export USER_NAME='root'
export PASSWORD='root'
export HOST_NAME='localhost'
export DATABASE='sys'

#run test script
python3.6 -m unittest -v init_test.py

if [ $? -eq 0 ]
then
	#run main script
	python3.6 autotrader.py -threads $threads -proxy_total $proxy_total -proxy_refresh $proxy_refresh -proxy_wait $proxy_wait -timeout $timeout
fi


