import queue
from bs4 import BeautifulSoup
from http.cookiejar import CookieJar
import pymysql
import sys
from threading import Thread
import requests

class crawler(Thread):
	#class variables
	proxy={'http':'http://200.192.255.102:8080', 'https':'http://200.192.255.102:8080'}
	header={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
				AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36'}

	def __init__(self,path):
		Thread.__init__(self)
		#instance variables
		self.req = requests.get(path,headers=self.header)
		self.content = self.req.content
		self.bsObj = BeautifulSoup(self.content,'lxml')
		
	
if __name__ == '__main__':

	Q = queue.Queue(-1)
	
	url = ["https://wwwb.autotrader.ca/cars/?rcp=100&rcs=",
			"&srt=33&pRng=",
			"%2C3",
			"&prx=-1&loc=V3J%203S9&hprc=True&wcp=True&sts=New-Used&inMarket=advancedSearch"]
	
	price_range = [(1001,10000),(10001,20000),(20001,30000),(30001,40000),(40001,50000),(50001,60000),
					(60001,70000),(70001,80000),(80001,90000),(90001),(100000),(100001,200000),(200001,2000000)]
					
	
	#crawler1 = crawler("https://wwwb.autotrader.ca/?gclid=EAIaIQobChMI47vuwbqm4gIVkcVkCh3Uyw2qEAAYASAAEgIEXfD_BwE")
	#print(crawler1.content)
	
	
