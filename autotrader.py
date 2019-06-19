import queue
from bs4 import BeautifulSoup
import mysql.connector
import sys
from threading import Thread, Lock
from datetime import datetime
from proxybroker import Broker
import asyncio
import requests
import random
import re

def get_q(rng):
		
		Q = queue.Queue(1000)
		for page in range(0,max_index*100,100):
		
			url = "https://www.autotrader.ca/cars/?rcp=100&rcs={}&srt=33&pRng={}%2C{}&prx=-1&loc=V3J%203S9&hprc=\
				True&wcp=True&sts=New-Used&inMarket=advancedSearch\
				".format(page,price_range[rng][0],price_range[rng][1])
			Q.put(url)
		
		return Q

def get_proxies(num):

	proxy_list = list()
	async def show(proxies):
	    while True:
	        proxy = await proxies.get()
	        if proxy is None: break
	        proxy_list.append(str(proxy))

 	proxies = asyncio.Queue()
	broker = Broker(proxies)
	tasks = asyncio.gather(
	    broker.find(types=['HTTP', 'HTTPS'], limit=num),
	    show(proxies))

	loop = asyncio.get_event_loop()
	loop.run_until_complete(tasks)

	return proxy_list


class crawler(Thread):
	#class variables
	conn = mysql.connector.connect(user='root', passwd='root', host='localhost',database='sys')

	def __init__(self,path):
		Thread.__init__(self)
		#instance variables
		#connection 
		self.req = requests.get(path,headers={random.choice(headers)})
		self.content = self.req.content
		self.bsObj = BeautifulSoup(self.content,'lxml')
		#structures
		self.bsParse = []
		self.links = []
		self.vehicles = []

	def random_ip(self):
		"""
		if "https".upper() in proxy: key = "https"
		if "http".upper() in proxy: key = "http"

		for char in proxy:
			if char.isdigit() == True: 
				ip = proxy[int(char):len(proxy)-1]

		proxy_dict[key] = ip
		"""
	def gather_links(self):
		
		self.bsParse = self.bsObj.findAll('div', {'class':'listing-details'})
		for tag in self.bsParse:
			if 'href' in tag.a.attrs: self.links.append('https://www.autotrader.ca'+ tag.a.attrs['href'])

	def update_request(self,link):
		
		self.req = requests.get(link,headers=self.header)
		self.content = self.req.content
		self.bsObj = BeautifulSoup(self.content,'lxml')
		
	def check_page_index(self):
		#get current page
		self.bsParse = str(self.bsObj.findAll('script',limit=20)[19:20])
		start_index = self.bsParse.rfind('"CurrentPage":')
		current_page = self.bsParse[start_index+15:start_index+18]
		#get max page
		start_index = self.bsParse.rfind('"MaxPage":')
		max_page = self.bsParse[start_index+11:start_index+14]

		if current_page < max_page: 
			return False
		else:
			return True

	def update_db(self, data):
		
		cursor = self.conn.cursor()
		
		for row in data:

			cur_time = datetime.now()
			formated = cur_time.strftime('%Y-%m-%d %H:%M:%S')
			
			values = (row['adID'], row['condition'], row['make'], row['model'], row['price'], row['province'],
			row['rawLocation'], row['year'], row['kilometres'], row['exterior colour'], row['fuel type'])

			sql_autotrader = """INSERT INTO autotrader(adID, `condition`, make, model, price, province, rawLocation, `year`, kilometers, exterior_color, fuel_type) 
							    VALUES('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');"""%(values)
			
			sql_turnover = """INSERT INTO turnover(adID, time_entered, time_updated) 
							  VALUES('%s','%s',%s)
							  ON DUPLICATE KEY UPDATE time_updated = '%s';"""%(row['adID'],formated,'NULL',formated)		
			
			try:
				cursor.execute(sql_turnover)
				self.conn.commit()
				cursor.execute(sql_autotrader)
				self.conn.commit()
			except:
				self.conn.rollback()
		
		cursor.close()

	def gather_details(self):

		self.vehicles = []

		for link in self.links:

			vehicle_details = {'adID':'','condition':'','make':'','model':'','price':'','province':'','rawLocation':'',
					'year':'','kilometres':'','exterior colour':'','fuel type':''}

			self.update_request(link) 
			#collect data from gtmManager.initializeDataLayer				
			self.bsParse = self.bsObj.findAll('script',limit=3)
			details = re.sub('"','',self.bsParse[2].text)
			details = re.split(',|:|{',details)
			details = details[:details.index('pageType')]
							
			#collect remaining data from id="vdp-specs-content"
			self.bsParse = self.bsObj.findAll('div',{'id':'vdp-specs-content'})
			self.bsParse = re.sub('\\n|<th>|</th>|</td>',' ',str(self.bsParse[0]).lower())
			self.bsParse = re.split('<tr>|</tr>|<td>',self.bsParse)
			for item in range(len(self.bsParse)): self.bsParse[item] = self.bsParse[item].strip()

			details = details + self.bsParse
			#add to vehicle_details dict
			for key in vehicle_details:
				index = details.index(key)					#TODO: error handling on .index()
				vehicle_details[key] = details[index+1]
				
			#if self.getName()=='thread0': print(vehicle_details)
			self.vehicles.append(vehicle_details)
			#if self.getName()=='thread0': print('----------------------------------------------------------')
			#if self.getName()=='thread0': print(self.vehicles)

		self.links = []
		return self.vehicles

	def run(self):
		last_page = False

		print('starting {} ...'.format(self.getName()))
		
		while (last_page==False):

			self.gather_links()
			print('{} gathered link details ...'.format(self.getName()))
			vehicles = self.gather_details()
			print('{} gathered vehicle details ...'.format(self.getName()))

			lock.acquire()
			print('{} acquired the lock ...'.format(self.getName()))
			self.update_db(vehicles)
			lock.release()
			print('{} acquired released the lock ...'.format(self.getName()))
			
			self.update_request(Q.get())
			last_page = self.check_page_index()
			print('{} last page is {} ...'.format(self.getName(),last_page))



if __name__ == '__main__':
	'''
	autotrader.ca search returns a maximum of 1000 indices when 100 postiings per page is set. By breaking the search 
	into price intervals, this allows the search to stay below the 1000 index max.
	'''

	#global variables
	price_range = [(1001,10000),(10001,20000),(20001,30000),(30001,40000),(40001,50000),(50001,60000),
					(60001,70000),(70001,80000),(80001,90000),(90001,100000),(100001,200000),(200001,2000000)]

	headers = ['Mozilla/5.0 (Windows NT 5.1; rv:7.0.1) Gecko/20100101 Firefox/7.0.1',
			   'Mozilla/5.0 (Windows NT 5.1; rv:33.0) Gecko/20100101 Firefox/33.0',
			   'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:61.0) Gecko/20100101 Firefox/61.0',
			   'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:57.0) Gecko/20100101 Firefox/57.0',
			   'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:51.0) Gecko/20100101 Firefox/51.0',
			   'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
			   'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
			   'Mozilla/5.0 (iPhone; CPU iPhone OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148',
			   'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/17.17134',
			   'Opera/9.80 (Windows NT 6.1; WOW64) Presto/2.12.388 Version/12.18',
			   'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.1 Safari/605.1.15]']
	
	max_index = 1000
	threads = []
	set_threads = 10
	lock = Lock()
	num_proxies = 20

	proxies = get_proxies(num_proxies)
	print(proxies)
	"""
	#main loop
	for current_range in range(len(price_range)):
		print('-----------------------------------------------------------------------------')
		print('populating queue for range {} ...'.format(price_range[current_range]))
		print('-----------------------------------------------------------------------------')
		Q = get_q(current_range)
		print('-----------------------------------------------------------------------------')
		print('retrieving proxies ...')
		print('-----------------------------------------------------------------------------')
		proxies = get_proxies(num_proxies)

		for i in range(set_threads):
			thread = crawler(Q.get())
			thread.setName('thread'+str(i))
			threads.append(thread)
			thread.start()
		
		for thread in threads: thread.join()
		"""	


	
	
