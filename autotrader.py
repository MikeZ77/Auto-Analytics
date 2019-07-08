import queue
from bs4 import BeautifulSoup
import mysql.connector
import sys
from threading import Thread, Lock
from datetime import datetime
from proxybroker import Broker
import asyncio
import logging
import warnings
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
		broker.find(types=['HTTPS'], limit=num, countries=['US','CA']),
		show(proxies))

	try: 
		loop = asyncio.get_event_loop()
		loop.run_until_complete(tasks)
	except:
		print("CLOSING ...")
		sys.exit(-1)
	finally:
		print(proxy_list)
		return proxy_list


def parse_proxies(proxy_list, protocol):
	parsed_list = list();
	for proxy in proxy_list:
		index = proxy.find(']')
		parsed_list.append(protocol+'://'+proxy[index+2:len(proxy)-1]) 
	return parsed_list

class crawler(Thread):
	#class variables
	headers = ['Mozilla/5.0 (Windows NT 5.1; rv:7.0.1) Gecko/20100101 Firefox/7.0.1',
		   'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36',
		   'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
		   'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:57.0) Gecko/20100101 Firefox/57.0',
		   'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:51.0) Gecko/20100101 Firefox/51.0',
		   'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
		   'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
		   'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/17.17134',
		   'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0',
		   'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.1 Safari/605.1.15]']

	conn = mysql.connector.connect(user='root', passwd='root', host='localhost',database='sys')
	proxies = None

	def __init__(self, outside_proxy, main_Q, worker_Q):
		Thread.__init__(self)
		#instance variables
		#connection/page objects
		self.req = None
		self.content = None
		self.bsObj = None
		self.path = None
		self.init_proxies(outside_proxy)
		#page structures
		self.bsParse = []
		self.links = []
		self.vehicles = []
		#proxy structures
		self.main_Q = main_Q
		self.worker_Q = worker_Q

	def gather_links(self):

		self.bsParse = self.bsObj.findAll('div', {'class':'listing-details organic'})
		for tag in self.bsParse:
			if 'href' in tag.a.attrs: self.links.append('https://www.autotrader.ca'+ tag.a.attrs['href'])

	def init_proxies(self, outside_proxy):
		self.proxies = outside_proxy

	def update_request(self,link):

		self.req = None
		while self.req is None:
			
			try:
				proxy = random.choice(self.proxies)
				self.req = requests.get(link,headers={'user-agent':random.choice(self.headers)}, proxies={'https':proxy}, timeout=10)
			
			except (requests.exceptions.Timeout, requests.exceptions.ConnectTimeout):
				print("{} connection timeout using ip: {} ... Dropping from proxies ...".format(self.getName(), proxy))
				if proxy in self.proxies: 
					self.proxies.remove(proxy)
			except requests.exceptions.RequestException:
				print("{} other connection issue using ip: {} ... Dropping from proxies ...".format(self.getName(), proxy))
				if proxy in self.proxies: 
					self.proxies.remove(proxy)

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
				cursor.execute(sql_autotrader)
				self.conn.commit()
			except:
				self.conn.rollback()
			try:
				cursor.execute(sql_turnover)
				self.conn.commit()
			except:
				self.conn.rollback()
		
		cursor.close()

	def gather_details(self):

		self.vehicles = []

		#TODO: Include error handling for empty links
		for link in self.links:

			vehicle_details = {'adID':'','condition':'','make':'','model':'','price':'','province':'','rawLocation':'',
					'year':'','kilometres':'','exterior colour':'','fuel type':''}

			self.update_request(link) 
			#collect data from gtmManager.initializeDataLayer				
			self.bsParse = self.bsObj.findAll('script',limit=3)
			try:
				details = re.sub('"','',self.bsParse[2].text)
			except:
				logging.warning("gtmManager.initializeDataLayer method is not available in source for this listing at url: {}".format(link))
				continue

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

			self.vehicles.append(vehicle_details)

		self.links = []
		return self.vehicles

	def run(self):

		last_page = False
		print('starting {} ...'.format(self.getName()))
		self.path = Q.get()
		self.update_request(self.path)
		
		while (last_page==False):

			self.gather_links()
			print('{} gathered link details ...'.format(self.getName()))
			vehicles = self.gather_details()
			print('{} gathered vehicle details ...'.format(self.getName()))

			db_lock.acquire()
			print('{} acquired the lock ...'.format(self.getName()))
			self.update_db(vehicles)
			db_lock.release()
			print('{} released the lock ...'.format(self.getName()))

			#Rotate fresh proxies
			#-----------------------------------------------------------------
			if (len(self.proxies) <= 5) and (proxy_lock.acquire(False)):
				print('{} rotating fresh proxies ...'.format(self.getName()))
				self.main_Q.put(True)
				fresh_proxies = self.worker_Q.get()
				self.proxies += fresh_proxies
				proxy_lock.release()
			#-----------------------------------------------------------------

			self.path = Q.get()
			self.update_request(self.path)

			last_page = self.check_page_index()
			#stop main loop from listening for fresh proxies
			if last_page: self.main_Q.put(False)
			print('{} last page is {} ...'.format(self.getName(),last_page))


if __name__ == '__main__':
	'''
	autotrader.ca search returns a maximum of 1000 indices when 100 postiings per page is set. By breaking the search 
	into price intervals, this allows the search to stay below the 1000 index max.
	'''
	#global variables
	price_range = [(1001,10000),(10001,20000),(20001,30000),(30001,40000),(40001,50000),(50001,60000),
					(60001,70000),(70001,80000),(80001,90000),(90001,100000),(100001,200000),(200001,2000000)]

	max_index = 1000
	num_proxies = 10
	cycled_proxies = 5
	set_threads = 10
	threads = []
	db_lock = Lock()
	proxy_lock = Lock()
	main_Q = queue.Queue()
	worker_Q = queue.Queue()
	warnings.simplefilter("error", UserWarning)
	logging.basicConfig(filename='error.log',level=logging.INFO,format='%(asctime)s:%(threadName)s:%(levelname)s:%(message)s')

	#main loop
	logging.info("STARTING NEW ITERATION")
	for current_range in range(len(price_range)):
		print('-----------------------------------------------------------------------------')
		print('populating queue for range {} ...'.format(price_range[current_range]))
		print('-----------------------------------------------------------------------------')
		Q = get_q(current_range)
		print('retrieving proxies ...')
		print('-----------------------------------------------------------------------------')
	
		proxies = get_proxies(num_proxies)
		proxies = parse_proxies(proxies, 'https')
	
		for i in range(set_threads):
			thread = crawler(proxies,main_Q,worker_Q)
			thread.setName('thread'+str(i))
			threads.append(thread)
			thread.start()

		while True:
		#listen for fresh proxy request from thread
			fresh_proxies = main_Q.get()
			if fresh_proxies:
				fresh_proxies = get_proxies(cycled_proxies)
				fresh_proxies = parse_proxies(fresh_proxies, 'https')
				worker_Q.put(fresh_proxies)
			else:
				break
		
		#block until all threads are joined
		for thread in threads: thread.join()
	
	logging.info("COMPLETED")
	sys.exit(0)



	
	
