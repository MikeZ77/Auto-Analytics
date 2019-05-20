import queue
from bs4 import BeautifulSoup
import pymysql
import sys
from threading import Thread
import requests
import re

class crawler(Thread):
	#class variables
	#proxy={'http':'http://200.192.255.102:8080', 'https':'http://200.192.255.102:8080'}
	header={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
				AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36'}

	def __init__(self,path):
		Thread.__init__(self)
		#instance variables
		#connection 
		self.req = requests.get(path,headers=self.header)
		self.content = self.req.content
		self.bsObj = BeautifulSoup(self.content,'lxml')
		#structures
		self.bsParse = []
		self.links = []
		self.vehicles = []
		
	def gather_links(self):
		
		self.bsParse = self.bsObj.findAll('div', {'class':'listing-details'})
		for tag in self.bsParse:
			if 'href' in tag.a.attrs: self.links.append('https://www.autotrader.ca'+ tag.a.attrs['href'])

	def update_request(self,link):
		print(link)
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

	def gather_details(self):

		vehicle_details = {'adID':'','condition':'','make':'','model':'','price':'','province':'','rawLocation':'',
							'year':'','kilometres':'','exterior colour':'','fuel type':''}

		for link in self.links:

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

			print(vehicle_details)
		
def get_q(rng):
	
		Q = queue.Queue(-1)

		for page in range(0,max_index*100,100):
			
			url = "https://www.autotrader.ca/cars/?rcp=100&rcs={}&srt=33&pRng={}%2C{}&prx=-1&loc=V3J%203S9&hprc=\
				True&wcp=True&sts=New-Used&inMarket=advancedSearch\
				".format(page,price_range[rng][0],price_range[rng][1])
			Q.put(url)

		return Q



if __name__ == '__main__':

	'''
	autotrader.ca search returns a maximum of 1000 indices when 100 postiings per page is set. By breaking the search 
	into price intervals, this allows the search to stay below the 1000 index max.
	'''

	#global variables
	#conn = pymysql.connect(user='root',host='localhost',database='test')
	
	price_range = [(1001,10000),(10001,20000),(20001,30000),(30001,40000),(40001,50000),(50001,60000),
					(60001,70000),(70001,80000),(80001,90000),(90001,100000),(100001,200000),(200001,2000000)]
	
	max_index = 1000
	threads = []
	set_threads = 10	
	
	#main loop
	for current_range in range(len(price_range)):
		Q = get_q(current_range)

		for i in range(set_threads):
			thread = crawler(Q.get())
			threads.append(thread)
			thread.start()



	
		for thread in threads: thread.join()
		

	
	#crawler1 = crawler('https://wwwb.autotrader.ca/cars/bc/coquitlam/?rcp=100&rcs=37100&srt=3&prx=100&prv=British%20Columbia&loc=V3J%203S9&hprc=True&wcp=True&sts=New-Used&inMarket=basicSearch')
	#flag = crawler1.check_page_index()
	#print(flag)
	#crawler1.gather_links()
	#crawler1.gather_details()
	
	
