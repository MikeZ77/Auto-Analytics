import unittest
import autotrader as a

TEST_LINK = 'https://www.autotrader.ca/cars/?rcp=100&rcs=200&srt=3&prx=100&prv=British%20Columbia&loc=V3J%203S9&hprc=True&wcp=True&sts=New-Used&inMarket=basicSearch'
HEADER = {'user-agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:66.0) Gecko/20100101 Firefox/66.0'}


class TestWebsite(unittest.TestCase):

	@classmethod
	def setUpClass(cls):
		#setup helper functions
		def setupLinks(parse):
			links = list()
			for tag in parse:
				if 'href' in tag.a.attrs: links.append('https://www.autotrader.ca'+ tag.a.attrs['href'])
			return links
		#LINKS PAGE
		#setup connections
		cls._req = a.requests.get(TEST_LINK,headers=HEADER)
		cls._dbConn = a.mysql.connector.connect(user='root', passwd='wc3tft', host='localhost',database='autotrader')
		#setup page objects
		cls._bsObj = a.BeautifulSoup(cls._req.content,'lxml')
		cls._bsParse = cls._bsObj.findAll('div', {'class':'listing-details organic'})
		cls._links = setupLinks(cls._bsParse)
		#ADD PAGE
		#setup connections
		cls._reqAdd = a.requests.get(a.random.choice(cls._links),headers=HEADER)
		#setup page objects
		cls._bsObjAdd = a.BeautifulSoup(cls._reqAdd.content,'lxml')


	def testConnections(self):
		self.assertEqual(str(self.__class__._req),'<Response [200]>')
		self.assertEqual(str(self.__class__._reqAdd),'<Response [200]>')
		self.assertTrue(self.__class__._dbConn. is_connected())

	def testLinks_links(self):
		#print(self.__class__._links)
		#assert number of gathered links is in acceptable range
		self.assertGreater(len(self.__class__._links),95)
		self.assertLess(len(self.__class__._links),101)


	def testLinks_format(self):
		#assert path format is consistant
		for link in self.__class__._links:
			self.assertNotEqual(len(link),0)			#use regex here to check some patterns in the url
			#assert an ID is contained in the string
			#print(a.re.search(r'*/{2}_{8}_/*',link))

	def testLinks_page(self):
		#check that page indexing is available
		page = str(self.__class__._bsObj.findAll('script',limit=24)[15:24])

		self.assertIn('"CurrentPage":',page)
		self.assertIn('"MaxPage":',page)

		current_index = page.rfind('"CurrentPage":')
		max_index = page.rfind('"MaxPage":')

		current_num = page[current_index+15:current_index+18].strip()
		max_num = page[max_index+11:max_index+14]

		if current_num[-1]==',': current_num = current_num[:-1]

		self.assertTrue(current_num.isdigit())
		self.assertTrue(max_num.isdigit())
		self.assertLessEqual(current_num, max_num)
		self.assertLessEqual(max_num.isdigit(), 1000)

	def testAdd_gtmManager(self):
		parse = self.__class__._bsObjAdd.findAll('script',limit=3)[2].text

		self.assertIn('gtmManager.initializeDataLayer',parse)
		self.assertIn('"vehicle":{',parse)
		self.assertIn('"lists":',parse)
		self.assertIn('"city":',parse)

		self.assertLess(parse.index('"vehicle":{'), parse.index('"lists":'))
		self.assertLess(parse.index('"lists":'), parse.index('"city":'))

	def testAdd_vdpSpecsContent(self):
		parse = self.__class__._bsObjAdd.findAll('div',{'id':'vdp-specs-content'})

		self.assertEqual(len(parse),1)

		parse = str(parse[0])

		self.assertIn('KILOMETRES',parse)
		self.assertIn('EXTERIOR COLOUR',parse)
		self.assertIn('FUEL TYPE',parse)

	@classmethod
	def tearDownClass(cls):
		cls._dbConn.close()


if __name__ == '__main__':
	unittest.main()
