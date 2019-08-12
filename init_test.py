import pytest
import autotrader as a

def test_connection():
	
	TEST_LINK = 'https://www.autotrader.ca/cars/?rcp=100&rcs=200&srt=3&prx=100&prv=British%20Columbia&loc=V3J%203S9&hprc=True&wcp=True&sts=New-Used&inMarket=basicSearch'	
	HEADER = {'user-agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:66.0) Gecko/20100101 Firefox/66.0'}
	
	req = a.requests.get(TEST_LINK,headers=HEADER)

	assert str(requested_page) == '<Response [200]>'

def test_db_connection():
	#check that env variables are set and can connect
	True==True

def test_links():
	#listing-details organic
	bsObj = a.BeautifulSoup(req.content,'lxml')
	page_links = bsObj.findAll('div', {'class':'listing-details organic'})

	links = list()
	for tag in page_links:
		assert 'href' in tag.a.attrs
		if 'href' in tag.a.attrs:
			assert len(tag.a.attrs) != 0

def test_initializeDataLayer():
	pass

def test_vdp_specs_content():
	pass

def test_vehicle_details():
	pass

def test_page_index():
	pass

