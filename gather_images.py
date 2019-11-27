import requests
import mysql.connector
import argparse
import random
import os

def update_vehicle_image_table():
	cursor = conn.cursor()
	#ONLY update table mapping / find images for vehicles which have a reasonable total count
	QUERY = """
			SELECT full_vehicle FROM
			(
			SELECT full_vehicle FROM autotrader.main
			GROUP BY full_vehicle
			HAVING COUNT(full_vehicle) > 30
			) AS vehicle_to_add
			WHERE full_vehicle NOT IN
			(
			SELECT full_vehicle FROM autotrader.vehicle_image
			);
			"""

	cursor.execute(QUERY)
	results = cursor.fetchall()

	for full_vehicle in results:

		QUERY = """
				INSERT INTO `autotrader`.`vehicle_image`
				(`full_vehicle`,
				`image_path`)
				VALUES
				('%s', NULL);
				"""%((full_vehicle[0]))

		try:
			cursor.execute(QUERY)
			conn.commit()
		except:
			conn.rollback()

	cursor.close()

def get_proxies(num, wait):
	os.system('> proxies.txt')
	os.system('timeout '+str(wait)+'s '+'proxybroker find --types HTTPS --lvl High --countries US CA --strict -l '+ str(num) +' > proxies.txt')

	with open('proxies.txt','r') as proxy_file:
		proxy_list = proxy_file.readlines()

	return proxy_list

def parse_proxies(proxy_list, protocol):
	parsed_list = list();
	for proxy in proxy_list:
		proxy.strip()
		index = proxy.find(']')
		parsed_list.append(protocol+'://'+proxy[index+2:len(proxy)-2])
	print(parsed_list)
	return parsed_list

def get_vehicles():

	QUERY = """
			SELECT full_vehicle FROM autotrader.vehicle_image
			WHERE image_path IS NULL;
			"""

	cursor = conn.cursor()
	cursor.execute(QUERY)
	results = cursor.fetchall()
	cursor.close()
	return results

def update_path(path, full_vehicle):
	QUERY = """
			UPDATE vehicle_image SET image_path = '%s'
			WHERE full_vehicle = '%s';
			"""%(path,full_vehicle)

	cursor = conn.cursor()
	cursor.execute(QUERY)
	conn.commit()
	cursor.close()

def get_image(image_url, proxy):
	try:
		img_data = requests.get('http:'+image_url,
								proxies={'https':proxy},
								headers={
									'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'
									}
								).content

	except (requests.exceptions.HTTPError, requests.exceptions.SSLError, requests.exceptions.ProxyError):
		return False
	else:
		return img_data


if __name__ == '__main__':

	conn = mysql.connector.connect(user=os.environ['USER_NAME'], passwd=os.environ['PASSWORD'], host=os.environ['HOST_NAME'],database=os.environ['DATABASE'])
	req = None

	parser = argparse.ArgumentParser()
	parser.add_argument("-filepath","--filepath", type=str, default="/home/michael/envs/website/autostats/dashboards/static/vehicle_images/")
	args = parser.parse_args()
	filepath = args.filepath

	update_vehicle_image_table()
	proxy_list = get_proxies(30,30)
	proxy_list = parse_proxies(proxy_list, 'https')
	vehicle_results = get_vehicles()
	#update_table(filepath,results)

	for year_make_model in vehicle_results:
		print("Searching for "+year_make_model[0])
		if os.path.isfile(filepath+year_make_model[0].replace(' ','_')+'.jpg') == True:
			update_path(filepath+year_make_model[0].replace(' ','_')+'.jpg',year_make_model[0])
			print("Image exists ....")
			continue

		while str(req) != '<Response [200]>':
			proxy = random.choice(proxy_list)
			try:
				req = requests.get(
					"https://api.qwant.com/api/search/images",
					params={
						'count': 1,
						'q': year_make_model[0],
						't': 'images',
						'safesearch': 1,
						'locale': 'en_US',
						'uiv': 4
						},
					proxies={'https':proxy},
					headers={
						'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'
						}
					)
				req.raise_for_status()

			except (requests.exceptions.HTTPError, requests.exceptions.SSLError, requests.exceptions.ProxyError):
				proxy_list.remove(proxy)
				if len(proxy_list) < 5:
					print("Repopulating proxies")
					add_proxy_list = get_proxies(30,30)
					add_proxy_list = parse_proxies(add_proxy_list, 'https')
					proxy_list += add_proxy_list
				continue

			try:
				image_url = dict((req.json().get('data').get('result').get('items'))[0])['thumbnail']
				print(image_url)
			except:
				break

			img_data = False
			while img_data == False:
				img_data = get_image(image_url, proxy)

				if img_data == False:
					proxy_list.remove(proxy)
					if len(proxy_list) < 5:
						print("Repopulating proxies")
						add_proxy_list = get_proxies(30,30)
						add_proxy_list = parse_proxies(add_proxy_list, 'https')
						proxy_list += add_proxy_list
					proxy = random.choice(proxy_list)

			with open(filepath+year_make_model[0].replace(' ','_')+'.jpg','wb') as file:
				file.write(img_data)

			update_path(filepath+year_make_model[0].replace(' ','_')+'.jpg',year_make_model[0])

		req = None

	conn.close()
	print("-----COMPLETED-----")
