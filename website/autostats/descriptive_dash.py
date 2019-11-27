import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_daq as daq
import dash_table
import plotly.graph_objs as go
import numpy as np
import pandas as pd
import mysql.connector
import requests
from django_plotly_dash import DjangoDash

#DASH APP
app = DjangoDash(name='descriptive')

app.layout = html.Div( children=[

	dcc.Dropdown(id='vehicle_data', value='', style={'display': 'none'}),
	html.Div(id='graph_body')

],
style={
		'width': '100%',
		'display': 'inline-block',
		}
)

@app.callback(
	dash.dependencies.Output('graph_body','children'),
	[dash.dependencies.Input('vehicle_data','value')]
	)
def init_graphs(vehicle_values):
	data = vehicle_values.split()

	DASHBOARD = "Descriptive Analytics"
	MAKE = "'"+data[0]+"'"
	MODEL = "'"+data[1]+"'"
	YEAR = "'"+data[2]+"'"

	conn = mysql.connector.connect(user='root', password='wc3tft',
	                              host='127.0.0.1',
	                              database='autotrader')

	df = pd.read_sql("SELECT * FROM main "
					"LEFT JOIN time USING(adID) "
					"WHERE make="+ MAKE +" AND model="+ MODEL +" AND year="+ YEAR, conn)
	conn.close()

	pd.options.mode.chained_assignment = None

	color_palett = {
		'black':'rgb(0,0,0)',
		'blue':'rgb(0,0,255)',
		'burgundy':'rgb(159,29,53)',
		'charcoal':'rgb(25,24,24)',
		'dark grey':'rgb(105,105,105)',
		'grey':'rgb(128,128,128)',
		'red':'rgb(255,0,0)',
		'silver':'rgb(192,192,192)',
		'white':'rgb(255,255,255)',
		'maroon':'rgb(128,0,0)',
		'yellow':'rgb(255,255,0)',
		'beige':'rgb(245,245,220)',
		'green':'rgb(0,128,0)',
		'brown':'rgb(139,69,19)',
	}

	#QWANT IMAGE API
	query = '{} {} {}'.format(MAKE, MODEL, YEAR)

	try:
		req = requests.get(
			"https://api.qwant.com/api/search/images",
			params={
			    'count': 1,
		        'q': query,
		        't': 'images',
		        'safesearch': 1,
		        'locale': 'en_US',
		        'uiv': 4
				},
			headers={
		        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'
		    	}
			)
	except:
		image = '/static/images/not_available.jpeg'
	else:
		check_response = str(req)
		if check_response != '<Response [429]>':
			image = dict((req.json().get('data').get('result').get('items'))[0])['thumbnail']
		else:
			image = '/static/images/not_available.jpeg'

	#SUBSET OF DATA SET IF REQURED FOR EACH GRAPH OBJECT

	#IDEAS: TOTAL POSTINGS, OPEN/CLOSED POSTINGS, TURNOVER

	#time series
	df_line = pd.DataFrame()

	df_line['date'] = df['time_entered']
	df_line['date'] = df_line['date']
	df_line['price'] = pd.to_numeric(df['price'])

	df_line = df_line.groupby('date', as_index=False).agg({"price": "mean"})

	#histogram
	df_bar = pd.DataFrame()

	df_bar['kilometers'] = pd.to_numeric(df['kilometers'].str[:-2].str.replace(',',''))
	df_bar['kilometers'] = df_bar['kilometers'].round(-4)
	df_bar['count'] = 1
	df_bar = df_bar.groupby('kilometers', as_index=False).agg({"count": "sum"})

	#pie chart
	df_pie = pd.DataFrame()

	df_pie['color'] = df['exterior_color']
	df_pie['count'] = 1
	df_pie = df_pie.groupby('color', as_index=False).agg({"count": "sum"})
	df_pie = df_pie[df_pie['color'] != '-']

	pie_total_colors = df_pie['count'].sum()
	pie_colors = []
	for index,row in df_pie.iterrows():
		#remvoe rare colors so no color dictionary occurs
		if row['count']/pie_total_colors > 0.03:
			pie_colors.append(color_palett[str(row['color'])])

	#bubble chart
	df_bubble = pd.DataFrame()
	df_bubble['kilometers'] = pd.to_numeric(df['kilometers'].str[:-2].str.replace(',',''))
	df_bubble['kilometers'] = df_bubble['kilometers'].round(-4)
	df_bubble['price'] = pd.to_numeric(df['price'])
	df_bubble['count'] = 1

	df_bubble = df_bubble.groupby('kilometers', as_index=False).agg({"price":"mean","count": "sum"})

	#DESCRIPTIVE STATISTICS TABLE
	df_table = pd.DataFrame({'Statistic':['Price(CAD)','Kilometers(KM)'],
							'Mean':[0,0],'SD':[0,0],'Variance':[0,0],
							'Median':[0,0],'Min':[0,0],'Max':[0,0]})

	df_table = df_table[['Statistic','Mean','SD','Variance','Median','Min','Max']]
	#mean
	df_table['Mean'][0] = pd.to_numeric(df['price']).mean()
	df_table['Mean'][1] = pd.to_numeric(df['kilometers'].str[:-2].str.replace(',','')).mean()
	#SD
	df_table['SD'][0] = pd.to_numeric(df['price']).std()
	df_table['SD'][1] = pd.to_numeric(df['kilometers'].str[:-2].str.replace(',','')).std()
	#vaiance
	df_table['Variance'][0] = pd.to_numeric(df['price']).var()
	df_table['Variance'][1] = pd.to_numeric(df['kilometers'].str[:-2].str.replace(',','')).var()
	#median
	df_table['Median'][0] = pd.to_numeric(df['price']).median()
	df_table['Median'][1] = pd.to_numeric(df['kilometers'].str[:-2].str.replace(',','')).median()
	#min
	df_table['Min'][0] = pd.to_numeric(df['price']).min()
	df_table['Min'][1] = pd.to_numeric(df['kilometers'].str[:-2].str.replace(',','')).min()
	#max
	df_table['Max'][0] = pd.to_numeric(df['price']).max()
	df_table['Max'][1] = pd.to_numeric(df['kilometers'].str[:-2].str.replace(',','')).max()

	return [
		html.Div(children=[
			html.H1(
				children=['Dashboard - {}: {} {} {}'.format(DASHBOARD, YEAR.strip("'"),
					MAKE.strip("'"), MODEL.strip("'"))
				],
				style={
					'margin-left':'18%',
					'font-family':'"Helvetica Neue", Helvetica, Arial, sans-serif',
					'color':'#f9f9f9',
					'font-size':'2.5em'
				}
			),
			html.Img(src=image, height='130',width='200',
				style={
					'margin-top':'20px',
					'margin-left':'10px',
					'position': 'absolute',
					'top':'0px',
					'border-style': 'solid',
	  				'border-width': '3px',
	  				'border-color': '#f9f9f9'
				}
			),
			# html.Button('Save',
			# 		id='link_dashboard',
			# 		style = {
			# 			'margin-left':'90%',
			# 			'top':'75px',
			# 			'position': 'absolute',
			# 		}
			# 		),
			daq.StopButton(
    			buttonText='Save Dashboard',
				size=120,
				style = {
					'margin-left':'90%',
					'top':'68px',
					'position': 'absolute',

				}
			),
			],
			style={
				'background-color':'#404040',
				'padding': '30px',
				'padding-left':'0',
				'height':'100px'
			}
		),
		html.Div(children=[
			html.H3('Summary Statistics',
				style={
					'color':'#404040',
					'font-family':'"Helvetica Neue", Helvetica, Arial, sans-serif'
					}),
			dash_table.DataTable(
				data=df_table.to_dict('records'),
				columns=[{'id': c, 'name': c} for c in df_table.columns],
				style_cell={'textAlign': 'left'},
				style_header={'backgroundColor':'#ef736b','fontWeight':'bold'}
			)],
			style={'width': '95%','margin': 'auto'},
		),
		html.Div(
			dcc.Graph(
				id='time-series',
				figure = {
					'data':[
						go.Scatter(
							x=df_line['date'],
							y=df_line['price']
						)
					],
					'layout': go.Layout(
							title = 'Price History',
							xaxis={'title': 'Date'},
							yaxis={'title': 'Price (CAD)'},
							template = 'ggplot2'
						)
				}
			),
			style={'width': '50%','display': 'inline-block'}
		),
		html.Div(
			dcc.Graph(
				id='histogram',
				figure = {
					'data':[
						go.Bar(
								x=df_bar['kilometers'],
								y=df_bar['count']
							)
						],
					'layout': go.Layout(
								title = 'Kilometers',
								xaxis={'title': 'Kilometers'},
								yaxis={'title': 'Vehicles'},
								template = 'ggplot2'
							)
				}
			),
			style={'width': '50%','display': 'inline-block'}
		),
		html.Div(
			dcc.Graph(
				id='pie-chart',
				figure = {
					'data':[
						go.Pie(
							labels=df_pie['color'],
							values=df_pie['count'],
							hole=.3,
							marker_colors=pie_colors
						),
					],
					'layout':
							go.Layout(
								title = 'Exterior Color',
								template = 'ggplot2'
							)
				}
			),
			style={'width': '50%',
			'display': 'inline-block',
			}
		),
		html.Div(
			dcc.Graph(
				id='bubble-chart',
				figure = {
					'data':[
						go.Scatter(
							x=df_bubble['kilometers'],
							y=df_bubble['price'],
							marker_size=df_bubble['count'],
							mode='markers'
						),
					],
					'layout':
							go.Layout(
								title = 'Kilometers to Price',
								xaxis={'title': 'Kilometers'},
								yaxis={'title': 'Price (CAD)'},
								template = 'ggplot2'
							)
				}
			),
			style={'width': '50%',
			'display': 'inline-block',
			}
		),
	]

if __name__=='__main__':
	app.run_server(debug=True)
