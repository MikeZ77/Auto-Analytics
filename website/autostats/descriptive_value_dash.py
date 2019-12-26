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
from . import colors
#IDEAS: TOTAL POSTINGS, OPEN/CLOSED POSTINGS, TURNOVER
#TABLE: total postings, open postings, turnover, p/km correlation
#DESCRIPTIVE PRICE/VOLUME: price time series with volume trace (callback to compare price/class)
#DESCRIPTIVE COLOR: Histogram with color/price
#DESCRIPTIVE Kilometers

#FUNCTIONS -----------------------------------------------------------------------------------------------------------------------
def load_connection(type, make, model, year):
	conn = mysql.connector.connect(user='root', password='wc3tft',
	                              host='127.0.0.1',
	                              database='autotrader')

	df = pd.read_sql("SELECT * FROM main "
					"LEFT JOIN time USING(adID) "
					"WHERE body_type='"+type+"' AND make='"+make+"' AND model='"+model+"' AND year='"+year+"'", conn)
	conn.close()
	return df

def load_time_series(df):
	#time series
	df_line = pd.DataFrame()

	df_line['date'] = df['time_entered']
	df_line['date'] = df_line['date']
	df_line['price'] = pd.to_numeric(df['price'])

	df_line = df_line.groupby('date', as_index=False).agg({"price": "mean"})
	return df_line

def load_histogram(df):
	#histogram
	df_bar = pd.DataFrame()

	df_bar['kilometers'] = pd.to_numeric(df['kilometers'].str[:-2].str.replace(',',''))
	df_bar['kilometers'] = df_bar['kilometers'].round(-4)
	df_bar['count'] = 1
	df_bar = df_bar.groupby('kilometers', as_index=False).agg({"count": "sum"})
	return df_bar

def load_pie_chart(df, color_palett):
	#pie chart
	df_pie = pd.DataFrame()

	df_pie['color'] = df['exterior_color']
	df_pie['count'] = 1
	df_pie = df_pie.groupby('color', as_index=False).agg({"count": "sum"})
	df_pie = df_pie[df_pie['color'] != '-']
	#ONLY use colors that are greater than 5% of the total, use the rest as 'other'
	pie_total_colors = df_pie['count'].sum()
	min = int(0.05 * pie_total_colors)
	df_pie = df_pie[df_pie['count'] >= min].reset_index(drop = True)
	pie_used_colors = df_pie['count'].sum()
	df_other = pd.DataFrame([['other',pie_total_colors-pie_used_colors]],columns=['color','count'])
	df_pie = df_pie.append(df_other, ignore_index=True)

	pie_colors = []
	for index,row in df_pie.iterrows():
		pie_colors.append(color_palett[str(row['color'])])

	return df_pie, pie_colors

def load_bubble_chart(df):
	#bubble chart
	df_bubble = pd.DataFrame()
	df_bubble['kilometers'] = pd.to_numeric(df['kilometers'].str[:-2].str.replace(',',''))
	df_bubble['kilometers'] = df_bubble['kilometers'].round(-4)
	df_bubble['price'] = pd.to_numeric(df['price'])
	df_bubble['count'] = 1

	df_bubble = df_bubble.groupby('kilometers', as_index=False).agg({"price":"mean","count": "sum"})
	return df_bubble

def load_descriptive_table(df):
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

	return df_table

def load_second_descriptive_table(df):
	#SECOND DESCRIPTIVE STATISTICS TABLE
	pass


#DASH APP -----------------------------------------------------------------------------------------------------------------------
TYPE, MAKE, MODEL, YEAR = (None, None, None, None)

app = DjangoDash(name='descriptive_value')

tab_style = {
	'font-family':'"Helvetica Neue", Helvetica, Arial, sans-serif',
	'fontWeight': 'bold',
	'backgroundColor':'#dfdfe5',
	'margin-top':'5px',
}

active_tab_style = {
	'borderTop': '5px solid #ac0404',
	'font-family':'"Helvetica Neue", Helvetica, Arial, sans-serif',
	'fontWeight': 'bold',
}

app.layout = html.Div( children=[

	# dcc.Dropdown(id='vehicle_data', value='', style={'display': 'none'}),
	# html.Div(id='graph_body')
	dcc.Tabs(id="tabs_data", value='', children=[
        dcc.Tab(label='Descriptive', value='descriptive', style=tab_style, selected_style=active_tab_style),
        dcc.Tab(label='Value', value='value', style=tab_style, selected_style=active_tab_style),
    ]),
	html.Div(id='content_body')

],
style={
		'width': '80%',
		'margin': 'auto',
		'padding-top':'20px',
		}
)

@app.callback(
	dash.dependencies.Output('content_body','children'),
	[dash.dependencies.Input('tabs_data','value')]
	)
def init_graphs(vehicle_values):
	#MAIN -----------------------------------------------------------------------------------------------------------------------
	data = vehicle_values.split()
	color_palett = colors.color_palett
	pd.options.mode.chained_assignment = None

	TYPE, MAKE, MODEL, YEAR = (data[0], data[1], data[2], data[3])

	df = load_connection(TYPE, MAKE, MODEL, YEAR)
	df_line = load_time_series(df)
	df_bar = load_histogram(df)
	df_pie, pie_colors = load_pie_chart(df, color_palett)
	df_bubble = load_bubble_chart(df)
	df_table = load_descriptive_table(df)

	return [
		html.Div(children=[
			html.Div(children=[
		    	html.H3('Descriptive Statistics',
		    		style={
		    			'color':'#404040',
		    			'font-family':'"Helvetica Neue", Helvetica, Arial, sans-serif',
		                # 'backgroundColor':'#ebebeb',
		                'background-image': 'linear-gradient(to right, #dfdfe5 , #f8f8ff)',
		                'line-height': '250%',
		                'padding-left':'7px',
		    			}
		        ),
				dash_table.DataTable(
					data=df_table.to_dict('records'),
					columns=[{'id': c, 'name': c} for c in df_table.columns],
					style_cell={'textAlign': 'left','font_family': 'arial','backgroundColor':'#f5f5f5'},
					style_cell_conditional=[{'if':{'column_id':c},'width':'131px'} for c in df_table.columns],
					style_header={'backgroundColor': '#ac0404','fontWeight':'bold','color':'#f9f9f9','font_family': 'arial'},
	                style_data_conditional=[{
	                    "if": {"column_id": "Statistic"},
	                    "backgroundColor": "#dfdfe5",
	                    },
	                ],
				)],
				style={'margin-bottom':'6px'}
			),
			html.Div(
				dcc.Graph(
					id='time-series',
					figure = {
						'data':[
							go.Scatter(
								x=df_line['date'],
								y=df_line['price'],
								marker=dict(color='#ac0404')
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
									y=df_bar['count'],
									marker=dict(color='#ac0404')
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
								marker=dict(color='#ac0404'),
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
		],
	    style={
            # 'width': '80%',
			"backgroundColor": 'white',
            'margin': 'auto',
	        'position': 'relative',
			'padding':'20px',
            # 'top':'40px',
            }
	)]

if __name__=='__main__':
	app.run_server(debug=True)
