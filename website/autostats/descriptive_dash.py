import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_daq as daq
import dash_table
import plotly.graph_objs as go
import plotly.express as px
import numpy as np
import pandas as pd
import mysql.connector
import requests
from django_plotly_dash import DjangoDash
from . import colors
import datetime

#FUNCTIONS -----------------------------------------------------------------------------------------------------------------------
def load_connection(type, make, model, year):
	conn = mysql.connector.connect(user='root', password='wc3tft',
	                              host='127.0.0.1',
	                              database='autotrader')

	df = pd.read_sql("SELECT * FROM main "
					"LEFT JOIN time USING(adID) "
					"WHERE body_type='"+type+"' AND make='"+make+"' AND model='"+model+"' AND year='"+year+"'", conn)

	df_cutoff = pd.read_sql(
					"SELECT MAX(time_updated) AS max_cutoff FROM autotrader.time; "
					, conn)
	conn.close()
	return df, df_cutoff

def load_time_series(df):
	#time series
	df_line = pd.DataFrame()
	#COMPUTE the average price and volume(count) for each date and the moving average for each date
	df_line['date'] = df['time_entered'].dt.date
	df_line['price'] = pd.to_numeric(df['price'])
	df_line = df_line.groupby('date', as_index=False)['price'].agg({"price": "mean","volume":"count"})
	#SET required columns
	df_line['weight'] = 0
	df_line['cummulative_sum'] = 0
	df_line['moving_average'] = 0
	#COMPUTE cummulative sum
	for index, row in df_line.iterrows():
		rolling_sum = df_line.iloc[:index+1,2].sum()
		df_line.iloc[index,4] = rolling_sum

	for index, row in df_line.iterrows():
		moving_sum = 0

		for prev_index in range(index+1):
			df_line.iloc[prev_index,3] = df_line.iloc[prev_index,2]/df_line.iloc[index,4]
			moving_sum += df_line.iloc[prev_index,1] * df_line.iloc[prev_index,3]

		df_line.iloc[index,5] = moving_sum
		#DEBUG:
		# print(df_line.iloc[:index+1,:])
		
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
	df_pie['price'] = df['price']
	df_pie['count'] = 1
	df_pie = df_pie.groupby(['color'], as_index=False).agg({"count": "sum","price":"mean"})
	df_pie = df_pie[df_pie['color'] != '-']
	
	#ONLY use colors that are greater than 5% of the total, use the rest as 'other'
	pie_total_colors = df_pie['count'].sum()
	min = int(0.05 * pie_total_colors)
	#COMPUTE the acg price for 'other'
	df_pie_other = df_pie[df_pie['count'] < min].reset_index(drop = True)
	other_avg_price = df_pie_other['price'].mean()
	# other_avg_price = df_pie[df_pie['price'] ==]
	df_pie = df_pie[df_pie['count'] >= min].reset_index(drop = True)
	pie_used_colors = df_pie['count'].sum()
	df_other = pd.DataFrame([['other',pie_total_colors-pie_used_colors,other_avg_price]],columns=['color','count','price'])

	df_pie = df_pie.append(df_other, ignore_index=True)
	df_pie['price'] = round(df_pie['price'],2)

	pie_colors = []
	for index,row in df_pie.iterrows():
		pie_colors.append(color_palett[str(row['color'])])

	return df_pie, pie_colors

def load_scatter_plot(df):
	#bubble chart
	df_scatter = pd.DataFrame()
	df_scatter['kilometers'] = pd.to_numeric(df['kilometers'].str[:-2].str.replace(',',''))
	df_scatter['price'] = pd.to_numeric(df['price'])

	df_scatter = df_scatter.groupby('kilometers', as_index=False).agg({"price":"mean"})
	df_scatter = df_scatter.rename(columns={'price':'Price(CAD)'})
	df_scatter = df_scatter.rename(columns={'kilometers':'Kilometers'})
	df_scatter['color'] = 0.5

	return df_scatter

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

def load_details_table(df, df_turnover):
	#DETAILS STATISTICS TABLE
	df_table = pd.DataFrame({'Postings':['Open','Closed'],
							'Total':[0,0],'Oldest':[0,0],'Newest':[0,0],
							'Mean Volume':[0,0],'Mean Turnover (Days)':['N/A',0],
							'Dealer (%)':[0,0]})

	#TURNOVER: CASE 1: time_updated = NaT => IF (today - time_entered) is greater than the mean turnover, then update  
	#				   time_updated with the mean turnover
	#		   CASE 2: time_updated < (the cutoff) => This is computed turnover
	#		   CASE 3: time_updated > (the cutoff) => This is a (still) open add (not turnover)

	#FIND the turnover cutoff date
	#TAKE the MAX date, subtract two days (assumes the script will take at most three days to run)
	turnover_cutoff = df_turnover['max_cutoff'].values[0] - pd.Timedelta('3 days')
	df_turnover = df[['time_entered','time_updated','adType']]

	#STORE the NaT values
	df_nat = df_turnover.copy()
	df_nat = df_nat[df_nat['time_updated'].isna()].reset_index(drop=True)
	#COMPUTE turnover (closed postings)
	df_turnover = df_turnover[~df_turnover['time_updated'].isna()]
	df_turnover = df_turnover[df_turnover['time_updated'] < turnover_cutoff].reset_index(drop=True)
	df_turnover['turnover'] = (df_turnover['time_updated'] - df_turnover['time_entered']).astype('timedelta64[D]')
	turnover = df_turnover['turnover'].mean()
	#CHECK NaT values
	df_nat = df_nat[df_nat['time_entered'] < pd.to_datetime('today') - pd.Timedelta(str(turnover)+' days')].reset_index(drop=True)
	df_nat['turnover'] = (df_nat['time_updated'] - df_nat['time_entered']).astype('timedelta64[D]')
	#ADD back NaT values whoch are less than cutoff and recompute turnover
	df_turnover = df_turnover.append(df_nat, ignore_index=True)
	turnover = round(df_turnover['turnover'].mean(),2)
	
	#COMPUTE total closed postings
	total_closed = df_turnover['time_entered'].count()

	#COMPUTE closed oldest and newest
	oldest_closed = df_turnover['time_entered'].dt.date.min()
	newest_closed = df_turnover['time_entered'].dt.date.max()

	#COMPUTE total open postings
	#CALC df - df_turnover
	df_open = pd.concat([df_turnover[['time_entered','time_updated','adType']],df[['time_entered','time_updated','adType']]]).drop_duplicates(keep=False).reset_index(drop=True)
	total_open = df_open['time_entered'].count()

	#COMPUTE open oldest
	oldest_open = df_open['time_entered'].dt.date.min()
	newest_open = df_open['time_entered'].dt.date.max()
	
	#VOLUME => On a given day how many postings are created?
	#OPEN volume => average number of positings created on each run which are still open
	#CLOSED volume => average number of postings created on each run which are now closed
	#COMPUTE open volume
	df_volume_open = df_open.copy()
	df_volume_open['time_entered'] = df_volume_open['time_entered'].dt.date
	df_volume_open = df_volume_open.groupby('time_entered', as_index=False).count()
	df_volume_open = df_volume_open.rename(columns={'time_updated':'volume'})
	volume_open = str(round(df_volume_open['volume'].mean(),2))

	#COMPUTE closed volume
	df_volume_closed = df_turnover.copy()
	df_volume_closed = df_volume_closed.groupby('time_entered', as_index=False).count()
	df_volume_closed = df_volume_closed.drop(columns=['turnover'])
	df_volume_closed = df_volume_closed.rename(columns={'time_updated':'volume'})
	volume_closed = str(round(df_volume_closed['volume'].mean(),2))

	#COMPUTE Dealer(%) open
	df_dealer_open = df_open.copy()
	df_dealer_open = df_dealer_open[~df_dealer_open['adType'].isna()].reset_index(drop=True)
	df_dealer_open = df_dealer_open.groupby('adType', as_index=False).count()
	df_dealer_open = df_dealer_open.drop(columns=['time_updated'])
	df_dealer_open = df_dealer_open.rename(columns={'time_entered':'add_type_count'})
	dealer_open = str(round(df_dealer_open.iloc[0,1] / df_dealer_open['add_type_count'].sum(),2))

	#COMPUTE Dealer(%) closed
	df_dealer_closed = df_turnover.copy()
	df_dealer_closed = df_dealer_closed[~df_dealer_closed['adType'].isna()].reset_index(drop=True)
	df_dealer_closed = df_dealer_closed.groupby('adType', as_index=False).count()
	df_dealer_closed = df_dealer_closed.drop(columns=['time_updated','turnover'])
	df_dealer_closed = df_dealer_closed.rename(columns={'time_entered':'add_type_count'})
	dealer_closed = str(round(df_dealer_closed.iloc[0,1] / df_dealer_closed['add_type_count'].sum(),2))

	#POPULATE details table
	#total
	df_table['Total'][0] = total_open
	df_table['Total'][1] = total_closed
	#odlest
	df_table['Oldest'][0] = oldest_open
	df_table['Oldest'][1] = oldest_closed
	#newest
	df_table['Newest'][0] = newest_open
	df_table['Newest'][1] = newest_closed
	#volume
	df_table['Mean Volume'][0] = volume_open
	df_table['Mean Volume'][1] = volume_closed
	#turnover
	df_table['Mean Turnover (Days)'][1] = turnover
	#dealer
	df_table['Dealer (%)'][0] = dealer_open
	df_table['Dealer (%)'][1] = dealer_closed

	return df_table

#DASH APP -----------------------------------------------------------------------------------------------------------------------
app = DjangoDash(name='descriptive')

app.layout = html.Div( children=[
	dcc.Loading(
	    id="loading-1",
	    	children=[
			dcc.Dropdown(id='vehicle_data', value='', style={'display': 'none'}),
			html.Div(id='graph_body'),
			],
		type='circle',
		color='#ac0404',
		style={'margin-top':'19%'}
	)
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
	#MAIN -----------------------------------------------------------------------------------------------------------------------
	data = vehicle_values.split('$')
	color_palett = colors.color_palett
	pd.options.mode.chained_assignment = None

	DASHBOARD = "Descriptive Analytics"
	TYPE, MAKE, MODEL, YEAR = (data[0], data[1], data[2], data[3])

	df, df_cutoff = load_connection(TYPE, MAKE, MODEL, YEAR)
	df_line = load_time_series(df)
	df_bar = load_histogram(df)
	df_pie, pie_colors = load_pie_chart(df, color_palett)
	df_scatter = load_scatter_plot(df)
	df_table = load_descriptive_table(df)
	df_details_table = load_details_table(df, df_cutoff)

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
					style_header={'background-image': 'linear-gradient(#850101, #a60101 , #850101)','fontWeight':'bold','color':'#f9f9f9','font_family': 'arial'},
	                style_data_conditional=[{
	                    "if": {"column_id": "Statistic"},
	                    "backgroundColor": "#dfdfe5",
	                    },
	                ],
				)],
				style={'margin-bottom':'15px'}
			),
	    	html.H3('Posting Details',
	    		style={
	    			'color':'#404040',
	    			'font-family':'"Helvetica Neue", Helvetica, Arial, sans-serif',
	                'background-image': 'linear-gradient(to right, #dfdfe5 , #f8f8ff)',
	                'line-height': '250%',
	                'padding-left':'7px',
	    			}
	        ),
			html.Div(children=[
				dash_table.DataTable(
					data=df_details_table.to_dict('records'),
					columns=[{'id': c, 'name': c} for c in df_details_table.columns],
					style_cell={'textAlign': 'left','font_family': 'arial','backgroundColor':'#f5f5f5'},
					style_cell_conditional=[{'if':{'column_id':c},'width':'131px'} for c in df_details_table.columns],
					style_header={'background-image': 'linear-gradient(#850101, #a60101 , #850101)','fontWeight':'bold','color':'#f9f9f9','font_family': 'arial'},
	                style_data_conditional=[{
	                    "if": {"column_id": "Postings"},
	                    "backgroundColor": "#dfdfe5",
	                    },
	                ],
				)],
				style={'margin':'15px 0 50px 0'}

			),
			html.H3('Price History',
	    		style={
	    			'color':'#404040',
	    			'font-family':'"Helvetica Neue", Helvetica, Arial, sans-serif',
	                'background-image': 'linear-gradient(to right, #dfdfe5 , #f8f8ff)',
	                'line-height': '250%',
	                'padding-left':'7px',
	    			}
	        ),
			html.Div(
				dcc.Graph(
					id='time-series',
					figure = {
						'data':[
							go.Scatter(
								name='Average Price',
								x=df_line['date'],
								y=df_line['price'],
								marker=dict(color='#ac0404'),
								line={'shape': 'spline', 'smoothing': 0.3, 'width':4},
							),
							go.Scatter(
								name='Moving Average',
								x=df_line['date'],
								y=df_line['moving_average'],
								marker=dict(color='#ac0404'),
								line={'dash':'dash','shape': 'spline', 'smoothing': 0.7,'width':4},
							),
							go.Scatter(
								name='Volume',
								x=df_line['date'],
								y=df_line['volume'],
								marker=dict(color='#00CDCD'),
								yaxis='y2',
								fill='tozeroy',
								line={'shape': 'spline', 'smoothing': 0.5},
							),
						],
						'layout': go.Layout(
								title = {'text':'Price Volume Timeseries','y':0.99},
								xaxis={'title': 'Date'},
								yaxis={'title': 'Price (CAD)'},
								yaxis2={'title': 'Volume (Number of Postings)',
												'overlaying':'y',
												'side':'right',
												'range':[0,int(df_line['volume'].max()*4.2)]},
								margin={'t':30},
								template = 'ggplot2'
							)
					},
					style={'margin-top':'6px'}
				),
			),
			html.H3('Kilometers',
	    		style={
	    			'color':'#404040',
	    			'font-family':'"Helvetica Neue", Helvetica, Arial, sans-serif',
	                'background-image': 'linear-gradient(to right, #dfdfe5 , #f8f8ff)',
	                'line-height': '250%',
	                'padding-left':'7px',
	    			}
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
									title = 'Kilometers Histogram',
									xaxis={'title': 'Kilometers'},
									yaxis={'title': 'Number of Vehicles'},
									template = 'ggplot2',
									margin={'t':30},
								)
					}
				),
				style={'width': '50%','display': 'inline-block'}
			),
			html.Div(
				dcc.Graph(
					id='scatter-chart',
					figure=px.scatter(df_scatter,
									x='Kilometers',
									y='Price(CAD)',
									color='color',
									trendline="ols",
									template='ggplot2',
									title='Kilometers to Price (corr='+str(round(df_scatter[['Kilometers','Price(CAD)']].corr()['Price(CAD)'][0],2))+')',
									height=450,
									).update(layout={"margin": {"t": 30}}).update_traces(marker={'color':'#ac0404'})

				),
				style={'width': '50%',
				'display': 'inline-block',
				}
			),
			html.H3('Colors',
	    		style={
	    			'color':'#404040',
	    			'font-family':'"Helvetica Neue", Helvetica, Arial, sans-serif',
	                'background-image': 'linear-gradient(to right, #dfdfe5 , #f8f8ff)',
	                'line-height': '250%',
	                'padding-left':'7px',
	    			}
	        ),
			html.Div(
				dcc.Graph(
					id='pie-color',
					figure = {
						'data':[
							go.Pie(
								labels=df_pie['color'],
								values=df_pie['count'],
								hole=.3,
								marker_colors=pie_colors,
								marker=dict(line=dict(color='#F5F5F5', width=2)),
							),
						],
						'layout':
								go.Layout(
									title = 'Exterior Color(%)',
									template = 'ggplot2',
									margin={'t':30},
								)
					}
				),
				style={'width': '50%',
				'display': 'inline-block',
				}
			),
			html.Div(
				dcc.Graph(
					id='pie-average-price',
					figure = {
						'data':[
							go.Pie(
								labels=df_pie['color'],
								values=df_pie['price'],
								hole=.3,
								marker_colors=pie_colors,
								hoverinfo='label+percent',
								textinfo='value',
								marker=dict(line=dict(color='#F5F5F5', width=2)),
							),
						],
						'layout':
								go.Layout(
									title = 'Exterior Color AVG Price(CAD)',
									template = 'ggplot2',
									margin={'t':30},
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
            # 'margin': 'auto',
	        'position': 'relative',
			'padding':'20px',
            'top':'15px',
            'border-style': 'solid',
			'border-width': '2px',
			'border-color': '#c22b2d',
			'border-radius': '6px',
            }
	)]

if __name__=='__main__':
	app.run_server(debug=True)
