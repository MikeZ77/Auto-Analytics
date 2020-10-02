import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import dash_table
import plotly.graph_objs as go
import numpy as np
import pandas as pd
import mysql.connector
import datetime
import json
from dataframes import DataFrame

#FUNCTIONS -----------------------------------------------------------------------------------------------------------------------------------
def load_stats_table(df_obj):
    #GET required paramaters
    partition = df_obj.get_current_stats_partition()
    class_list = df_obj.get_class_list()
    #GET required dataframes
    df, df_all_years = df_obj.get_years()
    df_table_stats = df_obj.get_table_stats(partition)
    df_table_total_rank = df_obj.get_total_rank()
    df_table_class_rank = df_obj.get_total_class_rank()
    class_list = df_obj.get_class_list()
    df_class_value_ratios = df_obj.get_class_value_ratios(partition, class_list)
    df_class_depreciation =  df_obj.get_class_depreciation(partition, class_list)
    df_table_total_rank_count =  df_obj.get_total_rank_count()
    df_table_stats_descriptive =  df_obj.get_table_stats_descriptive()
    df_table_stats_depr = df_obj.get_table_stats_depreciation()

    now = datetime.datetime.now()
    #GET counts of vehicles in class for each category
    count_p_km = str(df_class_value_ratios[df_class_value_ratios['p/km'] != 0.00].shape[0])
    count_p_age = str(df_class_value_ratios[df_class_value_ratios['p/age'] != 0.00].shape[0])
    count_dealer_premium = str(df_class_value_ratios[df_class_value_ratios['dealer_premium'] != '-'].shape[0])
    count_avg_physical_depr = str(df_class_value_ratios[df_class_value_ratios['avg_physical_depr'] != 0.00].shape[0])

    count_one_week = str(df_class_depreciation[df_class_depreciation['1_week_depr']!= '-'].shape[0])
    count_one_month = str(df_class_depreciation[df_class_depreciation['1_month_depr']!= '-'].shape[0])
    count_three_month = str(df_class_depreciation[df_class_depreciation['3_month_depr']!= '-'].shape[0])
    count_six_month = str(df_class_depreciation[df_class_depreciation['6_month_depr']!= '-'].shape[0])
    count_one_year = str(df_class_depreciation[df_class_depreciation['1_year_depr']!= '-'].shape[0])
    count_three_year = str(df_class_depreciation[df_class_depreciation['3_year_depr']!= '-'].shape[0])
    #df_class_value_ratios, df_class_depreciation = clean_data(clean_class_ratios=df_class_value_ratios, clean_class_depr=df_class_depreciation)
    count_total = str(df_table_total_rank_count['total_count'].values[0])

    df_table_value = pd.DataFrame({'Value Ratios':['P/KM','P/AGE','Dealer Premium','AVG Physical Depretiation'],
                                        'Mean':[0,0,0,0],'Min':[0,0,0,0],'Max':[0,0,0,0],'SD':[0,0,0,0], 'In Class Rank':[0,0,0,0],'Total Rank':[0,0,0,0]})

    df_table_depr = pd.DataFrame({'Depreciation':['Percent(%)','Dollar($CAD)','In Class Rank','Total Rank'],
                                        '1 Week':['-','-','-','-'],'1 Month':['-','-','-','-'],'3 Months':['-','-','-','-'],
                                        '6 Months':['-','-','-','-'], "1 Year":['-','-','-','-'],'3 Years':['-','-','-','-']})
    # ASSIGN precomputed stats
    #BY column
    df_table_value['Mean'][0] = str(df_table_stats['p/km'].values[0])
    df_table_value['Mean'][1] = str(df_table_stats['p/age'].values[0])
    df_table_value['Mean'][2] = str(df_table_stats['dealer_premium'].values[0])
    df_table_value['Mean'][3] = str(df_table_stats['avg_physical_depr'].values[0])

    df_table_value['Min'][0] = str(df_table_stats_descriptive['min_p_km'].values[0])
    df_table_value['Min'][1] = str(df_table_stats_descriptive['min_p_age'].values[0])
    df_table_value['Min'][2] = str(df_table_stats_descriptive['min_dealer'].values[0])
    df_table_value['Min'][3] = str(df_table_stats_descriptive['min_phys_depr'].values[0])

    df_table_value['Max'][0] = str(df_table_stats_descriptive['max_p_km'].values[0])
    df_table_value['Max'][1] = str(df_table_stats_descriptive['max_p_age'].values[0])
    df_table_value['Max'][2] = str(df_table_stats_descriptive['max_dealer'].values[0])
    df_table_value['Max'][3] = str(df_table_stats_descriptive['max_phys_depr'].values[0])

    df_table_value['SD'][0] = str(df_table_stats_descriptive['sd_p_km'].values[0])
    df_table_value['SD'][1] = str(df_table_stats_descriptive['sd_p_age'].values[0])
    df_table_value['SD'][2] = str(df_table_stats_descriptive['sd_dealer'].values[0])
    df_table_value['SD'][3] = str(df_table_stats_descriptive['sd_phys_depr'].values[0])

    df_table_value['In Class Rank'][0] = str(df_table_class_rank['p/km_rank'].values[0]) +' / '+ count_p_km
    df_table_value['In Class Rank'][1] = str(df_table_class_rank['p/age_rank'].values[0]) +' / '+ count_p_age
    df_table_value['In Class Rank'][2] = str(df_table_class_rank['dealer_premium_rank'].values[0]) +' / '+ count_dealer_premium
    df_table_value['In Class Rank'][3] = str(df_table_class_rank['avg_physical_depr_rank'].values[0]) +' / '+ count_avg_physical_depr

    df_table_value['Total Rank'][0] = "N/A"
    df_table_value['Total Rank'][1] = "N/A"
    df_table_value['Total Rank'][2] = str(df_table_total_rank['dealer_premium_rank'].values[0]) +' / '+ count_total
    df_table_value['Total Rank'][3] = str(df_table_total_rank['avg_physical_depr_rank'].values[0]) +' / '+ count_total
    #BY row
    df_table_depr['1 Week'][0] = str(df_table_stats['1_week_depr'].values[0])
    df_table_depr['1 Month'][0] = str(df_table_stats['1_month_depr'].values[0])
    df_table_depr['3 Months'][0] = str(df_table_stats['3_month_depr'].values[0])
    df_table_depr['6 Months'][0] = str(df_table_stats['6_month_depr'].values[0])
    df_table_depr['1 Year'][0] = str(df_table_stats['1_year_depr'].values[0])
    df_table_depr['3 Years'][0] = str(df_table_stats['3_year_depr'].values[0])

    df_table_depr['1 Week'][1] = str(df_table_stats_depr['1_week'].values[0])
    df_table_depr['1 Month'][1] = str(df_table_stats_depr['1_month'].values[0])
    df_table_depr['3 Months'][1] = str(df_table_stats_depr['3_month'].values[0])
    df_table_depr['6 Months'][1] = str(df_table_stats_depr['6_month'].values[0])
    df_table_depr['1 Year'][1] = str(df_table_stats_depr['1_year'].values[0])
    df_table_depr['3 Years'][1] = str(df_table_stats_depr['3_year'].values[0])

    df_table_depr['1 Week'][2] = str(df_table_class_rank['1_week_depr_rank'].values[0]) +' / '+ count_one_week
    df_table_depr['1 Month'][2] = str(df_table_class_rank['1_month_depr_rank'].values[0]) +' / '+ count_one_month
    df_table_depr['3 Months'][2] = str(df_table_class_rank['3_month_depr_rank'].values[0]) +' / '+ count_three_month

    df_table_depr['1 Week'][3] = str(df_table_total_rank['1_week_depr_rank'].values[0]) +' / '+ count_total
    df_table_depr['1 Month'][3] = str(df_table_total_rank['1_month_depr_rank'].values[0]) +' / '+ count_total
    df_table_depr['3 Months'][3] = str(df_table_total_rank['3_month_depr_rank'].values[0]) +' / '+ count_total
    #STATISTICS have been collected for a minimum 3 months
    if df_table_stats['6_month_depr'].values[0] == '-':
        (df_table_depr['6 Months'][2], df_table_depr['1 Year'][2], df_table_depr['3 Years'][2]) = ("N/A","N/A","N/A")
        (df_table_depr['6 Months'][3], df_table_depr['1 Year'][3], df_table_depr['3 Years'][3]) = ("N/A","N/A","N/A")

    elif df_table_stats['1_year_depr'].values[0] == '-':
        df_table_depr['6 Months'][2] = str(df_table_class_rank['6_month_depr_rank'].values[0]) +' / '+ count_six_month
        df_table_depr['6 Months'][3] = str(df_table_total_rank['6_month_depr_rank'].values[0]) +' / '+ count_total
        (df_table_depr['1 Year'][2], df_table_depr['3 Years'][2]) = ("N/A","N/A")
        (df_table_depr['1 Year'][3], df_table_depr['3 Years'][3]) = ("N/A","N/A")

    elif df_table_stats['3_year_depr'].values[0] == '-':
        df_table_depr['6 Months'][2] = str(df_table_class_rank['6_month_depr_rank'].values[0]) +' / '+ count_six_month
        df_table_depr['1 Year'][2] = str(df_table_class_rank['1_year_depr_rank'].values[0]) +' / '+ count_one_year
        df_table_depr['6 Months'][3] = str(df_table_total_rank['6_month_depr_rank'].values[0]) +' / '+ count_total
        df_table_depr['1 Year'][3] = str(df_table_total_rank['1_year_depr_rank'].values[0]) +' / '+ count_total
        df_table_depr['3 Years'][2] = "N/A"
        df_table_depr['3 Years'][3] = "N/A"
    else:
        df_table_depr['6 Months'][2] = str(df_table_class_rank['6_month_depr_rank'].values[0]) +' / '+ count_six_month
        df_table_depr['1 Year'][2] = str(df_table_class_rank['1_year_depr_rank'].values[0]) +' / '+ count_one_year
        df_table_depr['3 Years'][2] = str(df_table_class_rank['3_year_depr_rank'].values[0]) +' / '+ count_three_year
        df_table_depr['6 Months'][3] = str(df_table_total_rank['6_month_depr_rank'].values[0]) +' / '+ count_total
        df_table_depr['1 Year'][3] = str(df_table_total_rank['1_year_depr_rank'].values[0]) +' / '+ count_total
        df_table_depr['3 Years'][3] = str(df_table_total_rank['3_year_depr_rank'].values[0]) +' / '+ count_total

    return df_table_value, df_table_depr

def load_sub_table_value_ratios(df_obj):

    df_class_value_ratios = df_obj.df_class_value_ratios
    df_class_value_ratios = df_obj.clean_dealer_premium(df_class_value_ratios)

    df_hist_p_km = df_class_value_ratios[['full_vehicle','p/km']].sort_values('p/km',ascending=False)
    df_hist_p_age = df_class_value_ratios[['full_vehicle','p/age']].sort_values('p/age',ascending=False)

    df_hist_p_dealer_premium = df_class_value_ratios[['full_vehicle','dealer_premium']].sort_values('dealer_premium',ascending=False)
    df_hist_p_dealer_premium = df_hist_p_dealer_premium[df_hist_p_dealer_premium['dealer_premium'].isna() == False].reset_index(drop = True)

    df_hist_avg_physical_depr = df_class_value_ratios[['full_vehicle','avg_physical_depr']].sort_values('avg_physical_depr',ascending=False)
    df_hist_avg_physical_depr = df_hist_avg_physical_depr[df_hist_avg_physical_depr['avg_physical_depr'] != 0.00].reset_index(drop = True)

    return df_hist_p_km, df_hist_p_age, df_hist_p_dealer_premium, df_hist_avg_physical_depr

def load_sub_table_depretiation(df_obj):

    df_class_depreciation = df_obj.df_class_depreciation
    df_class_depreciation = df_obj.clean_depreciation(df_class_depreciation)

    df_hist_1_week_depr = df_class_depreciation[['full_vehicle','1_week_depr']].sort_values('1_week_depr',ascending=False)
    df_hist_1_week_depr = df_hist_1_week_depr[df_hist_1_week_depr['1_week_depr'].isna() == False].reset_index(drop = True)

    df_hist_1_month_depr = df_class_depreciation[['full_vehicle','1_month_depr']].sort_values('1_month_depr',ascending=False)
    df_hist_1_month_depr = df_hist_1_month_depr[df_hist_1_month_depr['1_month_depr'].isna() == False].reset_index(drop = True)

    df_hist_3_month_depr = df_class_depreciation[['full_vehicle','3_month_depr']].sort_values('3_month_depr',ascending=False)
    df_hist_3_month_depr = df_hist_3_month_depr[df_hist_3_month_depr['3_month_depr'].isna() == False].reset_index(drop = True)

    df_hist_6_month_depr = df_class_depreciation[['full_vehicle','6_month_depr']].sort_values('6_month_depr',ascending=False)
    df_hist_6_month_depr = df_hist_6_month_depr[df_hist_6_month_depr['6_month_depr'].isna() == False].reset_index(drop = True)

    df_hist_1_year_depr = df_class_depreciation[['full_vehicle','1_year_depr']].sort_values('1_year_depr',ascending=False)
    df_hist_1_year_depr = df_hist_1_year_depr[df_hist_1_year_depr['1_year_depr'].isna() == False].reset_index(drop = True)

    df_hist_3_year_depr = df_class_depreciation[['full_vehicle','3_year_depr']].sort_values('3_year_depr',ascending=False)
    df_hist_3_year_depr = df_hist_3_year_depr[df_hist_3_year_depr['3_year_depr'].isna() == False].reset_index(drop = True)

    return df_hist_1_week_depr, df_hist_1_month_depr, df_hist_3_month_depr, df_hist_6_month_depr, df_hist_1_year_depr, df_hist_3_year_depr

def load_value_by_location(df_obj):
    df_current_year = df_obj.df_year
    # MAP variable province/traces to go.Box objects
    traces = list()
    trace_map = {
            'ON':None, 'AB':None, 'BC':None, 'QC':None, 'MB':None, 'SK':None,
            'NS':None, 'NL':None, 'YT':None, 'NB':None, 'PE':None, 'NT':None, 'NU':None
            }
    #SETUP data for boxplot
    df_by_province = df_current_year[['province','price']].copy()
    df_by_city = df_current_year[['province','city','price']].copy()
    provinces = df_by_province['province'].unique()
    count_by_province = df_by_province.groupby('province').agg(count=("price", "count"))

    for province in provinces:
        if count_by_province['count'].loc[province] <= 10:
            df_by_province = df_by_province.drop(df_by_province[df_by_province['province'] == province].index)

    provinces = df_by_province['province'].unique()
    #CREATE traces for each province
    for province in provinces:
        df_current_province = df_by_province.loc[df_by_province['province'] == province]
        trace_map[province] = df_current_province['price'].values.tolist()
        trace = {'name':province, 'y':trace_map[province]}
        # GENERATE province options for dropdown on avg price of cities histogram
        traces.append(trace)

    return traces, df_by_city

def load_boxplot_traces(df_boxplot):
	traces = list()
	trace = dict()
	province_options = list()
	#PARSE to dash objects, province = df_boxplot.iloc[row,0]
	for row in range(df_boxplot.shape[0]):

		trace[df_boxplot.iloc[row,0]] = go.Box(
			y = df_boxplot.iloc[row,1],
		    name = df_boxplot.iloc[row,0],
		)
		traces.append(trace[df_boxplot.iloc[row,0]])
		province_options.append({'label': df_boxplot.iloc[row,0], 'value': df_boxplot.iloc[row,0]})

	return traces, province_options

def load_city_histogram(province, df_by_city):
    #IF there are more than 10 cities, group by the largest 10 cities
    df_update_province = df_by_city[df_by_city.province == province].copy()
    df_update_province = df_update_province.groupby('city')['price'].agg(mean_price='mean',count='count').reset_index()
    df_update_province = df_update_province.sort_values('count',ascending=False).reset_index()
    #n/a is an option for city selection so remove it
    df_update_province = df_update_province.drop(df_update_province[df_update_province['city'] == 'n/a'].index)

    rows = df_update_province.shape[0]
    if rows > 10:
        df_update_province = df_update_province[['city','mean_price']].head(10)
    else:
        df_update_province = df_update_province[['city','mean_price']].head(rows)

    df_update_province = df_update_province.sort_values('mean_price',ascending=False)
    return{
    'data':[
    	go.Bar(
    			x=df_update_province['city'],
    			y=df_update_province['mean_price'],
                marker=dict(color='#8a0303')
    		),
    	],
    'layout': go.Layout(
    			title = {'text':'<b>Average Price - Cities in '+ province+'<b>','y':0.95,'x':0.5},
                margin = dict(t=50),
    			xaxis={'title': 'City'},
    			yaxis={'title': 'Price(CAD)'},
    			template = 'ggplot2'
    		),
     }

def buying_power(df_obj):
    #GET only the adID's that have adjusted prices
    df_current_year = df_obj.df_year
    df_buying_power = df_current_year[['adID','price','adjusted_price','time_entered']]
    df_buying_power = df_buying_power[df_buying_power['adjusted_price'].isnull() == False].reset_index(drop = True)
    #COMPUTE probability of drop in price
    max_price = int(df_buying_power['price'].max()) + 1
    min_price = int(df_buying_power['price'].min()) - 1
    #DIVIDE into 8 buckets
    bucket_size = (max_price - min_price)/8
    buckets = [min_price+(bucket_size*i) for i in range(9)]
    #COMPUTE price and counts
    df_buying_power['price_delta'] = df_buying_power['price'] - df_buying_power['adjusted_price']
    df_group_total_count = df_buying_power.groupby(pd.cut(df_buying_power['price'], buckets))['price'].agg(total_count='count').reset_index()
    df_buying_power = df_buying_power[df_buying_power['price_delta'] != 0].reset_index(drop = True)
    df_group_count = df_buying_power.groupby(pd.cut(df_buying_power['price'], buckets))['price'].agg(count='count').reset_index()
    df_group_price = df_buying_power.groupby(pd.cut(df_buying_power['price'], buckets))['price_delta'].agg(price_delta='mean').reset_index()
    df_group_price['price_delta'] = df_group_price['price_delta'].fillna(0)
    df_buckets = df_group_price['price']
    #CONCAT and compute weighted average
    df_group = pd.concat([df_buckets, df_group_price['price_delta'], df_group_count['count'], df_group_total_count['total_count']], axis=1)
    df_group['probability'] = df_group['count'] / df_group['total_count']
    df_group['weighted_avg'] = df_group['price_delta'] * df_group['probability']
    df_group = df_group[['price','weighted_avg']]
    #GET mid of each interval since graph objects to not seem yo support interval[float64] 
    df_group['price'] = df_group['price'].apply(lambda interval: interval.mid)

    return df_group

#DASH APP -----------------------------------------------------------------------------------------------------------------------------------

app = dash.Dash()
app.config['suppress_callback_exceptions'] = True

app.layout = html.Div([
		dcc.Dropdown(id='vehicle_details', style={'display': 'none'}, value="sedan Ford Focus 2013"),
		html.Div(id='dataframes', style={'display': 'none'}),
		html.Div(id='dashboard')

])

@app.callback(Output('dataframes', 'children'), [Input('vehicle_details', 'value')])
def load_data(vehicle_details):
	#SETUP -----------------------------------------------------------------------------------------------------------------------------------
	pd.options.mode.chained_assignment = None
	vehicle_details = vehicle_details.split()
	TYPE, MAKE, MODEL, YEAR = vehicle_details[0], vehicle_details[1], vehicle_details[2], vehicle_details[3]

	df_obj = DataFrame(TYPE,MAKE,MODEL,YEAR)

	df_table_value, df_table_depr = load_stats_table(df_obj)
	df_hist_p_km, df_hist_p_age, df_hist_p_dealer_premium, df_hist_avg_physical_depr = load_sub_table_value_ratios(df_obj)
	df_hist_1_week_depr, df_hist_1_month_depr, df_hist_3_month_depr, df_hist_6_month_depr, df_hist_1_year_depr, df_hist_3_year_depr = load_sub_table_depretiation(df_obj)
	traces, df_by_city = load_value_by_location(df_obj)
	df_buying_power = buying_power(df_obj)
	df_obj.close_connection()

	datasets = {
		'table_value': df_table_value.to_json(orient='split', date_format='iso'),
		'table_depr': df_table_depr.to_json(orient='split', date_format='iso'),
		'hist_p_km': df_hist_p_km.to_json(orient='split', date_format='iso'),
		'hist_p_age': df_hist_p_age.to_json(orient='split', date_format='iso'),
		'hist_p_dealer_premium': df_hist_p_dealer_premium.to_json(orient='split', date_format='iso'),
		'hist_avg_physical_depr': df_hist_avg_physical_depr.to_json(orient='split', date_format='iso'),
		'hist_1_week_depr': df_hist_1_week_depr.to_json(orient='split', date_format='iso'),
		'hist_1_month_depr': df_hist_1_month_depr.to_json(orient='split', date_format='iso'),
		'hist_3_month_depr': df_hist_3_month_depr.to_json(orient='split', date_format='iso'),
		'hist_6_month_depr': df_hist_6_month_depr.to_json(orient='split', date_format='iso'),
		'hist_1_year_depr': df_hist_1_year_depr.to_json(orient='split', date_format='iso'),
		'hist_3_year_depr': df_hist_3_year_depr.to_json(orient='split', date_format='iso'),
		'traces': json.dumps(traces),
		'by_city': df_by_city.to_json(orient='split', date_format='iso'),
		'buying_power': df_buying_power.to_json(orient='split', date_format='iso'),
	}

	return json.dumps(datasets) 

@app.callback(
Output('dashboard', 'children'),[Input('dataframes', 'children')])
def load_value_dashboard(df_jsonified):
	
	datasets = json.loads(df_jsonified)

	df_table_value = pd.read_json(datasets['table_value'], orient='split')
	df_table_depr = pd.read_json(datasets['table_depr'], orient='split')
	df_hist_p_km = pd.read_json(datasets['hist_p_km'], orient='split')
	df_hist_p_age = pd.read_json(datasets['hist_p_age'], orient='split')
	df_hist_p_dealer_premium = pd.read_json(datasets['hist_p_dealer_premium'], orient='split')
	df_hist_avg_physical_depr = pd.read_json(datasets['hist_avg_physical_depr'], orient='split')
	df_hist_1_week_depr = pd.read_json(datasets['hist_1_week_depr'], orient='split')
	df_hist_1_month_depr = pd.read_json(datasets['hist_1_month_depr'], orient='split')
	df_hist_3_month_depr = pd.read_json(datasets['hist_3_month_depr'], orient='split')
	df_hist_6_month_depr = pd.read_json(datasets['hist_6_month_depr'], orient='split')
	df_hist_1_year_depr = pd.read_json(datasets['hist_1_year_depr'], orient='split')
	df_hist_3_year_depr = pd.read_json(datasets['hist_3_year_depr'], orient='split')
	df_buying_power = pd.read_json(datasets['buying_power'], orient='split')
	df_boxplot_traces = pd.read_json(datasets['traces'])
	df_boxplot_by_city = pd.read_json(datasets['by_city'], orient='split')

	#The number of box's in boxplot is dynamic
	traces, province_options = load_boxplot_traces(df_boxplot_traces)
	# DASH formatting variables
	button_style = {
	    'background-image': 'linear-gradient(#282828, #404040 , #282828)',
	    'text-align': 'center',
	    'color':'#f8f8ff',
	    'padding':'3px 0 3px 0',
	    'font-family':'"Helvetica Neue", Helvetica, Arial, sans-serif',
	    'border-radius': '8px',
	    'width':'100%',
	    'font-weight': 'bold',
    }

	return [
		html.Div(children=[
			html.Div(children=[
		    	html.H3('Value Statistics',
		    		style={
		    			'color':'#404040',
		    			'font-family':'"Helvetica Neue", Helvetica, Arial, sans-serif',
		                # 'backgroundColor':'#ebebeb',
		                'background-image': 'linear-gradient(to right, #dfdfe5 , #f8f8ff)',
		                'line-height': '250%',
		                'padding-left':'7px',
		    			}
		        ),
		        html.Div(
		        	dash_table.DataTable(
		        		data=df_table_value.to_dict('records'),
		        		columns=[{'id': c, 'name': c} for c in df_table_value.columns],
		        		style_cell={'textAlign': 'left','font_family': 'arial','backgroundColor':'#f5f5f5'},
		                style_cell_conditional=[{'if':{'column_id':c},'width':'131px'} for c in df_table_value.columns],
		        		style_header={'background-image': 'linear-gradient(#850101, #a60101 , #850101)','fontWeight':'bold','color':'#f9f9f9','font_family': 'arial'},
		                style_data_conditional=[{
		                    "if": {"column_id": "Value Ratios"},
		                    "backgroundColor": "#dfdfe5",
		                    },
		                ]),
		                style={'margin-bottom':'6px'}
		        ),
		        html.Div(children=[
		            html.Button(
		                id='button_price_km',
		                value='Hide',
		                children=[
		                'Price/Kilometers',
		                # html.Img(src='plus.png')
		                ],
		                style=button_style
		            ),
		            html.Div(
		                dcc.Graph(
		                    id='histogram p/km',
		        			figure = {
		        				'data':[
		        					go.Bar(
		        							x=df_hist_p_km['full_vehicle'],
		        							y=df_hist_p_km['p/km'],
		                                    marker=dict(color='#8a0303')
		            					),
		        					],
		        				'layout': go.Layout(
		        							title = 'In Class P/KM',
		        							xaxis={'title': 'Vehicle'},
		        							yaxis={'title': 'P/KM'},
		        							template = 'ggplot2'
		        						),
		        			}
		                ),
		            id='container p/km',
		            style={'width': '100%'}),

		        ]),
		        html.Div(children=[
		            html.Button(
		                id='button_price_age',
		                value='Hide',
		                children=[
		                'Price/Age',
		                # html.Img(src='plus.png')
		                ],
		                style=button_style
		            ),
		            html.Div(
		                dcc.Graph(
		                    id='histogram p/age',
		    				figure = {
		    					'data':[
		    						go.Bar(
		    								x=df_hist_p_age['full_vehicle'],
		    								y=df_hist_p_age['p/age'],
		                                    marker=dict(color='#8a0303')
		            					),
		    						],
		    					'layout': go.Layout(
		    								title = 'In Class P/AGE',
		    								xaxis={'title': 'Vehicle'},
		    								yaxis={'title': 'P/AGE'},
		    								template = 'ggplot2'
		    							),
		    				}
		                ),
		            id='container p/age',
		            style={'width': '100%'}),

		        ]),
		        html.Div(children=[
		            html.Button(
		                id='button_dealer_premium',
		                value='Hide',
		                children=[
		                'Dealer Premium',
		                # html.Img(src='plus.png')
		                ],
		                style=button_style
		            ),
		            html.Div(
		                dcc.Graph(
		                    id='histogram dealer_premium',
		    				figure = {
		    					'data':[
		    						go.Bar(
		    								x=df_hist_p_dealer_premium['full_vehicle'],
		    								y=df_hist_p_dealer_premium['dealer_premium'],
		                                    marker=dict(color='#8a0303')
		            					),
		    						],
		    					'layout': go.Layout(
		    								title = 'In Class Dealer Premium',
		    								xaxis={'title': 'Vehicle'},
		    								yaxis={'title': 'Dealer Premium(%)'},
		    								template = 'ggplot2'
		    							),
		    				}
		        	),
		            id='container dealer_premium',
		            style={'width': '100%'}),

		        ]),
		        html.Div(children=[
		            html.Button(
		                id='button_avg_physical_depr',
		                value='Hide',
		                children=[
		                'Average Physical Depreciation',
		                # html.Img(src='plus.png')
		                ],
		                style=button_style
		            ),
		            html.Div(
		                dcc.Graph(
		                    id='histogram avg_physical_depr',
		    				figure = {
		    					'data':[
		    						go.Bar(
		    								x=df_hist_avg_physical_depr['full_vehicle'],
		    								y=df_hist_avg_physical_depr['avg_physical_depr'],
		                                    marker=dict(color='#8a0303')
		            					),
		    						],
		    					'layout': go.Layout(
		    								title = 'In Class Average Physical Depreciation',
		    								xaxis={'title': 'Vehicle'},
		    								yaxis={'title': 'Physical Depretiation'},
		    								template = 'ggplot2'
		    							),
		    				}
		        	),
		            id='container avg_physical_depr',
		            style={'width': '100%'}),

		        ]),

		        html.Div(
		        	dash_table.DataTable(
		                id='table1',
		        		data=df_table_depr.to_dict('records'),
		        		columns=[{'id': c, 'name': c} for c in df_table_depr.columns],
		        		style_cell={'textAlign': 'left','font_family': 'arial','backgroundColor':'#f5f5f5'},
		                style_cell_conditional=[{'if':{'column_id':c},'width':'69px'} for c in df_table_depr.columns],
		        		style_header={'background-image': 'linear-gradient(#850101, #a60101 , #850101)','fontWeight':'bold','color':'#f9f9f9','font_family': 'arial'},
		                style_data_conditional=[{
		                    "if": {"column_id": "Depreciation"},
		                    "backgroundColor": "#dfdfe5",
		                    }],
		        	),
		            style = {
		                'position': 'relative',
		                'top':'20px',
		                'margin-bottom':'25px'
		            }
		        ),
		        html.Div(children=[
		            html.Button(
		                id='button_1_week_depr',
		                value='Hide',
		                children=[
		                '1 Week Price Depreciation',
		                # html.Img(src='plus.png')
		                ],
		                style=button_style
		            ),
		            html.Div(
		                dcc.Graph(
		                    id='histogram 1_week_depr',
		        			figure = {
		        				'data':[
		        					go.Bar(
		        							x=df_hist_1_week_depr['full_vehicle'],
		        							y=df_hist_1_week_depr['1_week_depr'],
		                                    marker=dict(color='#8a0303')
		            					),
		        					],
		        				'layout': go.Layout(
		        							title = 'One Week Price Depreciation',
		        							xaxis={'title': 'Vehicle'},
		        							yaxis={'title': 'Depreciation(%)'},
		        							template = 'ggplot2'
		        						),
		        			}
		                ),
		            id='container 1_week_depr',
		            style={'width': '100%'}),
		        ],
		        id='outside container 1_week_depr',
		        ),

		        html.Div(children=[
		            html.Button(
		                id='button_1_month_depr',
		                value='Hide',
		                children=[
		                '1 Month Price Depreciation',
		                #html.Img(src='plus.png')
		                ],
		                style=button_style
		            ),
		            html.Div(
		                dcc.Graph(
		                    id='histogram 1_month_depr',
		        			figure = {
		        				'data':[
		        					go.Bar(
		        							x=df_hist_1_month_depr['full_vehicle'],
		        							y=df_hist_1_month_depr['1_month_depr'],
		                                    marker=dict(color='#8a0303')
		            					),
		        					],
		        				'layout': go.Layout(
		        							title = 'One Month Price Depreciation',
		        							xaxis={'title': 'Vehicle'},
		        							yaxis={'title': 'Depreciation(%)'},
		        							template = 'ggplot2'
		        						),
		        			}
		                ),
		            id='container 1_month_depr',
		            style={'width': '100%'}),

		        ]),
		        html.Div(children=[
		            html.Button(
		                id='button_3_month_depr',
		                value='Hide',
		                children=[
		                '3 Month Price Depreciation',
		                # html.Img(src='plus.png')
		                ],
		                style=button_style
		            ),
		            html.Div(
		                dcc.Graph(
		                    id='histogram 3_month_depr',
		        			figure = {
		        				'data':[
		        					go.Bar(
		        							x=df_hist_3_month_depr['full_vehicle'],
		        							y=df_hist_3_month_depr['3_month_depr'],
		                                    marker=dict(color='#8a0303')
		            					),
		        					],
		        				'layout': go.Layout(
		        							title = 'Three Month Price Depreciation',
		        							xaxis={'title': 'Vehicle'},
		        							yaxis={'title': 'Depreciation(%)'},
		        							template = 'ggplot2'
		        						),
		        			}
		                ),
		            id='container 3_month_depr',
		            style={'width': '100%'}),

		        ]),
		        html.Div(children=[
		            html.Button(
		                id='button_6_month_depr',
		                value='Hide',
		                children=[
		                '6 Month Price Depreciation',
		                # html.Img(src='plus.png')
		                ],
		                style=button_style
		            ),
		            html.Div(
		                dcc.Graph(
		                    id='histogram 6_month_depr',
		        			figure = {
		        				'data':[
		        					go.Bar(
		        							x=df_hist_6_month_depr['full_vehicle'],
		        							y=df_hist_6_month_depr['6_month_depr'],
		                                    marker=dict(color='#8a0303')
		            					),
		        					],
		        				'layout': go.Layout(
		        							title = 'Six Month Price Depreciation',
		        							xaxis={'title': 'Vehicle'},
		        							yaxis={'title': 'Depreciation(%)'},
		        							template = 'ggplot2'
		        						),
		        			}
		                ),
		            id='container 6_month_depr',
		            style={'width': '100%'}),

		        ]),
		        html.Div(children=[
		            html.Button(
		                id='button_1_year_depr',
		                value='Hide',
		                children=[
		                '1 Year Price Depreciation',
		                # html.Img(src='plus.png')
		                ],
		                style=button_style
		            ),
		            html.Div(
		                dcc.Graph(
		                    id='histogram 1_year_depr',
		        			figure = {
		        				'data':[
		        					go.Bar(
		        							x=df_hist_1_year_depr['full_vehicle'],
		        							y=df_hist_1_year_depr['1_year_depr'],
		                                    marker=dict(color='#8a0303')
		            					),
		        					],
		        				'layout': go.Layout(
		        							title = 'One Year Price Depreciation',
		        							xaxis={'title': 'Vehicle'},
		        							yaxis={'title': 'Depreciation(%)'},
		        							template = 'ggplot2'
		        						),
		        			}
		                ),
		            id='container 1_year_depr',
		            style={'width': '100%'}),

		        ]),
		        html.Div(children=[
		            html.Button(
		                id='button_3_year_depr',
		                value='Hide',
		                children=[
		                '3 Year Price Depreciation',
		                # html.Img(src='plus.png')
		                ],
		                style=button_style
		            ),
		            html.Div(
		                dcc.Graph(
		                    id='histogram 3_year_depr',
		        			figure = {
		        				'data':[
		        					go.Bar(
		        							x=df_hist_3_year_depr['full_vehicle'],
		        							y=df_hist_3_year_depr['3_year_depr'],
		                                    marker=dict(color='#8a0303')
		            					),
		        					],
		        				'layout': go.Layout(
		        							title = 'Three Year Price Depreciation',
		        							xaxis={'title': 'Vehicle'},
		        							yaxis={'title': 'Depreciation(%)'},
		        							template = 'ggplot2'
		        						),
		        			}
		                ),
		            id='container 3_year_depr',
		            style={'width': '100%'}),

		        ]),
		    	html.H3('Buying Power',
		    		style={
		    			'color':'#404040',
		    			'font-family':'"Helvetica Neue", Helvetica, Arial, sans-serif',
		                # 'backgroundColor':'#ebebeb',
		                'background-image': 'linear-gradient(to right, #dfdfe5 , #f8f8ff)',
		                'line-height': '250%',
		                'padding-left':'7px',
		    			}
		        ),
		        dcc.Graph(
		            id='buying_power_plot',
		            figure={
		                'data':[
		                    go.Scatter(x=df_buying_power['price'],
		                        y=df_buying_power['weighted_avg'],
		                        fill='tozeroy',
		                        line_color='#cc0000')
		                ],
		                'layout': go.Layout(
		                            # title = {'text':'Buying Power','y':0.80,'x':0.5},
		                            xaxis={'title': 'Vehicle Price ($CAD)'},
		                            yaxis={'title': 'Weighted AVG Price Decrease ($CAD)'},
		                            template = 'ggplot2',
		                            margin={'t': 0, 'b':5}
		                        ), 
		            },
		            # style={
		            # 'padding-top':'-40px'
		            # }
		        ),
		    	html.H3('Value by Location',
		    		style={
		    			'color':'#404040',
		    			'font-family':'"Helvetica Neue", Helvetica, Arial, sans-serif',
		                # 'backgroundColor':'#ebebeb',
		                'background-image': 'linear-gradient(to right, #dfdfe5 , #f8f8ff)',
		                'line-height': '250%',
		                'padding-left':'7px',
		    			}
		        ),
		        dcc.Graph(
		            figure={
		            'data': traces,
					'layout': go.Layout(
								title = {'text':'<b>Boxplot Prices by Province</b>','y':0.95,'x':0.5},
		                        margin = dict(t=50),
								xaxis={'title': 'Provinces'},
								yaxis={'title': 'Price(CAD)'},
								template = 'ggplot2'
							),
		            },
		            id='boxplot_province',
					style={'width': '50%',
					'display': 'inline-block',
		            'margin-top':'20px'
					}
		        ),
		        html.Div(
		        children=[
		            dcc.Dropdown(
		                id='provinces_dropdown',
		                options=province_options,
		                value=province_options[0]['label']
		            ),
		            dcc.Graph(id='histogram cities')
		        ],
				style={'width': '50%',
				'display': 'inline-block',
				}
		        ),
		        ],
			    style={
		            'width': '80%',
		            # 'width': '101.7%',
		            'margin': 'auto',
		            'position': 'relative',
		            'top':'40px',
		            },
		        ),],

		    style={
		    	'background-color':'#404040',
		    	'padding': '30px',
		    	'padding-left':'0',
		    	'height':'100px'
		    }
		)
	]

# CALLBACKS ---------------------------------------------------------------------------------------------
# TOGGLE histograms for statistics tables ---------------------------------------------------------------
def toggle_check(toggle_value, no_stats):
    if no_stats:
        return {'display': 'none'}
    else:
        if toggle_value == None:
            return {'display': 'none'}
        else:
            if toggle_value % 2 == 0:
                return {'display': 'none'}
            else:
                return {'display': 'block'}

# Values Statistics
@app.callback(Output('container p/km', 'style'), [Input('button_price_km', 'n_clicks')])
def toggle_histogram_p_km(toggle_value):
    return toggle_check(toggle_value, False)

@app.callback(Output('container p/age', 'style'), [Input('button_price_age', 'n_clicks')])
def toggle_histogram_p_age(toggle_value):
    return toggle_check(toggle_value, False)

@app.callback(Output('container dealer_premium', 'style'), [Input('button_dealer_premium', 'n_clicks')])
def toggle_histogram_dealer_premium(toggle_value):
    return toggle_check(toggle_value, False)

@app.callback(Output('container avg_physical_depr', 'style'), [Input('button_avg_physical_depr', 'n_clicks')])
def toggle_histogram_avg_physical_depr(toggle_value):
    return toggle_check(toggle_value, False)

# Depretiation Statistics
@app.callback(Output('container 1_week_depr', 'style'), 
	[Input('button_1_week_depr', 'n_clicks'),
	Input('dataframes', 'children')])
def toggle_histogram_1_week_depr(toggle_value, df_jsonified):
	json_hist = json.loads(df_jsonified)
	return toggle_check(toggle_value, pd.read_json(json_hist['hist_1_week_depr'], orient='split').empty)

@app.callback(Output('container 1_month_depr', 'style'), 
	[Input('button_1_month_depr', 'n_clicks'),
	Input('dataframes', 'children')])
def toggle_histogram_1_month_depr(toggle_value, df_jsonified):
	json_hist = json.loads(df_jsonified)
	return toggle_check(toggle_value, pd.read_json(json_hist['hist_1_month_depr'], orient='split').empty)

@app.callback(Output('container 3_month_depr', 'style'), 
	[Input('button_3_month_depr', 'n_clicks'),
	Input('dataframes', 'children')])
def toggle_histogram_3_month_depr(toggle_value, df_jsonified):
	json_hist = json.loads(df_jsonified)
	return toggle_check(toggle_value, pd.read_json(json_hist['hist_3_month_depr'], orient='split').empty)

@app.callback(Output('container 6_month_depr', 'style'), 
	[Input('button_6_month_depr', 'n_clicks'),
	Input('dataframes', 'children')])
def toggle_histogram_6_month_depr(toggle_value, df_jsonified):
	json_hist = json.loads(df_jsonified)
	return toggle_check(toggle_value, pd.read_json(json_hist['hist_6_month_depr'], orient='split').empty)

@app.callback(Output('container 1_year_depr', 'style'), 
	[Input('button_1_year_depr', 'n_clicks'),
	Input('dataframes', 'children')])
def toggle_histogram_6_month_depr(toggle_value, df_jsonified):
	json_hist = json.loads(df_jsonified)
	return toggle_check(toggle_value, pd.read_json(json_hist['hist_1_year_depr'], orient='split').empty)

@app.callback(Output('container 3_year_depr', 'style'), 
	[Input('button_3_year_depr', 'n_clicks'),
	Input('dataframes', 'children')])
def toggle_histogram_6_month_depr(toggle_value, df_jsonified):
	json_hist = json.loads(df_jsonified)
	return toggle_check(toggle_value, pd.read_json(json_hist['hist_3_year_depr'], orient='split').empty)

# DROPDOWN on avg price cities histogram ---------------------------------------------------------------
@app.callback(Output('histogram cities', 'figure'), 
	[Input('provinces_dropdown', 'value'),
	Input('dataframes', 'children')])
def change_city_histogram(province, df_jsonified):

	datasets = json.loads(df_jsonified)
	df_by_city = pd.read_json(datasets['by_city'], orient='split')
	return load_city_histogram(province, df_by_city)

if __name__ == '__main__':
    app.run_server(debug=True)


