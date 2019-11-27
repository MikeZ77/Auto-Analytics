import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import dash_table
import plotly.graph_objs as go
import numpy as np
import pandas as pd
import mysql.connector
import requests
import datetime
import textwrap
from colors import color_palett

#TOOLTIPS
def explain_value_ratios(value_ratio):
    print(type(value_ratio))
    if value_ratio == 'P/KM':
        print('lololol')
        return 'Price/Kilometers'
    elif value_ratio == 'P/AGE':
        return "hello"
    elif value_ratio == 'Physical Depretiation':
        return "hello"
    else:
        return "cat"

#FUNCTIONS -----------------------------------------------------------------------------------------------------------------------------------
def get_image(make, model, year):

    query = '{} {} {}'.format(make, model, year)
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
    # return dict((req.json().get('data').get('result').get('items'))[0])['thumbnail']
    return '/home/michael/envs/website/autostats/dashboards/static/not_available.jpeg/'

def compute_price_change(df, today, time_period):
    if today['time_entered'] - time_period > today['time_entered'] - df.loc[0]['time_entered']: return '-'

    #Find the date closest
    t_zero = df[df['time_entered'] >= time_period ].iloc[-1]

    if t_zero['time_entered'] == time_period:
        one_week_delta = (today['price'] - t_zero['price'])/t_zero['price']
    else:
        t_one = df[df['time_entered'] < time_period ].iloc[0]
        #Extrapolate the price
        slope = (t_zero['price'] - t_one['price'])/((t_zero['time_entered'] - t_one['time_entered']).days)
        days = (time_period - t_one['time_entered']).days
        start_price = t_one['price'] + slope*days
        #Compute the price from today
        one_week_delta = (today['price'] - start_price)/start_price

    return str(round(one_week_delta*100,2))+'%'

def get_connection(type, make, model, year):
    conn = mysql.connector.connect(user='root', password='wc3tft',
                                  host='127.0.0.1',
                                  database='autotrader')

    full_vehicle = make+" "+model+" "+year
    full_vehicle = full_vehicle.replace("'",'')

    df_all_years = pd.read_sql("SELECT * FROM main "
    				"LEFT JOIN time USING(adID) "
    				"WHERE body_type="+ type +"AND make="+ make +" AND model="+ model, conn)

    df_table_stats = pd.read_sql("SELECT * FROM total_stats "
                    "WHERE full_vehicle = '"+full_vehicle+"' AND body_type= "+type, conn)

    df_table_total_rank = pd.read_sql("SELECT * FROM stats_rank_all "
                    "WHERE full_vehicle = '"+full_vehicle+"' AND body_type= "+type, conn)

    df_table_total_rank_count = pd.read_sql("SELECT COUNT(*) AS total_count FROM stats_rank_all ",conn)

    df_table_class_rank = pd.read_sql("SELECT * FROM stats_rank_classes "
                    "WHERE full_vehicle = '"+full_vehicle+"' AND body_type= "+type, conn)

    df_class = pd.read_sql("SELECT full_vehicle FROM classes_mapping "
                        "WHERE vehicle_class = ("
                        "SELECT vehicle_class FROM classes_mapping "
                        "WHERE full_vehicle = '"+full_vehicle+"' "
                        "AND vehicle_class LIKE "+type[:-1]+"%')", conn)

    conn.close()
    return df_all_years, df_table_stats, df_table_total_rank, df_table_class_rank, df_class, df_table_total_rank_count

def clean_data(df_all_years):
#Reformat kilometers
    df_all_years['kilometers'] = df_all_years['kilometers'].str.slice_replace(start=-3,repl='')
    df_all_years = df_all_years.drop(df_all_years[df_all_years['kilometers'].map(len) < 4].index)           #drop rows which would have less than 1000KM
    df_all_years['kilometers'] = df_all_years['kilometers'].str.replace(",","")
    df_all_years['kilometers'] = df_all_years['kilometers'].astype(str).astype(int)
#Reformat price
    df_all_years['price'].astype(int)
#Seperate selected year with all years (for AVG Physical Depretiation calc)
    df = df_all_years.drop(df_all_years[df_all_years['year'].astype(int) != int(YEAR.replace("'",""))].index).copy()
#Remove price outliers greater than 1.65 standard deviations (97.5%)
    tolerance = 1.96
    std = df['price'].std()
    avg = df['price'].mean()
    df = df.drop(df[(df['price'] > (avg+tolerance*std)) | (df['price'] < (avg-tolerance*std)) ].index)

    return df, df_all_years


def stats_table(df, df_all_years, df_table_stats, df_table_total_rank, df_table_class_rank, df_class, df_table_total_rank_count):

    now = datetime.datetime.now()
    count_in_class = str(df_class.shape[0])
    count_total = str(df_table_total_rank_count['total_count'].values[0])

    df_table_value = pd.DataFrame({'Value Ratios':['P/KM','P/AGE','Dealer Premium','AVG Physical Depretiation'],
                                        'Mean':[0,0,0,0],'Min':[0,0,0,0],'Max':[0,0,0,0],'SD':[0,0,0,0], 'In Class Rank':[0,0,0,0],'Total Rank':[0,0,0,0]})

    df_table_depr = pd.DataFrame({'Depreciation':['Percent(%)','Dollar($CAD)','In Class Rank','Total Rank'],
                                        '1 Week':['-','-','-','-'],'1 Month':['-','-','-','-'],'3 Months':['-','-','-','-'],
                                        '6 Months':['-','-','-','-'], "1 Year":['-','-','-','-'],'3 Years':['-','-','-','-']})

    # #COMPUTE common statistics
    # avg_price = df['price'].mean()
    # avg_kilometers = df['kilometers'].mean()
    #
    # #COMPUTE Price/Kilometers
    # P_to_KM = avg_price / avg_kilometers
    #
    # #COMPUTE Price/Age
    # age = now.year + (now.day+now.month*30)/365 - int(YEAR[1:-1])
    # P_to_AGE = avg_price/age
    #
    # #COMPUTE Dealer Premium
    # add_type = df[['adType','price']]
    # grouped_add_type = add_type.groupby(['adType'],as_index=False).mean()
    # dealer_price = grouped_add_type['price'][0]
    # private_price = grouped_add_type['price'][1]
    #
    # dealer_premium = str(round((dealer_price/private_price-1)*100,2))+'%'
    #
    # #COMPUTE Average Physical Depretiation
    # #avg KM / old KM
    # df_all_years = df_all_years[['year','kilometers']]
    # df_all_years = df_all_years.groupby(['year'],as_index=False)['kilometers'].agg(['mean','count','std']).reset_index()
    # #Find the oldest model year which has a reasonable number of samples
    # df_all_years = df_all_years[df_all_years['count'] > 25].iloc[0]
    # #Assume the likely endlife of the vehicle is 1.5 SD beyond the average KM of the oldest model
    # oldest_km = df_all_years['mean']*1.5
    # #Average Physical Depretiation
    # avg_pd = avg_kilometers/oldest_km
    #
    # #COMPUTE depreciation table
    # #Find the latest date
    # df = df.groupby(['time_entered'],as_index=False)['price'].mean().sort_values(by=['time_entered'], ascending=False)
    # df['time_entered'] = pd.to_datetime(df['time_entered'])
    # today = df.loc[df['time_entered'].idxmax()]
    # # COMPUTE 1 Week price change
    # one_week_delta = compute_price_change(df, today, today['time_entered'] - pd.DateOffset(days=7))
    # one_month_delta = compute_price_change(df, today, today['time_entered'] - pd.DateOffset(days=30))
    # three_month_delta = compute_price_change(df, today, today['time_entered'] - pd.DateOffset(days=30*3))
    # six_month_delta = compute_price_change(df, today, today['time_entered'] - pd.DateOffset(days=30*6))
    # one_year_delta = compute_price_change(df, today, today['time_entered'] - pd.DateOffset(days=365))
    # three_year_delta = compute_price_change(df, today, today['time_entered'] - pd.DateOffset(days=365*3))

    # ASSIGN precomputed stats
    P_to_KM = float(df_table_stats['p/km'])
    P_to_AGE = float(df_table_stats['p/age'])
    dealer_premium = str(df_table_stats['dealer_premium'])
    avg_pd = str(df_table_stats['avg_physical_depr'])

    print(df_table_stats['p/km'].values[0])
    print(df_table_stats['avg_physical_depr'].values[0])

    df_table_value['Mean'][0] = str(df_table_stats['p/km'].values[0])
    df_table_value['Mean'][1] = str(df_table_stats['p/age'].values[0])
    df_table_value['Mean'][2] = str(df_table_stats['dealer_premium'].values[0])
    df_table_value['Mean'][3] = str(df_table_stats['avg_physical_depr'].values[0])

    df_table_value['In Class Rank'][0] = str(df_table_class_rank['p/km_rank'].values[0]) +' / '+ count_in_class
    df_table_value['In Class Rank'][1] = str(df_table_class_rank['p/age_rank'].values[0]) +' / '+ count_in_class
    df_table_value['In Class Rank'][2] = str(df_table_class_rank['dealer_premium_rank'].values[0]) +' / '+ count_in_class
    df_table_value['In Class Rank'][3] = str(df_table_class_rank['avg_physical_depr_rank'].values[0]) +' / '+ count_in_class

    df_table_value['Total Rank'][0] = "N/A"
    df_table_value['Total Rank'][1] = "N/A"
    df_table_value['Total Rank'][2] = str(df_table_total_rank['dealer_premium_rank'].values[0]) +' / '+ count_total
    df_table_value['Total Rank'][3] = str(df_table_total_rank['avg_physical_depr_rank'].values[0]) +' / '+ count_total

    df_table_depr['1 Week'][0] = str(df_table_stats['1_week_depr'].values[0])
    df_table_depr['1 Month'][0] = str(df_table_stats['1_month_depr'].values[0])
    df_table_depr['3 Months'][0] = str(df_table_stats['3_month_depr'].values[0])
    df_table_depr['6 Months'][0] = str(df_table_stats['6_month_depr'].values[0])
    df_table_depr['1 Year'][0] = str(df_table_stats['1_year_depr'].values[0])
    df_table_depr['3 Years'][0] = str(df_table_stats['3_year_depr'].values[0])

    df_table_depr['1 Week'][2] = str(df_table_class_rank['1_week_depr_rank'].values[0]) +' / '+ count_in_class
    df_table_depr['1 Month'][2] = str(df_table_class_rank['1_month_depr_rank'].values[0]) +' / '+ count_in_class
    df_table_depr['3 Months'][2] = str(df_table_class_rank['3_month_depr_rank'].values[0]) +' / '+ count_in_class
    df_table_depr['6 Months'][2] = str(df_table_class_rank['6_month_depr_rank'].values[0]) +' / '+ count_in_class
    df_table_depr['1 Year'][2] = str(df_table_class_rank['1_year_depr_rank'].values[0]) +' / '+ count_in_class
    df_table_depr['3 Years'][2] = str(df_table_class_rank['3_year_depr_rank'].values[0]) +' / '+ count_in_class

    df_table_depr['1 Week'][3] = str(df_table_total_rank['1_week_depr_rank'].values[0]) +' / '+ count_total
    df_table_depr['1 Month'][3] = str(df_table_total_rank['1_month_depr_rank'].values[0]) +' / '+ count_total
    df_table_depr['3 Months'][3] = str(df_table_total_rank['3_month_depr_rank'].values[0]) +' / '+ count_total
    df_table_depr['6 Months'][3] = str(df_table_total_rank['6_month_depr_rank'].values[0]) +' / '+ count_total
    df_table_depr['1 Year'][3] = str(df_table_total_rank['1_year_depr_rank'].values[0]) +' / '+ count_total
    df_table_depr['3 Years'][3] = str(df_table_total_rank['3_year_depr_rank'].values[0]) +' / '+ count_total
    #mean
    # df_table['Mean'][0] = pd.to_numeric(df['price']).mean()
    # df_table['Mean'][1] = pd.to_numeric(df['kilometers'].str[:-2].str.replace(',','')).mean()
    # #SD
    # df_table['SD'][0] = pd.to_numeric(df['price']).std()
    # df_table['SD'][1] = pd.to_numeric(df['kilometers'].str[:-2].str.replace(',','')).std()
    # #vaiance
    # df_table['Variance'][0] = pd.to_numeric(df['price']).var()
    # df_table['Variance'][1] = pd.to_numeric(df['kilometers'].str[:-2].strRank.replace(',','')).var()
    # #median
    # df_table['Median'][0] = pd.to_numeric(df['price']).median()
    #"'In Class'Rank"'MTotal Rank][1] = pd.to_numeric(df['kilometers'].str[:-2].str.replace(',','')).median()
    # #min
    # df_table['Min'][0] = pd.to_numeric(df['price']).min()
    # df_table['Min'][1] = pd.to_numeric(df['kilometers'].str[:-2].str.replace(',','')).min()
    # #max
    # df_table['SD'][0] = pd.to_numeric(df['price']).max()
    # df_table['Max'][1] = pd.to_numeric(df['kilometers'].str[:-2].str.replace(',','')).max()
    return df_table_value, df_table_depr

#SETUP -----------------------------------------------------------------------------------------------------------------------------------

#TEMP VEHICLE DATA
DASHBOARD = "Value Analytics"
TYPE = "'sedan'"
MAKE = "'Ford'"
MODEL = "'Focus'"
YEAR = "'2013'"

pd.options.mode.chained_assignment = None
#pd.set_option('display.max_rows',2100)
image = get_image(MAKE, MODEL, YEAR)
df_all_years, df_table_stats, df_table_total_rank, df_table_class_rank, df_class, df_table_total_rank_count = get_connection(TYPE, MAKE, MODEL, YEAR)
print(df_all_years)
print(df_table_stats)
print(df_table_total_rank)
print(df_table_class_rank)
print(df_class)
print(df_table_total_rank_count)
df, df_all_years = clean_data(df_all_years)
#get data for table
df_table_value, df_table_depr = stats_table(df, df_all_years,df_table_stats, df_table_total_rank, df_table_class_rank, df_class, df_table_total_rank_count)

#DASH APP -----------------------------------------------------------------------------------------------------------------------------------

app = dash.Dash()
app.layout = html.Div(children=[
    dcc.Input(id='input-1', style={'display': 'none'}),
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
    	dash_table.DataTable(
    		data=df_table_value.to_dict('records'),
    		columns=[{'id': c, 'name': c} for c in df_table_value.columns],
    		style_cell={'textAlign': 'left','font_family': 'arial','backgroundColor':'#f5f5f5'},
            style_cell_conditional=[{'if':{'column_id':c},'width':'131px'} for c in df_table_value.columns],
    		style_header={'backgroundColor':'#ef736b','fontWeight':'bold','font_family': 'arial'},
            style_data_conditional=[{
                "if": {"column_id": "Value Ratios"},
                "backgroundColor": "#ebebeb",
                }],
            tooltip={
                'Value Ratios':[{'type': 'markdown','value': explain_value_ratios(df_table_value.loc[row,'Value Ratios'])} for row in range(len(df_table_value))],
                #'Value Ratios':[{'type': 'markdown','row_index':0,'value': 'Price/Kilometers'}]

                }
            #tooltip={'Value Ratios':{'type': 'markdown','value': "hello"}  }

    	),
        html.Div(
        	dash_table.DataTable(
        		data=df_table_depr.to_dict('records'),
        		columns=[{'id': c, 'name': c} for c in df_table_depr.columns],
        		style_cell={'textAlign': 'left','font_family': 'arial','backgroundColor':'#f5f5f5'},
                style_cell_conditional=[{'if':{'column_id':c},'width':'69px'} for c in df_table_depr.columns],
        		style_header={'backgroundColor':'#ef736b','fontWeight':'bold'},
                style_data_conditional=[{
                    "if": {"column_id": "Depreciation"},
                    "backgroundColor": "#ebebeb",
                    }],
        	),
            style = {
                'position': 'relative',
                'top':'20px'
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
    })



if __name__ == '__main__':
    app.run_server(debug=True)
