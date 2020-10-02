import numpy as np
import pandas as pd
import datetime

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
    province_options = list()
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

        trace_map[province] = go.Box(
                                    y = df_current_province['price'].values.tolist(),
                                    name = province,
                                    # jitter = 0.3,
                                    # pointpos = -1.8,
                                    # boxpoints = 'all',
                                    # marker = dict(color='rgb(7,40,89)'),
                                    # line = dict(color='rgb(7,40,89)')
                                )
        traces.append(trace_map[province])
        # GENERATE province options for dropdown on avg price of cities histogram
        province_options.append({'label': province, 'value': province})


    return traces, province_options, df_by_city

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