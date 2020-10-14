import numpy as np
import pandas as pd
import mysql.connector
from tqdm import tqdm
import math
import datetime
import os

conn = mysql.connector.connect(user=os.environ['USER_NAME'], password=os.environ['PASSWORD'],
                              host=host=os.environ['HOST_NAME'],
                              database=os.environ['DATABASE'])


def get_distinct_vehicles_and_types():

    df_distinct_vehicles = pd.read_sql(
                    "SELECT DISTINCT make, model, year "
                    "FROM main "
                    "WHERE model != 'Unspecified' ", conn
    )
    #CONVERT to python list
    list_distinct_vehicles = df_distinct_vehicles.values.tolist()
    #ARBITRARY must be at least 1000 of each body_type
    df_types = pd.read_sql(
                    "SELECT DISTINCT body_type "
                    "FROM ( "
                    "SELECT body_type FROM main "
                    "GROUP BY body_type "
                    "HAVING COUNT(*) > 1000 "
                    ") AS valid_body_type "
                    "WHERE body_type != '-' "
                    "AND body_type IS NOT NULL;"
                    ,conn
    )

    list_types = df_types.values.tolist()
    list_types = [item[0] for item in list_types]

    return list_distinct_vehicles, list_types

def get_vehicle_data(MAKE, MODEL):

    MAKE = "'"+MAKE+"'"
    MODEL = "'"+MODEL+"'"

    df_all_years = pd.read_sql("SELECT * FROM main "
    				"LEFT JOIN time USING(adID) "
    				"WHERE make="+ MAKE +" AND model="+ MODEL, conn)


    return df_all_years

def partition_total_stats_table():

    cursor = conn.cursor()
    #CREATE a new partition for the most recently computed statistics
    now = datetime.datetime.now()
    now_max = now.strftime("%Y-%m-%d %H:%M:%S")
    year = str(now.year)
    month = str(now.month)
    day = str(now.day)
    current_partition = year + '-' + month + '-' + day
    #Every update the partitions must be reorganized. A new partition for the day is created from the `max` partition (which should always be empty)
    QUERY = """
            ALTER TABLE autotrader.total_stats REORGANIZE PARTITION `max`
	        INTO ( PARTITION `"""+ current_partition + """` VALUES LESS THAN (UNIX_TIMESTAMP('"""+ now_max +"""')),
            PARTITION `max` VALUES LESS THAN MAXVALUE);
            """
    cursor.execute(QUERY)
    cursor.close()

def create_price_bucket():
    cursor = conn.cursor()
    #TRUNCATE table here so it is not in forloop in MAIN
    QUERY_TRUNCATE_TABLE = """
                TRUNCATE TABLE `autotrader`.`classes_mapping`;
                TRUNCATE TABLE `autotrader`.`total_stats_p_km`;
                TRUNCATE TABLE `autotrader`.`total_stats_p_age`;
                TRUNCATE TABLE `autotrader`.`total_stats_dealer_premium`;
                TRUNCATE TABLE `autotrader`.`total_stats_avg_phys_depr`;
                TRUNCATE TABLE `autotrader`.`total_stats_dollar_depr`;
                """
    results = cursor.execute(QUERY_TRUNCATE_TABLE, multi=True)
    for query in results:
        if cursor.fetchwarnings() != None: print("WARNING: TRUNCATING TABLES")
    conn.commit()
    cursor.close()
    #CREATE price intervals/buckets
    #THE function (1.17^x) * 5000 from 0 to 30 (categories) roughly captures the idea that as price increases, the size of the interval gets larger
    price_intervals = [((math.pow(1.17, x))*5000)-4000 for x in range(30)]
    price_intervals.append(math.inf)
    #CREATE buckets
    price_buckets = []
    for index in range(len(price_intervals)-1):
        if price_intervals[index+1] == math.inf:
            price_buckets.append((math.floor(price_intervals[index]), price_intervals[index+1]))
        else:
            price_buckets.append((math.floor(price_intervals[index]), math.floor(price_intervals[index+1])-1))

    return price_buckets

def update_vehicle_classes(df_all_years, unique_body_types, price_buckets):
    cursor = conn.cursor()
    df_grouping = df_all_years.copy()
    df_grouping = df_grouping.groupby(['body_type','full_vehicle'],as_index=False)['price'].mean()
    df_grouping = df_grouping.drop(df_grouping[df_grouping['body_type'].isin(unique_body_types) == False ].index).reset_index(drop = True)
    #df_grouping['vehicle_class'] = ""
    for row in range(df_grouping.shape[0]):
        values = list()
        #THE current row
        current_vehicle = df_grouping.iloc[row]
        year = str(current_vehicle['full_vehicle'])[-4:]
        type = str(current_vehicle['body_type'])
        #FIND the correct price bucket
        price = int(current_vehicle['price'])

        for bucket in price_buckets:
            if bucket[0]<= price <=bucket[1]: break

        values += [str(current_vehicle['full_vehicle']),type , type + ' ' + year + ' ' + str(bucket)]
        values = tuple(values)

        INSERT_CLASS_MAPPING = """
                        INSERT INTO classes_mapping(full_vehicle, body_type, vehicle_class)
                        VALUES('%s', '%s', '%s')
                        """%(values)

        try:
            cursor.execute(INSERT_CLASS_MAPPING)
            conn.commit()
        except:
            #NOTE: because classes are determined through the all_years df, duplicates will be found
            conn.rollback()

    cursor.close()

def insert_into_total_stats(vehicle_stats, MAKE, MODEL, YEAR):
    cursor = conn.cursor()

    for vehicle_type_stats in vehicle_stats:
        for table in vehicle_type_stats:
            vehicle_type_stats[table]['full_vehicle'] = MAKE+' '+MODEL+' '+YEAR

        total_stats_values = (
            vehicle_type_stats['total_stats']['full_vehicle'], vehicle_type_stats['total_stats']['body_type'], vehicle_type_stats['total_stats']['p/km'], vehicle_type_stats['total_stats']['p/age'],
            vehicle_type_stats['total_stats']['dealer_premium'], vehicle_type_stats['total_stats']['avg_physical_depr'], vehicle_type_stats['total_stats']['1_week_depr'], vehicle_type_stats['total_stats']['1_month_depr'],
            vehicle_type_stats['total_stats']['3_month_depr'], vehicle_type_stats['total_stats']['6_month_depr'], vehicle_type_stats['total_stats']['1_year_depr'], vehicle_type_stats['total_stats']['3_year_depr']
            )
        total_stats_p_km_values = (
            vehicle_type_stats['total_stats_p_km']['full_vehicle'], vehicle_type_stats['total_stats_p_km']['body_type'], vehicle_type_stats['total_stats_p_km']['min'],
            vehicle_type_stats['total_stats_p_km']['max'], vehicle_type_stats['total_stats_p_km']['sd']
        )
        total_stats_p_age_values = (
            vehicle_type_stats['total_stats_p_age']['full_vehicle'], vehicle_type_stats['total_stats_p_age']['body_type'], vehicle_type_stats['total_stats_p_age']['min'],
            vehicle_type_stats['total_stats_p_age']['max'], vehicle_type_stats['total_stats_p_age']['sd']
        )
        total_stats_dealer_premium_values = (
            vehicle_type_stats['total_stats_dealer_premium']['full_vehicle'], vehicle_type_stats['total_stats_dealer_premium']['body_type'], vehicle_type_stats['total_stats_dealer_premium']['min'],
            vehicle_type_stats['total_stats_dealer_premium']['max'], vehicle_type_stats['total_stats_dealer_premium']['sd']
        )
        total_stats_avg_phys_depr_values = (
            vehicle_type_stats['total_stats_avg_phys_depr']['full_vehicle'], vehicle_type_stats['total_stats_avg_phys_depr']['body_type'], vehicle_type_stats['total_stats_avg_phys_depr']['min'],
            vehicle_type_stats['total_stats_avg_phys_depr']['max'], vehicle_type_stats['total_stats_avg_phys_depr']['sd']
        )
        total_stats_dollar_depr_values = (
            vehicle_type_stats['total_stats_dollar_depr']['full_vehicle'], vehicle_type_stats['total_stats_dollar_depr']['body_type'], vehicle_type_stats['total_stats_dollar_depr']['1_week'],
            vehicle_type_stats['total_stats_dollar_depr']['1_month'], vehicle_type_stats['total_stats_dollar_depr']['3_month'], vehicle_type_stats['total_stats_dollar_depr']['6_month'],
            vehicle_type_stats['total_stats_dollar_depr']['1_year'], vehicle_type_stats['total_stats_dollar_depr']['3_year']
        )

        QUERY_TOTAL_STATS = """
                INSERT INTO total_stats(`full_vehicle`,
                                        `body_type`,
                                        `p/km`,
                                        `p/age`,
                                        `dealer_premium`,
                                        `avg_physical_depr`,
                                        `1_week_depr`,
                                        `1_month_depr`,
                                        `3_month_depr`,
                                        `6_month_depr`,
                                        `1_year_depr`,
                                        `3_year_depr`,
                                        `last_run`)
                VALUES('%s', '%s', %s, %s,'%s', %s,'%s','%s','%s','%s','%s','%s', NOW());
                """%(total_stats_values)

        QUERY_TOTAL_P_KM = "INSERT INTO total_stats_p_km(`full_vehicle`,`body_type`,`min`,`max`,`sd`) VALUES('%s','%s', %s, %s, %s);"%(total_stats_p_km_values)
        QUERY_TOTALL_P_AGE = "INSERT INTO total_stats_p_age(`full_vehicle`,`body_type`,`min`,`max`,`sd`) VALUES('%s','%s', %s, %s, %s);"%(total_stats_p_age_values)
        QUERY_TOTAL_DEALER = "INSERT INTO total_stats_dealer_premium(`full_vehicle`,`body_type`,`min`,`max`,`sd`) VALUES('%s','%s', '%s', '%s', '%s');"%(total_stats_dealer_premium_values)
        QUERY_TOTAL_DEPR = "INSERT INTO total_stats_avg_phys_depr(`full_vehicle`,`body_type`,`min`,`max`,`sd`) VALUES('%s','%s', %s, %s, %s);"%(total_stats_avg_phys_depr_values)
        QUERY_TOTAL_DOLLAR_DEPR = "INSERT INTO total_stats_dollar_depr(`full_vehicle`,`body_type`,`1_week`,`1_month`,`3_month`,`6_month`,`1_year`,`3_year`) VALUES('%s','%s', %s, %s, %s, %s, %s, %s);"%(total_stats_dollar_depr_values)

        queries = (QUERY_TOTAL_STATS,QUERY_TOTAL_P_KM,QUERY_TOTALL_P_AGE,QUERY_TOTAL_DEALER,QUERY_TOTAL_DEPR,QUERY_TOTAL_DOLLAR_DEPR)

        for query in queries:
            try:
                cursor.execute(query)
                conn.commit()
            except:
                conn.rollback()
                print("----------------- ERROR (NEEDS LOGGING): unable to insert -----------------")
                print(query)
                print("---------------------------------------------------------------------------")
    cursor.close()

def clean_data(df_all_years):

#TODO: Remove all full_vehicle that have less than X count
#Reformat kilometers
    df_all_years['kilometers'] = df_all_years['kilometers'].str.slice_replace(start=-3,repl='')
    #drop rows which would have less than 1000KM
    df_all_years = df_all_years.drop(df_all_years[df_all_years['kilometers'].map(len) < 4].index)
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

def compute_price_change(df, today, time_period):
    if today['time_entered'] - time_period > today['time_entered'] - df.loc[0]['time_entered']: return ('-', '0.0')
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
    #COMPUTE dollar change
    dollar_change = str(round(today['price']-(today['price']/(1+one_week_delta)),2))
    one_week_delta = str(round(one_week_delta*100,2))+'%'

    return (one_week_delta, dollar_change)

def compute_statistics(df_total, df_all_years_total, YEAR, body_type):
    computations = list()
    #SEPERATE statistic computation by body_type
    for body in body_type:

        tables = dict()
        df = df_total.copy()
        df_all_years = df_all_years_total.copy()

        df = df.drop(df[df['body_type'] != body].index).reset_index(drop = True)
        df_all_years = df_all_years.drop(df_all_years[df_all_years['body_type'] != body].index).reset_index(drop = True)
        # print(df)
        # print(df_all_years)
        if df.empty or df_all_years.empty: continue
        now = datetime.datetime.now()
        #COMPUTE common statistics
        avg_price = df['price'].mean()
        avg_kilometers = df['kilometers'].mean()

        #COMPUTE Price/Kilometers
        df_p_to_km = df[['full_vehicle','body_type','price','kilometers']].copy()
        df_p_to_km['p_km'] = pd.Series(df_p_to_km['price']/df_p_to_km['kilometers'], index=df_p_to_km.index)
        #STD of one row returns NULL which cannot be inserted to DB
        if df_p_to_km.shape[0]==1:
            sd_p_km = 0.0
        else:
            sd_p_km = round(df_p_to_km['p_km'].std(),2)

        avg_p_km = round(df_p_to_km['p_km'].mean(),2)
        min_p_km = round(df_p_to_km['p_km'].min(),2)
        max_p_km = round(df_p_to_km['p_km'].max(),2)
        #COMPUTE Price/Age
        age = now.year + (now.day+now.month*30)/365 - int(YEAR)
        df_p_to_age = df[['full_vehicle','body_type','price']].copy()
        df_p_to_age['p_age'] = pd.Series(df_p_to_age['price']/age, index=df_p_to_age.index)
        #STD of one row returns NULL which cannot be inserted to DB
        if df_p_to_age.shape[0]==1:
            sd_p_age = 0.0
        else:
            sd_p_age = round(df_p_to_age['p_age'].std(),2)
        avg_p_age = round(df_p_to_age['p_age'].mean(),2)
        min_p_age = round(df_p_to_age['p_age'].min(),2)
        max_p_age = round(df_p_to_age['p_age'].max(),2)
        #COMPUTE Dealer Premium
        df_dealer_premium = df[['full_vehicle','body_type','adType','price']].copy()
        avg_dealer_price = df_dealer_premium[df_dealer_premium['adType']=='dealer']['price'].mean()
        df_dealer_premium = df_dealer_premium[df_dealer_premium['adType']=='private']
        df_dealer_premium['dealer_premium'] = pd.Series(avg_dealer_price/df_dealer_premium['price'], index=df_dealer_premium.index)

        if not df_dealer_premium['dealer_premium'].isnull().all():

            df_dealer_premium['dealer_premium'] = (df_dealer_premium['dealer_premium']-1)*100
            avg_dealer_premium = str(round(df_dealer_premium['dealer_premium'].mean(),2))+'%'
            min_dealer_premium = str(round(df_dealer_premium['dealer_premium'].min(),2))+'%'
            max_dealer_premium = str(round(df_dealer_premium['dealer_premium'].max(),2))+'%'
            sd_dealer_premium = str(round(df_dealer_premium['dealer_premium'].std(),2))+'%'
        else:
            avg_dealer_premium, min_dealer_premium, max_dealer_premium, sd_dealer_premium = ('-','-','-','-')
        #COMPUTE avg_physical_depr
        df_phys_depr = df[['full_vehicle','body_type','kilometers']].copy()
        #avg KM / old KM
        df_all_years = df_all_years[['year','kilometers']]
        df_all_years = df_all_years.groupby(['year'],as_index=False)['kilometers'].agg(['mean','count','std']).reset_index()
        #Find the oldest model year which has a reasonable number of samples
        if df_all_years[df_all_years['count'] > 25].empty:
            avg_phys_depr, min_phys_depr, max_phys_depr, sd_phys_depr = (0.0, 0.0, 0.0, 0.0)
        else:
            #GET the oldest year with a reasonable sample size
            df_oldest_year = df_all_years[df_all_years['count'] > 25].iloc[0]
            #Assume the likely endlife of the vehicle is 1.5 SD beyond the average KM of the oldest model
            oldest_km = df_oldest_year['mean']+(1.5*df_oldest_year['std'])
            #Average Physical Depretiation
            df_phys_depr['physical_depr'] = pd.Series(df_phys_depr['kilometers']/oldest_km, index=df_phys_depr.index)

            if df_phys_depr.shape[0]==1:
                sd_phys_depr = 0.0
            else:
                sd_phys_depr = round(df_phys_depr['physical_depr'].std(),2)
            avg_phys_depr = round(df_phys_depr['physical_depr'].mean(),2)
            min_phys_depr = round(df_phys_depr['physical_depr'].min(),2)
            max_phys_depr = round(df_phys_depr['physical_depr'].max(),2)
        #COMPUTE depreciation table
        #Find the latest date
        df_ordered_price = df.groupby(['time_entered'],as_index=False)['price'].mean().sort_values(by=['time_entered'], ascending=False)

        if df_ordered_price.empty:
            (one_week_delta, one_month_delta, three_month_delta, six_month_delta, one_year_delta, three_year_delta) = ('-', '-', '-', '-', '-', '-')
            (one_week_dollar, one_month_dollar, three_month_dollar, six_month_dollar, one_year_dollar, three_year_dollar) = ('-', '-', '-', '-', '-', '-')
        else:
            df_ordered_price['time_entered'] = pd.to_datetime(df_ordered_price['time_entered'])
            today = df_ordered_price.loc[df_ordered_price['time_entered'].idxmax()]
            # COMPUTE depreciation and dollar change
            one_week_delta, one_week_dollar = compute_price_change(df_ordered_price, today, today['time_entered'] - pd.DateOffset(days=7))
            one_month_delta, one_month_dollar = compute_price_change(df_ordered_price, today, today['time_entered'] - pd.DateOffset(days=30))
            three_month_delta, three_month_dollar = compute_price_change(df_ordered_price, today, today['time_entered'] - pd.DateOffset(days=30*3))
            six_month_delta, six_month_dollar = compute_price_change(df_ordered_price, today, today['time_entered'] - pd.DateOffset(days=30*6))
            one_year_delta, one_year_dollar = compute_price_change(df_ordered_price, today, today['time_entered'] - pd.DateOffset(days=365))
            three_year_delta, three_year_dollar = compute_price_change(df_ordered_price, today, today['time_entered'] - pd.DateOffset(days=365*3))

        tables['total_stats'] = {'body_type':body,
                                        'p/km':avg_p_km,
                                        'p/age':avg_p_age,
                                        'dealer_premium':avg_dealer_premium,
                                        'avg_physical_depr':avg_phys_depr,
                                        '1_week_depr':one_week_delta,
                                        '1_month_depr':one_month_delta,
                                        '3_month_depr':three_month_delta,
                                        '6_month_depr':six_month_delta,
                                        '1_year_depr':one_year_delta,
                                        '3_year_depr':three_year_delta
                                        }

        tables['total_stats_p_km'] = {'body_type':body,'min':min_p_km,'max':max_p_km,'sd':sd_p_km }
        tables['total_stats_p_age'] = {'body_type':body,'min':min_p_age,'max':max_p_age,'sd':sd_p_age }
        tables['total_stats_dealer_premium'] = {'body_type':body,'min':min_dealer_premium,'max':max_dealer_premium,'sd':sd_dealer_premium}
        tables['total_stats_avg_phys_depr'] = {'body_type':body,'min':min_phys_depr,'max':max_phys_depr,'sd':sd_phys_depr}
        tables['total_stats_dollar_depr'] = {'body_type':body,'1_week':one_week_dollar,'1_month':one_month_dollar,'3_month':three_month_dollar,'6_month':six_month_dollar,'1_year':one_year_dollar,'3_year':three_year_dollar}
        computations.append(tables)
    #RETURN computed dict
    return computations

def compute_total_ranking():

    cursor = conn.cursor()
    pd.options.mode.chained_assignment = None
    #MAPPING between total_stats and view (stats_rank_all)
    mapping = {
            'dealer_premium_rank':'dealer_premium',
            'avg_physical_depr_rank':'avg_physical_depr',
            '1_week_depr_rank':'1_week_depr',
            '1_month_depr_rank':'1_month_depr',
            '3_month_depr_rank':'3_month_depr',
            '6_month_depr_rank':'6_month_depr',
            '1_year_depr_rank':'1_year_depr',
            '3_year_depr_rank':'3_year_depr'
    }
    #RETRIEVE the most recent partition name
    #LIMIT 2,2 the most recent partition should be 3rd order desc
    QUERY_GET_PARTITION = """
        	SELECT PARTITION_NAME FROM information_schema.partitions
        	WHERE TABLE_SCHEMA='autotrader'
        	AND TABLE_NAME = 'total_stats'
        	AND PARTITION_NAME IS NOT NULL
        	ORDER BY information_schema.partitions.PARTITION_NAME DESC LIMIT 2 , 2;
            """

    cursor.execute(QUERY_GET_PARTITION)
    latest_partition = cursor.fetchall()
    latest_partition = latest_partition[0][0]
    QUERY_GET_STATS = """
            SELECT * FROM autotrader.total_stats PARTITION (`"""+ latest_partition +"""`);
            """

    stats_df = pd.read_sql(QUERY_GET_STATS, conn)
    #CONVERT % columns to float
    stats_df['dealer_premium'] = stats_df['dealer_premium'].str.slice_replace(start=-1,repl='')
    stats_df['1_week_depr'] = stats_df['1_week_depr'].str.slice_replace(start=-1,repl='')
    stats_df['1_month_depr'] = stats_df['1_month_depr'].str.slice_replace(start=-1,repl='')
    stats_df['3_month_depr'] = stats_df['3_month_depr'].str.slice_replace(start=-1,repl='')
    stats_df['6_month_depr'] = stats_df['6_month_depr'].str.slice_replace(start=-1,repl='')
    stats_df['1_year_depr'] = stats_df['1_year_depr'].str.slice_replace(start=-1,repl='')
    stats_df['3_year_depr'] = stats_df['3_year_depr'].str.slice_replace(start=-1,repl='')

    stats_columns = ['dealer_premium','1_week_depr','1_month_depr','3_month_depr','6_month_depr','1_year_depr','3_year_depr']
    stats_df[stats_columns] = stats_df[stats_columns].apply(pd.to_numeric)

    QUERY_GET_RANK_ALL = """
            SELECT * FROM autotrader.stats_rank_all;
             """

    rank_df = pd.read_sql(QUERY_GET_RANK_ALL, conn)
    #UPDATE the most recent full_vehicle
    if not rank_df.empty: rank_df = rank_df.truncate(after=-1)
    rank_df[['full_vehicle','body_type']] = stats_df[['full_vehicle','body_type']].copy()
    vehicles = stats_df[['full_vehicle','body_type']].values.tolist()
    #GET list of stats that need to be calculated
    stats_list = rank_df.columns.values.tolist()
    stats_list.remove('full_vehicle')
    stats_list.remove('body_type')

    #COMPUTE ranks for stats in stats list
    print("-------------------------------------------------------------------")
    print("|                POPULATING TOTAL RANK                            |")
    print("-------------------------------------------------------------------")
    for stat in tqdm(stats_list):

        current_df = stats_df[['full_vehicle','body_type', mapping[stat] ]].copy()
        current_df = current_df.sort_values(by=[mapping[stat]], ascending=False).reset_index(drop = True)
        current_df.index = range(1,len(current_df)+1)
        current_df = current_df.reset_index()
        # DEBUG print rankings for each category
        # print(current_df)
        for vehicle in vehicles:
            current_row = current_df.loc[(current_df['full_vehicle'] == vehicle[0]) & (current_df['body_type'] == vehicle[1])]
            rank = current_row['index'].values[0]
            rank_df[stat].loc[(rank_df['full_vehicle'] == vehicle[0]) & (rank_df['body_type'] == vehicle[1])] = rank

    # print(rank_df)
    #UPDATE / replace the view
    QUERY_TRUNCATE_TABLE = """
                TRUNCATE TABLE `autotrader`.`stats_rank_all`;
                """

    cursor.execute(QUERY_TRUNCATE_TABLE)

    rank_list = rank_df.values.tolist()

    for row in rank_list:
        converted_row = ["NULL" if item!=item else item for item in row]
        for index in range(len(converted_row)):
            if type(converted_row[index]) == float: converted_row[index] = str(converted_row[index])

        converted_row = tuple(converted_row)

        UPDATE_VIEW_ALL = """INSERT INTO `autotrader`.`stats_rank_all`
                            (`full_vehicle`,
                            `body_type`,
                            `dealer_premium_rank`,
                            `avg_physical_depr_rank`,
                            `1_week_depr_rank`,
                            `1_month_depr_rank`,
                            `3_month_depr_rank`,
                            `6_month_depr_rank`,
                            `1_year_depr_rank`,
                            `3_year_depr_rank`)
                    VALUES('%s','%s', %s, %s, %s, %s, %s, %s, %s, %s);"""%(converted_row)

        try:
            cursor.execute(UPDATE_VIEW_ALL)
            conn.commit()
        except:
            print("EXCEPTION")
            conn.rollback()

    cursor.close()

def compute_class_ranking():

    cursor = conn.cursor()
    pd.options.mode.chained_assignment = None

    #MAPPING between total_stats and view (stats_rank_all)
    mapping = {
            'p/km_rank':'p/km',
            'p/age_rank':'p/age',
            'dealer_premium_rank':'dealer_premium',
            'avg_physical_depr_rank':'avg_physical_depr',
            '1_week_depr_rank':'1_week_depr',
            '1_month_depr_rank':'1_month_depr',
            '3_month_depr_rank':'3_month_depr',
            '6_month_depr_rank':'6_month_depr',
            '1_year_depr_rank':'1_year_depr',
            '3_year_depr_rank':'3_year_depr'
    }
    #RETRIEVE the most recent partition name
    QUERY_GET_PARTITION = """
        	SELECT PARTITION_NAME FROM information_schema.partitions
        	WHERE TABLE_SCHEMA='autotrader'
        	AND TABLE_NAME = 'total_stats'
        	AND PARTITION_NAME IS NOT NULL
        	ORDER BY information_schema.partitions.PARTITION_NAME DESC LIMIT 2 , 2;
            """

    cursor.execute(QUERY_GET_PARTITION)
    latest_partition = cursor.fetchall()
    latest_partition = latest_partition[0][0]

    QUERY_GET_STATS = """
            SELECT * FROM autotrader.total_stats PARTITION (`"""+ latest_partition +"""`)
            LEFT JOIN classes_mapping USING(full_vehicle, body_type);
            """

    stats_df = pd.read_sql(QUERY_GET_STATS, conn)
    #REMOVE vehicles whoch do not have a class
    stats_df = stats_df.dropna(subset = ['vehicle_class']).reset_index(drop = True)
    #CONVERT % columns to float
    stats_df['dealer_premium'] = stats_df['dealer_premium'].str.slice_replace(start=-1,repl='')
    stats_df['1_week_depr'] = stats_df['1_week_depr'].str.slice_replace(start=-1,repl='')
    stats_df['1_month_depr'] = stats_df['1_month_depr'].str.slice_replace(start=-1,repl='')
    stats_df['3_month_depr'] = stats_df['3_month_depr'].str.slice_replace(start=-1,repl='')
    stats_df['6_month_depr'] = stats_df['6_month_depr'].str.slice_replace(start=-1,repl='')
    stats_df['1_year_depr'] = stats_df['1_year_depr'].str.slice_replace(start=-1,repl='')
    stats_df['3_year_depr'] = stats_df['3_year_depr'].str.slice_replace(start=-1,repl='')

    stats_columns = ['dealer_premium','1_week_depr','1_month_depr','3_month_depr','6_month_depr','1_year_depr','3_year_depr']
    stats_df[stats_columns] = stats_df[stats_columns].apply(pd.to_numeric)

    QUERY_GET_RANK_CLASSES = """
            SELECT * FROM stats_rank_classes;
            """

    rank_df = pd.read_sql(QUERY_GET_RANK_CLASSES, conn)

    QUERY_GET_CLASSES = """
            SELECT DISTINCT vehicle_class FROM autotrader.classes_mapping;
            """

    classes_df = pd.read_sql(QUERY_GET_CLASSES, conn)
    classes = classes_df['vehicle_class'].values.tolist()
    #UPDATE the most recent full_vehicle
    if not rank_df.empty: rank_df = rank_df.truncate(after=-1)
    rank_df[['full_vehicle','body_type']] = stats_df[['full_vehicle','body_type']].copy()
    #vehicles = stats_df[['full_vehicle','body_type']].values.tolist()
    #GET list of stats that need to be calculated
    stats_list = rank_df.columns.values.tolist()
    stats_list.remove('full_vehicle')
    stats_list.remove('body_type')

    # print(stats_df)
    # print(rank_df)
    # print(stats_list)
    print("-------------------------------------------------------------------")
    print("|                POPULATING CLASS RANK                            |")
    print("-------------------------------------------------------------------")
    for vehicle_class in tqdm(classes):
        stats_df_current = stats_df[stats_df['vehicle_class'] == vehicle_class]

        for stat in stats_list:

            current_df = stats_df_current[['full_vehicle','body_type', mapping[stat] ]].copy()
            current_df = current_df.sort_values(by=[mapping[stat]], ascending=False).reset_index(drop = True)
            current_df.index = range(1,len(current_df)+1)
            current_df = current_df.reset_index()
            #DEBUG print rankings for each category
            #print(current_df)
            for vehicle in range(current_df.shape[0]):

                current_row = current_df.iloc[vehicle]
                rank = current_row['index']
                current_full_vehicle = current_row['full_vehicle']
                current_body_type = current_row['body_type']
                rank_df[stat].loc[(rank_df['full_vehicle'] == current_full_vehicle) & (rank_df['body_type'] == current_body_type)] = rank

    #print(rank_df)
    QUERY_TRUNCATE_TABLE = """
                TRUNCATE TABLE `autotrader`.`stats_rank_classes`;
                """

    cursor.execute(QUERY_TRUNCATE_TABLE)
    rank_list = rank_df.values.tolist()

    for row in rank_list:
        converted_row = tuple(row)

        UPDATE_VIEW_ALL = """INSERT INTO `autotrader`.`stats_rank_classes`
                            (`full_vehicle`,
                            `body_type`,
                            `p/km_rank`,
                            `p/age_rank`,
                            `dealer_premium_rank`,
                            `avg_physical_depr_rank`,
                            `1_week_depr_rank`,
                            `1_month_depr_rank`,
                            `3_month_depr_rank`,
                            `6_month_depr_rank`,
                            `1_year_depr_rank`,
                            `3_year_depr_rank`)
                    VALUES('%s','%s', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""%(converted_row)

        try:
            cursor.execute(UPDATE_VIEW_ALL)
            conn.commit()
        except:
            print("EXCEPTION")
            conn.rollback()

    cursor.close()


######---------------------------------------MAIN---------------------------------------######
if __name__ == '__main__':

    list_distinct_vehicles, body_type = get_distinct_vehicles_and_types()
    price_buckets = create_price_bucket()
    print("-------------------------------------------------------------------")
    print("|               POPULATING STATISTICS                             |")
    print("-------------------------------------------------------------------")
    for vehicle in tqdm(list_distinct_vehicles):
        #SELECT string / vehicle data
        MAKE = vehicle[0]
        MODEL = vehicle[1]
        YEAR = vehicle[2]

        df_all_years = get_vehicle_data(MAKE, MODEL)
        df, df_all_years = clean_data(df_all_years)
        update_vehicle_classes(df_all_years, body_type, price_buckets)
        vehicle_stats = compute_statistics(df, df_all_years, YEAR, body_type)
        insert_into_total_stats(vehicle_stats, MAKE, MODEL, YEAR)

    partition_total_stats_table()
    compute_total_ranking()
    compute_class_ranking()


    conn.close()
