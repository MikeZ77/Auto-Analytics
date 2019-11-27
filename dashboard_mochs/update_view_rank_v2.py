import numpy as np
import pandas as pd
import mysql.connector
import math
import datetime

conn = mysql.connector.connect(user='root', password='wc3tft',
                              host='127.0.0.1',
                              database='autotrader')


def get_distinct_vehicles_and_types():

    df_distinct_vehicles = pd.read_sql(
                    "SELECT DISTINCT make, model, year "
                    "FROM main ", conn
    )
    #CONVERT to python list
    list_distinct_vehicles = df_distinct_vehicles.values.tolist()
    #ARBITRARY must be at least 1000 of each body_type
    df_types = pd.read_sql(
                    "SELECT DISTINCT body_type "
                    "FROM ( "
                    "SELECT body_type FROM autotrader.main "
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
    year = now.year
    month = now.month
    day = now.day
    date_now = year + '-' + month + '-' + day

    tomorrow = now + datetime.timedelta(days=1)
    year = tomorrow.year
    month = tomorrow.month
    day = tomorrow.day
    date_tomorrow = year + '-' + month + '-' + day

    #Every update the partitions must be reorganized. A new partition for the day is created from the `max` partition (which should always be empty)

    QUERY = """
            ALTER TABLE autotrader.total_stats REORGANIZE PARTITION `max`
	        INTO ( PARTITION `"""+ date_now + """` VALUES LESS THAN (TO_DAYS('"""+ date_tomorrow +"""')),
            PARTITION `max` VALUES LESS THAN MAXVALUE);
            """

    cursor.execute(QUERY)
    cursor.close()

def create_price_bucket():
    #TRUNCATE table here so it is not in forloop in MAIN
    cursor = conn.cursor()

    QUERY_TRUNCATE_TABLE = """
                TRUNCATE TABLE `autotrader`.`classes_mapping`;
                """
    cursor.execute(QUERY_TRUNCATE_TABLE)
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
            if price in range(bucket[0], bucket[1]): break

        #df_grouping.at[row,'vehicle_class'] = type + ' ' + year + ' ' + str(bucket)
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
    for vehicle in vehicle_stats:

        vehicle['full_vehicle'] = MAKE+' '+MODEL+' '+YEAR

        values = (
                vehicle['full_vehicle'], vehicle['body_type'], vehicle['p/km'], vehicle['p/age'], vehicle['dealer_premium'],
                vehicle['avg_physical_depr'], vehicle['1_week_depr'], vehicle['1_month_depr'], vehicle['3_month_depr'],
                vehicle['6_month_depr'], vehicle['1_year_depr'], vehicle['3_year_depr']
                )

        QUERY = """
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

                VALUES('%s', '%s',%s, %s,'%s', %s,'%s','%s','%s','%s','%s','%s', NOW());
                """%(values)

        try:
            cursor.execute(QUERY)
            conn.commit()
        except:
            conn.rollback()
            print("----------------- ERROR (NEEDS LOGGING): unable to insert -----------------")
            print(vehicle)
            print("---------------------------------------------------------------------------")
    cursor.close()

def clean_data(df_all_years):

#TODO: Remove all full_vehicle that have less than X count
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

def compute_statistics(df_total, df_all_years_total, YEAR, body_type):
    computations = list()
    #SEPERATE statistic computation by body_type
    for body in body_type:

        df = df_total.copy()
        df_all_years = df_all_years_total.copy()

        df = df.drop(df[df['body_type'] != body].index).reset_index(drop = True)
        df_all_years = df_all_years.drop(df_all_years[df_all_years['body_type'] != body].index).reset_index(drop = True)

        if df.empty or df_all_years.empty: continue

        now = datetime.datetime.now()
        #COMPUTE common statistics
        avg_price = df['price'].mean()
        avg_kilometers = df['kilometers'].mean()

        #COMPUTE Price/Kilometers
        P_to_KM = round(avg_price / avg_kilometers,2)

        #COMPUTE Price/Age
        age = now.year + (now.day+now.month*30)/365 - int(YEAR)
        P_to_AGE = round(avg_price/age,2)

        #COMPUTE Dealer Premium
        add_type = df[['adType','price']]
        grouped_add_type = add_type.groupby(['adType'],as_index=False).mean()

        if grouped_add_type.shape[0] == 2:
            dealer_price = grouped_add_type['price'][0]
            private_price = grouped_add_type['price'][1]
            dealer_premium = str(round((dealer_price/private_price-1)*100,2))+'%'
        else:
            dealer_premium = '-'
        #COMPUTE Average Physical Depretiation
        #avg KM / old KM
        df_all_years = df_all_years[['year','kilometers']]
        df_all_years = df_all_years.groupby(['year'],as_index=False)['kilometers'].agg(['mean','count','std']).reset_index()
        #Find the oldest model year which has a reasonable number of samples
        if df_all_years[df_all_years['count'] > 25].empty:
            avg_pd = 0.0
        else:
            df_all_years = df_all_years[df_all_years['count'] > 25].iloc[0]
            #Assume the likely endlife of the vehicle is 1.5 SD beyond the average KM of the oldest model
            oldest_km = df_all_years['mean']*1.5
            #Average Physical Depretiation
            avg_pd = round(avg_kilometers/oldest_km,2)

        #COMPUTE depreciation table
        #Find the latest date
        df = df.groupby(['time_entered'],as_index=False)['price'].mean().sort_values(by=['time_entered'], ascending=False)

        if df.empty:
            one_week_delta = '-'
            one_month_delta = '-'
            three_month_delta = '-'
            six_month_delta = '-'
            one_year_delta = '-'
            three_year_delta = '-'
        else:
            df['time_entered'] = pd.to_datetime(df['time_entered'])
            today = df.loc[df['time_entered'].idxmax()]
            # COMPUTE 1 Week price change
            one_week_delta = compute_price_change(df, today, today['time_entered'] - pd.DateOffset(days=7))
            one_month_delta = compute_price_change(df, today, today['time_entered'] - pd.DateOffset(days=30))
            three_month_delta = compute_price_change(df, today, today['time_entered'] - pd.DateOffset(days=30*3))
            six_month_delta = compute_price_change(df, today, today['time_entered'] - pd.DateOffset(days=30*6))
            one_year_delta = compute_price_change(df, today, today['time_entered'] - pd.DateOffset(days=365))
            three_year_delta = compute_price_change(df, today, today['time_entered'] - pd.DateOffset(days=365*3))


        computations.append(
                {'body_type':body,
                'p/km':P_to_KM,
                'p/age':P_to_AGE,
                'dealer_premium':dealer_premium,
                'avg_physical_depr':avg_pd,
                '1_week_depr':one_week_delta,
                '1_month_depr':one_month_delta,
                '3_month_depr':three_month_delta,
                '6_month_depr':six_month_delta,
                '1_year_depr':one_year_delta,
                '3_year_depr':three_year_delta
                })
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
    QUERY_GET_PARTITION = """
        	SELECT PARTITION_NAME FROM information_schema.partitions
        	WHERE TABLE_SCHEMA='autotrader'
        	AND TABLE_NAME = 'total_stats'
        	AND PARTITION_NAME IS NOT NULL
        	ORDER BY information_schema.partitions.PARTITION_NAME DESC LIMIT 1 , 1;
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

    stats_df[['dealer_premium',
            '1_week_depr',
            '1_month_depr',
            '3_month_depr',
            '6_month_depr',
            '1_year_depr',
            '3_year_depr']] = stats_df[['dealer_premium',
                                        '1_week_depr',
                                        '1_month_depr',
                                        '3_month_depr',
                                        '6_month_depr',
                                        '1_year_depr',
                                        '3_year_depr']].apply(pd.to_numeric)

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
    for stat in stats_list:

        current_df = stats_df[['full_vehicle','body_type', mapping[stat] ]].copy()
        current_df = current_df.sort_values(by=[mapping[stat]], ascending=False).reset_index(drop = True)
        current_df.index = range(1,len(current_df)+1)
        current_df = current_df.reset_index()
        # DEBUG print rankings for each category
        print(current_df)
        for vehicle in vehicles:
            current_row = current_df.loc[(current_df['full_vehicle'] == vehicle[0]) & (current_df['body_type'] == vehicle[1])]
            rank = current_row['index'].values[0]
            rank_df[stat].loc[(rank_df['full_vehicle'] == vehicle[0]) & (rank_df['body_type'] == vehicle[1])] = rank

    print(rank_df)
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
        	ORDER BY information_schema.partitions.PARTITION_NAME DESC LIMIT 1 , 1;
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

    stats_df[['dealer_premium',
            '1_week_depr',
            '1_month_depr',
            '3_month_depr',
            '6_month_depr',
            '1_year_depr',
            '3_year_depr']] = stats_df[['dealer_premium',
                                        '1_week_depr',
                                        '1_month_depr',
                                        '3_month_depr',
                                        '6_month_depr',
                                        '1_year_depr',
                                        '3_year_depr']].apply(pd.to_numeric)

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

    for vehicle_class in classes:
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

# list_distinct_vehicles, body_type = get_distinct_vehicles_and_types()
# price_buckets = create_price_bucket()
#
# for vehicle in list_distinct_vehicles:
#     #SELECT string / vehicle data
#     MAKE = vehicle[0]
#     MODEL = vehicle[1]
#     YEAR = vehicle[2]
#
#     df_all_years = get_vehicle_data(MAKE, MODEL)
#     df, df_all_years = clean_data(df_all_years)
#     update_vehicle_classes(df_all_years, body_type, price_buckets)
#     vehicle_stats = compute_statistics(df, df_all_years, YEAR, body_type)
#     insert_into_total_stats(vehicle_stats, MAKE, MODEL, YEAR)

# partition_total_stats_table()
# compute_total_ranking()
compute_class_ranking()

conn.close()
