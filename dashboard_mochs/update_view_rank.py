import numpy as np
import pandas as pd
import mysql.connector
import datetime
import os


def get_connection():
    conn = mysql.connector.connect(user=os.environ['USER_NAME'],
                                   passwd=os.environ['PASSWORD'],
                                   host=os.environ['HOST_NAME'],
                                   database=os.environ['DATABASE'])

    newest_vehicles = pd.read_sql("SELECT * FROM main "
                                  "LEFT JOIN time USING(adID) "
                                  "WHERE time_entered > (SELECT COALESCE( MAX(last_run),'1000-01-01') FROM total_stats)", conn)

    total_stats_table = pd.read_sql("SELECT * FROM total_stats", conn)

    conn.close()
    return newest_vehicles, total_stats_table


def clean_data(df):
    # Reformat kilometers
    df['kilometers'] = df['kilometers'].str.slice_replace(start=-3, repl='')
    df = df.drop(df[df['kilometers'].map(len) < 4].index)  # drop rows which would have less than 1000KM
    df['kilometers'] = df['kilometers'].str.replace(",", "")
    df['kilometers'] = df['kilometers'].astype(str).astype(int)
# Reformat price
    df['price'].astype(int)

# Need to remove outliers
    return df

def compute_statistics(newest_vehicles, total_stats_table):

    now = datetime.datetime.now()
    # AGGREGATE basic statistics
    # AGGREGATE for dealer premium
    add_type = newest_vehicles[['full_vehicle', 'adType', 'price']].copy()
    add_type = add_type.dropna()
    grouped_add_type = add_type.groupby(['full_vehicle', 'adType'], as_index=False).agg(['mean', 'count']).reset_index()
    # AGGREGATE for price and kilometers
    newest_vehicles = newest_vehicles.groupby(['full_vehicle'], as_index=False)['kilometers', 'price'].agg(['mean', 'count']).reset_index()
    newest_vehicles = newest_vehicles.drop(newest_vehicles.columns[2], axis=1)
    newest_vehicles['dealer_premium'] = 0
    # #Remove small sample sizes
    # df = df.drop( df[df['price']['count'] <= 25].index)
    # ADD new full_vehicle types which do not exist in the stats table
    diff = pd.concat([newest_vehicles['full_vehicle'], total_stats_table['full_vehicle']]).drop_duplicates(keep=False)
    # For each diff, append the row with emtpy stats
    for row in range(diff.shape[0]):
        total_stats_table = total_stats_table.append(pd.Series([diff.loc[row],
                                                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                                                               index=total_stats_table.columns), ignore_index=True)

    # COMPUTE dealer premium
    unique_vehicle = grouped_add_type['full_vehicle'].unique().tolist()

    for vehicle_name in unique_vehicle:
        grouped_add_type_rows = grouped_add_type.loc[grouped_add_type['full_vehicle'] == vehicle_name].reset_index(drop=True)
        if grouped_add_type_rows.shape[0] == 2:

            dealer_price = grouped_add_type_rows['price']['mean'][0]
            private_price = grouped_add_type_rows['price']['mean'][1]
            dealer_premium = dealer_price / private_price
            # ADD back to newest_vehicles
            index = newest_vehicles.index[newest_vehicles['full_vehicle'] == vehicle_name]
            newest_vehicles.at[index, 'dealer_premium'] = dealer_premium

    # For each row, compute the updated statistics
    # total_stats_table -> represents the mysql total_stats table
    # stats_row -> row from total_stats_table
    # current_row -> corresponding row from new vehicles added (newest_vehicles frame)
    for row in range(total_stats_table.shape[0]):
        # Get current rows to work on
        stats_row = total_stats_table.iloc[[row]]
        vehicle_name = stats_row['full_vehicle'].values[0]
        current_row = newest_vehicles.loc[newest_vehicles['full_vehicle'] == vehicle_name]
        # Assign basic stats
        avg_price = current_row['price']['mean'].values[0]
        avg_kilometers = current_row['kilometers']['mean'].values[0]
        # UPDATE the total count
        stats_row_total_count = stats_row['total_count'].values[0]
        current_row_count = current_row['price']['count'].values[0]
        stats_row_total_count += current_row_count
        # COMPUTE the moving averages, find the total weight of the new vehicles
        weight = current_row_count / stats_row_total_count
        # COMPUTE Price/Kilometers
        P_to_KM = (avg_price / avg_kilometers) * weight
        total_stats_table.at[row, 'p/km'] += P_to_KM
        # CONCAT dealer_premium
        dealer_premium = current_row['dealer_premium'].values[0] * weight
        total_stats_table.at[row, 'dealer_premium'] += dealer_premium
        # COMPUTE Price/Age
        YEAR = str(current_row['full_vehicle'].values[0])
        YEAR = int(YEAR[-4:])
        age = now.year + (now.day + now.month * 30) / 365 - YEAR
        P_to_AGE = avg_price / age
        total_stats_table.at[row, 'p/age'] += P_to_AGE
        # COMPUTE Average Physical Depretiation
        # avg KM / old KM

        # COMPUTE depreciation table
        # Find the latest date
        df = newest_vehicles.groupby(['time_entered'], as_index=False)['price'].mean().sort_values(by=['time_entered'], ascending=False)

        # UPDATE count
        total_stats_table.at[row, 'total_count'] = stats_row_total_count

    with pd.option_context('display.max_rows', 2100, 'display.max_columns', None):  # more options can be specified also
        print(total_stats_table)


######---------------------------------------MAIN---------------------------------------######
# UPDATE the total and class stats and create a new ranking view
newest_vehicles, total_stats_table = get_connection()
newest_vehicles = clean_data(newest_vehicles)
df = compute_statistics(newest_vehicles, total_stats_table)
