import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import dash_table
import plotly.graph_objs as go
import numpy as np
import pandas as pd
import os
import mysql.connector

class CleanData():
    def _reformat_columns(self, df):
        #Reformat kilometers
        df['kilometers'] = df['kilometers'].str.slice_replace(start=-3,repl='')
        #drop rows which would have less than 1000KM
        df = df.drop(df[df['kilometers'].map(len) < 4].index)
        df['kilometers'] = df['kilometers'].str.replace(",","")
        df['kilometers'] = df['kilometers'].astype(str).astype(int)
        #Reformat price
        df['price'].astype(int)
        return df

    def clean_vehicle_all_years(self, df_all_years):
        return self._reformat_columns(df_all_years)

    def clean_veicle_year(self, df_year, multi=False):
        #OPTIONAL arg, reduce a df with multiple years to a signle year = multi (for AVG Physical Depretiation calc)
        if multi:
            df_year = df_year.drop(df_year[df_year['year'].astype(int) != int(multi.replace("'",""))].index).copy()
        else:
            df_year = _reformat_columns(df_year)
        #Remove price outliers greater than 1.65 standard deviations (97.5%)
        tolerance = 1.96
        std = df_year['price'].std()
        avg = df_year['price'].mean()
        df_year = df_year.drop(df_year[(df_year['price'] > (avg+tolerance*std)) | (df_year['price'] < (avg-tolerance*std)) ].index)
        return df_year

    def clean_dealer_premium(self, df_dealer_premium):
        df_dealer_premium['dealer_premium'] = df_dealer_premium['dealer_premium'].str.slice_replace(start=-1,repl='')
        df_dealer_premium['dealer_premium'] = pd.to_numeric(df_dealer_premium['dealer_premium'])
        return df_dealer_premium

    def clean_depreciation(self, df_depreciation):
        for label in df_depreciation.drop(df_depreciation[['full_vehicle']], axis=1):
            df_depreciation[label] = df_depreciation[label].str.slice_replace(start=-1,repl='')
            df_depreciation[label] = pd.to_numeric(df_depreciation[label])
        return df_depreciation

class DataFrame(CleanData):
    #CLASS variables
    conn = mysql.connector.connect(user=os.environ['USER_NAME'], passwd=os.environ['PASSWORD'], host=os.environ['HOST_NAME'],database=os.environ['DATABASE'])
    def __init__(self, type, make, model, year):
        super().__init__()
        self.full_vehicle = make+" "+model+" "+year
        self.type = type
        self.make = make
        self.model = model
        self.year = year
        #STORE dataframes which will be resused as attributes
        self.df_year = None
        self.df_class_value_ratios = None
        self.df_class_depreciation = None

    def get_years(self):

        df_all_years = pd.read_sql("SELECT * FROM main "
        				"LEFT JOIN time USING(adID) "
                        "LEFT JOIN price_change USING(adID) "
        				"WHERE body_type='"+ self.type +"' AND make='"+ self.make +"' AND model='"+ self.model+"'", DataFrame.conn)

        df_all_years = self.clean_vehicle_all_years(df_all_years)
        self.df_year = self.clean_veicle_year(df_all_years, self.year)
        return df_all_years, self.df_year

    def get_current_stats_partition(self):
            current_partition = """
                        	SELECT PARTITION_NAME FROM information_schema.partitions
                        	WHERE TABLE_SCHEMA='autotrader'
                        	AND TABLE_NAME = 'total_stats'
                        	AND PARTITION_NAME IS NOT NULL
                        	ORDER BY information_schema.partitions.PARTITION_NAME DESC LIMIT 2 , 2;
                            """
            cursor = DataFrame.conn.cursor()
            cursor.execute(current_partition)
            partition = cursor.fetchall()[0][0]
            cursor.close()
            return partition

    def get_total_rank_count(self):
        return ( pd.read_sql("SELECT COUNT(*) AS total_count FROM stats_rank_all ",DataFrame.conn) )

    def get_table_stats(self, partition):
        return (
        pd.read_sql("SELECT * FROM autotrader.total_stats PARTITION (`"+ partition +"`) "
                    "WHERE full_vehicle ='"+ self.full_vehicle+"' AND body_type='"+self.type+"'", DataFrame.conn)
        )

    def get_total_rank(self):
        return (
        pd.read_sql("SELECT * FROM stats_rank_all "
                    "WHERE full_vehicle = '"+ self.full_vehicle+"' AND body_type= '"+ self.type+"'", DataFrame.conn)
        )

    def get_total_class_rank(self):
        return (
        pd.read_sql("SELECT * FROM stats_rank_classes "
                    "WHERE full_vehicle = '"+self.full_vehicle+"' AND body_type='"+self.type+"'", DataFrame.conn)
        )

    def get_class_list(self):
        df_class = pd.read_sql("SELECT full_vehicle FROM classes_mapping "
                    "WHERE vehicle_class = ("
                    "SELECT vehicle_class FROM classes_mapping "
                    "WHERE full_vehicle = '"+ self.full_vehicle +"' "
                    "AND vehicle_class LIKE '"+ self.type[:-1]+"%')", DataFrame.conn)

        df_class_list = df_class['full_vehicle'].tolist()
        df_class_list = ["'"+vehicle+"'" for vehicle in df_class_list]
        df_class_list = ','.join(df_class_list)
        return df_class_list

    def get_class_value_ratios(self, partition, class_list):
        self.df_class_value_ratios = pd.read_sql("SELECT full_vehicle, `p/km`, `p/age`, dealer_premium, avg_physical_depr "
                    "FROM autotrader.total_stats PARTITION (`"+ partition +"`) "
                    "WHERE full_vehicle IN ("+class_list+") AND body_type= '"+self.type+"'", DataFrame.conn)
        return self.df_class_value_ratios

    def get_class_depreciation(self, partition, class_list):

        self.df_class_depreciation = pd.read_sql("SELECT full_vehicle, 1_week_depr, 1_month_depr, 3_month_depr, 6_month_depr, 1_year_depr, 3_year_depr "
                "FROM autotrader.total_stats PARTITION (`"+ partition +"`) "
                "WHERE full_vehicle IN ("+class_list+") AND body_type= '"+self.type+"'", DataFrame.conn)
        return self.df_class_depreciation

    def get_price_by_province(self):
        return (
            pd.read_sql("SELECT province, AVG(price) AS average_price "
                    "FROM main "
                    "WHERE full_vehicle = '"+self.full_vehicle+"' AND body_type= '"+self.type+"'"
                    "GROUP BY province "
                    "HAVING count(*) > 10 ",DataFrame.conn)
        )

    def get_table_stats_descriptive(self):

        df_table_stats_descriptive = pd.read_sql("SELECT * FROM total_stats_p_km  "
                        "LEFT JOIN total_stats_p_age USING(full_vehicle, body_type) "
                        "LEFT JOIN total_stats_dealer_premium USING(full_vehicle, body_type) "
                        "LEFT JOIN total_stats_avg_phys_depr USING(full_vehicle, body_type)"
                        "WHERE full_vehicle = '"+self.full_vehicle+"' AND body_type= '"+self.type+"'", DataFrame.conn)

        df_table_stats_descriptive.columns = ['full_vehicle','body_type','min_p_km','max_p_km','sd_p_km','min_p_age','max_p_age',
                                            'sd_p_age','min_dealer','max_dealer','sd_dealer','min_phys_depr','max_phys_depr','sd_phys_depr']
        return df_table_stats_descriptive

    def get_table_stats_depreciation(self):
        return (
            pd.read_sql("SELECT * FROM total_stats_dollar_depr "
                        "WHERE full_vehicle = '"+self.full_vehicle+"' AND body_type= '"+self.type+"'", DataFrame.conn)
        )

    def close_connection(self):
        DataFrame.conn.close()
