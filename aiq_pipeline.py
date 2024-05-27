import requests
import sqlalchemy
import pandas as pd
import mysql.connector

from joblistconfig import *
from configparser import ConfigParser

class data_pipeline():
    def __init__(self) -> None:
        constants = ConfigParser()
        constants.read("constants.ini")

        self.user=constants.get("CONSTANTS","username")
        self.pwd=constants.get("CONSTANTS","password")
        self.host=constants.get("CONSTANTS","host")
        self.port=constants.get("CONSTANTS","port")
        self.db_name=constants.get("CONSTANTS","db_name")

        self.filename=constants.get("CONSTANTS","csv_file_name")
        self.openweather=constants.get("CONSTANTS","openweathermap_api")
        self.users_api=constants.get("CONSTANTS","users_api")
        self.appid=constants.get("CONSTANTS","appid")

        self.pkl_map=pkl_map

        self.start_pipeline()

    def connect_db(self,con_type) -> object:
        if con_type == 'mysql':
            return mysql.connector.connect(user=self.user, password=self.pwd, host=self.host)
        else:
            return sqlalchemy.create_engine("""mysql+mysqlconnector://{}:{}@{}:{}/{}""".format(self.user,self.pwd,self.host,self.port,self.db_name), echo=False)
        
    def execute_cursor(self,cnx,sql) -> None:
        cursor = cnx.cursor()
        cursor.execute(sql)

    def load_df_to_db(self,df,cnx,table_name) -> None:
        df.to_sql(con=cnx, name=table_name, if_exists='append', method='multi', index=False)

    def close_connect(self,cnx,con_type):
        if con_type == 'mysql':
            cnx.close()
        else:
            cnx.dispose()

    def get_request(self,url) -> object:
        return requests.get(url)
    
    def read_csv_file(self,file_path) -> object:
        return pd.read_csv(file_path)
    
    def drop_columns(self,df,drop_list) -> None:
        df.drop(columns=drop_list, inplace=True)

    def fillna_string(self,df,column) -> None:
        df['{}'.format(column)].fillna("NotAvailable", inplace=True)

    def fillna_numeric(self,df,column) -> None:
        df['{}'.format(column)].fillna(-999.0, inplace=True)

    def perform_groupby_add(self,df,column,agg_col):
        return df.groupby('{}'.format(column))['{}'.format(agg_col)].sum()

    def perform_groupby_avg(self,df,column,agg_col):
        return df.groupby('{}'.format(column))['{}'.format(agg_col)].mean()

    def df_column_rename(self,df,col):
        if type(col) is dict:
            df.rename(columns=col, inplace=True)
        else:
            df.columns=col
            
        return df

    def drop_dup(self,df) -> None:
        df.drop_duplicates(ignore_index=True, inplace=True, keep='first')

    def start_pipeline(self) -> None:
        df_users = pd.json_normalize(self.get_request(self.users_api).json())

        df_users.iloc[0,8] = 'London'
        df_users.iloc[1,8] = 'Tokyo'
        df_users.iloc[2,8] = 'Dubai'
        df_users.iloc[3,8] = 'Mumbai'
        df_users.iloc[4,8] = 'Melbourne'

        df_weather = pd.DataFrame()

        for city_name in df_users['address.city'].unique():
            build_url='{}q={}&appid={}'.format(self.openweather,city_name,self.appid)
            response_temp = self.get_request(build_url)
            if response_temp.ok:
                df_temp = pd.json_normalize(response_temp.json())
                df_weather = pd.concat([df_weather,df_temp], ignore_index=True)

        self.drop_columns(df_weather,['sys.type','sys.id','sys.sunrise','sys.sunset'])

        df_weather = self.df_column_rename(df_weather,{'name':'address.city','id':'city_id'})

        df_merged = df_users.merge(df_weather, on= 'address.city', how = 'left')

        # Not Working As Expected
        # df_merged.select_dtypes('float64').fillna(-999.0, inplace=True)
        # df_merged.select_dtypes('object').fillna("NotAvailable", inplace=True)

        for i in df_merged.select_dtypes('object').columns:
            self.fillna_string(df_merged, i)

        for i in df_merged.select_dtypes('float64').columns:
            self.fillna_numeric(df_merged, i)
        # Filling NULL for weather column

        for i in range(0,len(df_merged['weather'])):
            if pd.isnull(df_merged['weather'][i]) or df_merged['weather'][i] == "NotAvailable":
                df_merged['weather'][i]=[{'id':-999.0,'main':"NotAvailable",'description':"NotAvailable",'icon':"NotAvailable"}]


        explode = df_merged['weather'].explode()

        df_exploded = pd.DataFrame(explode.tolist(), index=explode.index)

        self.drop_columns(df_merged,['weather'])

        df_exploded = self.df_column_rename(df_exploded,{'id':'weather.id'})

        df_merged = df_merged.join(df_exploded)

        df_sales = self.read_csv_file(self.filename)

        df_final_ds = df_merged.merge(df_sales, left_on = 'id', right_on ='customer_id', how = 'inner')

        df_final_ds.reset_index(drop=True, inplace=True)

        self.drop_columns(df_final_ds,['icon','customer_id'])

        df_final_ds['order_date'] = pd.to_datetime(df_final_ds.order_date)

        df_final_ds['order_date_day']=df_final_ds.order_date.dt.day

        df_final_ds['order_date_mnth']=df_final_ds.order_date.dt.month

        df_final_ds['order_date_week']=df_final_ds.order_date.dt.day_of_week

        df_final_ds['order_date_year']=df_final_ds.order_date.dt.year

        df_final_ds['order_date_yr_mnth']=df_final_ds.order_date.dt.strftime('%Y-%m')

        df_temp = pd.DataFrame(self.perform_groupby_add(df_final_ds,'id','price'))
        df_temp = self.df_column_rename(df_temp, {'price':'total_price_per_customer'})
        df_final_ds = df_final_ds.join(df_temp, on='id', how='left')
        
        df_temp = pd.DataFrame(self.perform_groupby_avg(df_final_ds,'id','price'))
        df_temp = self.df_column_rename(df_temp, {'price':'avg_price_per_customer'})
        df_final_ds = df_final_ds.join(df_temp, on='id', how='left')

        df_temp = pd.DataFrame(self.perform_groupby_add(df_final_ds,'id','order_id'))
        df_temp = self.df_column_rename(df_temp, {'order_id':'total_orders_per_customer'})
        df_final_ds = df_final_ds.join(df_temp, on='id', how='left')

        df_temp = pd.DataFrame(self.perform_groupby_add(df_final_ds,'id','order_id'))
        df_temp = self.df_column_rename(df_temp, {'order_id':'times_orders_per_customer'})
        df_final_ds = df_final_ds.join(df_temp, on='id', how='left')

        df_temp = pd.DataFrame(self.perform_groupby_add(df_final_ds,'main','price'))
        df_temp = self.df_column_rename(df_temp, {'price':'total_price_per_main_w'})
        df_final_ds = df_final_ds.join(df_temp, on='main', how='left')

        df_temp = pd.DataFrame(self.perform_groupby_add(df_final_ds,'description','price'))
        df_temp = self.df_column_rename(df_temp, {'price':'total_price_per_desc_w'})
        df_final_ds = df_final_ds.join(df_temp, on='description', how='left')

        df_temp = pd.DataFrame(self.perform_groupby_add(df_final_ds,'main.temp','price'))
        df_temp = self.df_column_rename(df_temp, {'price':'total_price_per_temp'})
        df_final_ds = df_final_ds.join(df_temp, on='main.temp', how='left')

        df_temp = pd.DataFrame(self.perform_groupby_add(df_final_ds,'main.pressure','price'))
        df_temp = self.df_column_rename(df_temp, {'price':'total_price_per_pressure'})
        df_final_ds = df_final_ds.join(df_temp, on='main.pressure', how='left')

        df_temp = pd.DataFrame(self.perform_groupby_add(df_final_ds,'main.humidity','price'))
        df_temp = self.df_column_rename(df_temp, {'price':'total_price_per_humidity'})
        df_final_ds = df_final_ds.join(df_temp, on='main.humidity', how='left')

        df_temp = pd.DataFrame(self.perform_groupby_add(df_final_ds,'sys.country','price'))
        df_temp = self.df_column_rename(df_temp, {'price':'total_price_per_cntry'})
        df_final_ds = df_final_ds.join(df_temp, on='sys.country', how='left')

        self.end_pipeline(df_final_ds)

    def end_pipeline(self,df) -> None:
        
        df_customer = df[['id','name','username','email','phone','address.street','address.suite','address.city','sys.country','address.zipcode','address.geo.lat','address.geo.lng','coord.lon','coord.lat','total_price_per_customer','avg_price_per_customer','total_orders_per_customer','times_orders_per_customer','total_price_per_cntry']]
        df_customer = self.df_column_rename(df_customer, ['id','name','username','email','phone','street','suite','city','country','zipcode','cust_lat','cust_lng','city_lon','city_lat','total_price_per_customer','avg_price_per_customer','total_orders_per_customer','times_orders_per_customer','total_price_per_cntry'])
        self.drop_dup(df_customer)

        df_weather = df[['address.city','base','visibility','dt','timezone','city_id','cod','main.temp','main.feels_like','main.temp_min','main.temp_max','main.pressure','main.humidity','wind.speed','wind.deg','clouds.all','weather.id','main','description','total_price_per_main_w','total_price_per_temp','total_price_per_desc_w','total_price_per_pressure','total_price_per_humidity']]
        df_weather = self.df_column_rename(df_weather, ['city','base','visibility','dt','timezone','city_id','cod','temp','feels_like_temp','temp_min','temp_max','pressure','humidity','wind_speed','wind_deg','clouds','weather_id','main_status','description','total_price_per_main_w','total_price_per_temp','total_price_per_desc_w','total_price_per_pressure','total_price_per_humidity'])
        self.drop_dup(df_weather)

        df_company = df[['id','company.name','company.catchPhrase','company.bs']]
        df_company = self.df_column_rename(df_company, ['id','company_name','company_catchphrase','company_bs'])
        self.drop_dup(df_company)

        df_orders = df[['id','order_id','product_id','quantity','price','order_date','order_date_day','order_date_mnth','order_date_week','order_date_year','order_date_yr_mnth']]
        df_orders = self.df_column_rename(df_orders, ['id','order_id','product_id','quantity','price','order_date','order_date_day','order_date_mnth','order_date_week','order_date_year','order_date_yr_mnth'])
        self.drop_dup(df_orders)

        cnx = self.connect_db("mysql")

        self.execute_cursor(cnx,"""create database if not exists aiq;""")
        self.execute_cursor(cnx,"""use aiq;""")

        conn = self.connect_db("sqlalchemy")
        
        for i in self.pkl_map['root']:
            self.execute_cursor(cnx,"""DROP TABLE IF EXISTS {};""".format(i['table_name']))
            self.execute_cursor(cnx,i['create_statement'])
            
        self.load_df_to_db(df_customer,conn,'customer')
        self.load_df_to_db(df_company,conn,'company')
        self.load_df_to_db(df_weather,conn,'weather')
        self.load_df_to_db(df_orders,conn,'orders')

        self.close_connect(cnx,"msql")
        self.close_connect(conn,"sqlalchemy")

if __name__ == "__main__":
    data_pipeline()