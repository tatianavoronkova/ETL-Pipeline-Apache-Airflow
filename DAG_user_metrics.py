# coding=utf-8

from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from io import StringIO
import requests
import os

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Настройки подключения к БД
connection = {
    'host': os.getenv('HOST_NAME'),
    'database': os.getenv('DATABASE'),
    'user': os.getenv('USER_NAME'),
    'password': os.getenv('PASSWORD')
}

# Подключение к БД тест для загрузки
connection_test = {'host': os.getenv('HOST_ST'),
                      'database': os.getenv('DATABASE_ST'),
                      'user': os.getenv('USER_ST'), 
                      'password': os.getenv('PASSWORD_ST')
                     }

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': os.getenv('OWNER_AIR'),
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 7, 10),
}

# Интервал запуска DAG
schedule_interval = '@daily'

# Вчерашняя дата в формате  YYYY-MM-DD
yesterday_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_tvoronkova():

    @task()
    def extract_feed():
        q_feed = """
        select user_id, gender, age, os, countIf(action='view') as views, countIf(action='like') as likes
        from simulator_20240520.feed_actions
        where toDate(time) = yesterday()
        group by user_id, gender, age, os
        """
        
        df_feed = ph.read_clickhouse(q_feed, connection=connection)
        return df_feed

    @task()
    def extract_message():
        
        q_message = """
        select *
        from
        (select user_id, gender, age, os, messages_sent, users_sent, messages_received, users_received
        from
        (select user_id, gender, age, os, count(receiver_id) as messages_sent, countDistinct(receiver_id) as users_sent
        from simulator_20240520.message_actions
        where toDate(time) = yesterday()
        group by user_id, gender, age, os) tt1
        left join
        (select receiver_id, messages_received, users_received, gender, age, os
        from
        (select receiver_id, count(user_id) as messages_received, countDistinct(user_id) as users_received
        from simulator_20240520.message_actions
        where toDate(time) = yesterday()
        group by receiver_id) t1
        left join
        (select DISTINCT user_id, gender, age, os
        from simulator_20240520.message_actions) t2 on t1.receiver_id = t2.user_id) tt2
        on tt1.user_id = tt2.receiver_id

        union distinct

        select tt2.receiver_id as user_id, tt2.gender as gender, tt2.age as age, tt2.os as os, messages_sent, users_sent, messages_received, users_received
        from
        (select user_id, gender, age, os, count(receiver_id) as messages_sent, countDistinct(receiver_id) as users_sent
        from simulator_20240520.message_actions
        where toDate(time) = yesterday()
        group by user_id, gender, age, os) tt1
        right join
        (select receiver_id, messages_received, users_received, gender, age, os
        from
        (select receiver_id, count(user_id) as messages_received, countDistinct(user_id) as users_received
        from simulator_20240520.message_actions
        where toDate(time) = yesterday()
        group by receiver_id) t1
        left join
        (select *
        from 
        (select DISTINCT user_id, gender, age, os
        from simulator_20240520.message_actions
        union distinct
        select DISTINCT user_id, gender, age, os
        from simulator_20240520.feed_actions)) t2 on t1.receiver_id = t2.user_id) tt2
        on tt1.user_id = tt2.receiver_id)
        """
        df_message = ph.read_clickhouse(q_message, connection=connection)
        return df_message
    
    @task
    def union_data(df_feed, df_message):
        df_final = pd.concat([df_feed, df_message]).fillna(0)
        return df_final
    
    @task
    def transfrom_gender(df_final):
        df_gender = df_final[['gender', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]\
            .groupby(['gender'])\
            .sum()\
            .reset_index()
        df_gender.insert(0, 'dimension', 'gender')
        df_gender.insert(0, 'event_date', yesterday_date)
        df_gender = df_gender.rename(columns = {'gender':'dimension_value'})
        df_gender['dimension_value'] = df_gender['dimension_value'].astype(str) 
        return df_gender

    @task
    def transfrom_age(df_final):
        df_age = df_final[['age', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]\
            .groupby(['age'])\
            .sum()\
            .reset_index()
        df_age.insert(0, 'dimension', 'age')
        df_age.insert(0, 'event_date', yesterday_date)
        df_age = df_age.rename(columns = {'age':'dimension_value'})
        df_age['dimension_value'] = df_age['dimension_value'].astype(str) 
        return df_age

    @task
    def transfrom_os(df_final):
        df_os = df_final[['os', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]\
            .groupby(['os'])\
            .sum()\
            .reset_index()
        df_os.insert(0, 'dimension', 'os')
        df_os.insert(0, 'event_date', yesterday_date)
        df_os = df_os.rename(columns = {'os':'dimension_value'})
        df_os['dimension_value'] = df_os['dimension_value'].astype(str) 
        return df_os
    
    @task
    def load(df_gender, df_age, df_os):
        context = get_current_context()
        ts = context['ts']
        #загрузка среза по полу
        print(f'Metrics by gender were successfuly uploaded to database {ts}')
        ph.to_clickhouse(df=df_gender, table=os.getenv('TABLE_ST'), index=False, connection=connection_test)
        #загрузка среза по возрасту
        print(f'Metrics by age were successfuly uploaded to database {ts}')
        ph.to_clickhouse(df=df_age, table=os.getenv('TABLE_ST'), index=False, connection=connection_test)
        #загрузка среза по ОС
        print(f'Metrics by OS were successfuly uploaded to database {ts}')
        ph.to_clickhouse(df=df_os, table=os.getenv('TABLE_ST'), index=False, connection=connection_test)

    df_feed = extract_feed()
    df_message = extract_message()
    df_final = union_data(df_feed, df_message)
    df_gender = transfrom_gender(df_final)
    df_age = transfrom_age(df_final)
    df_os = transfrom_os(df_final)
    load(df_gender, df_age, df_os)

dag_tvoronkova = dag_tvoronkova()
