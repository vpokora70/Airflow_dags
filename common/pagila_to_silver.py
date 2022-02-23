from pyspark.sql import SparkSession
import pyspark.sql.functions as F
# import functools

import psycopg2
from psycopg2 import Error
from hdfs import InsecureClient

import os
from datetime import date
from datetime import datetime

client = InsecureClient('http://127.0.0.1:50070', user='user')

c_year = datetime.now().strftime("%Y")
c_month = datetime.now().strftime("%Y-%m")

# root path to silver zone to store data
silver_path = '/silver/pagila'
analize_path = '/silver/pagila/analize'

# root path to bronze zone
bronze_path = '/bronze/pagila'

# list of dataset in bronze zone
list_tables = ['film_actor', 'address', 'city', 'actor', 'film_category', 'inventory', 'category', 'country',
               'customer',
               'language', 'film', 'payment', 'payment_p2020_02', 'payment_p2020_05', 'payment_p2020_04',
               'payment_p2020_03',
               'payment_p2020_06', 'rental', 'staff', 'store', 'payment_p2020_01']

# lit of path in bronze zone
list_bronze_path = []
# lit of path in silver zone
list_silver_path = []

# created list of path in bronze zone
for table in list_tables:
    list_bronze_path.append(os.path.join(f"{bronze_path}/{c_year}/{c_month}/{table}.csv"))
# print(list_bronze_path)

# created list of path on silver zove
for table in list_tables:
    list_silver_path.append(os.path.join(f"{silver_path}/{c_year}/{c_month}/{table}"))
# print(list_silver_path)
# dictionary  with paths in bronze zone
df_bronze_path = dict(zip(list_tables, list_bronze_path))
# print(df_bronze_path)
# dictionary  with paths in silver zone
df_sliver_path = dict(zip(list_tables, list_silver_path))
# print(df_sliver_path)

# Spark sesion initialization

spark = SparkSession.builder \
    .master('local') \
    .appName('lesson_13') \
    .getOrCreate()

# exstract from broze zone film_actor dataset
film_actor_df = spark.read.load(df_bronze_path['film_actor']
                                , header="true"
                                , inferSchema="true"
                                , format="csv")
# add metadata atributes to datafarme
film_actor_df = film_actor_df.withColumn('record_load_date', F.current_date())
film_actor_df = film_actor_df.withColumn('record_delete_date', F.lit('null'))
film_actor_df = film_actor_df.withColumn('sourse-data', F.lit(df_bronze_path['film_actor']))

# create folder to store film_actor_df in silver zone
client.makedirs(df_sliver_path['film_actor'])
# store film_actor_df in silver zove
film_actor_df.write.parquet(df_sliver_path['film_actor'], mode='overwrite')

#
address_df = spark.read.load(df_bronze_path['address']
                             , header="true"
                             , inferSchema="true"
                             , format="csv")
address_df = address_df.withColumn('record_load_date', F.current_date())
address_df = address_df.withColumn('record_delete_date', F.lit('null'))
address_df = address_df.withColumn('sourse-data', F.lit(df_bronze_path['address']))
client.makedirs(df_sliver_path['address'])
address_df.write.parquet(df_sliver_path['address'], mode='overwrite')

#
city_df = spark.read.load(df_bronze_path['city']
                          , header="true"
                          , inferSchema="true"
                          , format="csv")
city_df = city_df.withColumn('record_load_date', F.current_date())
city_df = city_df.withColumn('record_delete_date', F.lit('null'))
city_df = city_df.withColumn('sourse-data', F.lit(df_bronze_path['city']))
client.makedirs(df_sliver_path['city'])
city_df.write.parquet(df_sliver_path['city'], mode='overwrite')

#
actor_df = spark.read.load(df_bronze_path['actor']
                           , header="true"
                           , inferSchema="true"
                           , format="csv")
actor_df = actor_df.withColumn('record_load_date', F.current_date())
actor_df = actor_df.withColumn('record_delete_date', F.lit('null'))
actor_df = actor_df.withColumn('sourse-data', F.lit(df_bronze_path['actor']))
client.makedirs(df_sliver_path['actor'])
actor_df.write.parquet(df_sliver_path['actor'], mode='overwrite')

#
film_category_df = spark.read.load(df_bronze_path['film_category']
                                   , header="true"
                                   , inferSchema="true"
                                   , format="csv")
film_category_df = film_category_df.withColumn('record_load_date', F.current_date())
film_category_df = film_category_df.withColumn('record_delete_date', F.lit('null'))
film_category_df = film_category_df.withColumn('sourse-data', F.lit(df_bronze_path['film_category']))
client.makedirs(df_sliver_path['film_category'])
film_category_df.write.parquet(df_sliver_path['film_category'], mode='overwrite')

#
inventory_df = spark.read.load(df_bronze_path['inventory']
                               , header="true"
                               , inferSchema="true"
                               , format="csv")
inventory_df = inventory_df.withColumn('record_load_date', F.current_date())
inventory_df = inventory_df.withColumn('record_delete_date', F.lit('null'))
inventory_df = inventory_df.withColumn('sourse-data', F.lit(df_bronze_path['inventory']))
client.makedirs(df_sliver_path['inventory'])
inventory_df.write.parquet(df_sliver_path['inventory'], mode='overwrite')

#
category_df = spark.read.load(df_bronze_path['category']
                              , header="true"
                              , inferSchema="true"
                              , format="csv")
category_df = category_df.withColumn('record_load_date', F.current_date())
category_df = category_df.withColumn('record_delete_date', F.lit('null'))
category_df = category_df.withColumn('sourse-data', F.lit(df_bronze_path['category']))
client.makedirs(df_sliver_path['category'])
category_df.write.parquet(df_sliver_path['category'], mode='overwrite')

#
country_df = spark.read.load(df_bronze_path['country']
                             , header="true"
                             , inferSchema="true"
                             , format="csv")
country_df = country_df.withColumn('record_load_date', F.current_date())
country_df = country_df.withColumn('record_delete_date', F.lit('null'))
country_df = country_df.withColumn('sourse-data', F.lit(df_bronze_path['country']))
client.makedirs(df_sliver_path['country'])
country_df.write.parquet(df_sliver_path['country'], mode='overwrite')

#
customer_df = spark.read.load(df_bronze_path['customer']
                              , header="true"
                              , inferSchema="true"
                              , format="csv")
customer_df = customer_df.withColumn('record_load_date', F.current_date())
customer_df = customer_df.withColumn('record_delete_date', F.lit('null'))
customer_df = customer_df.withColumn('sourse-data', F.lit(df_bronze_path['customer']))
client.makedirs(df_sliver_path['customer'])
customer_df.write.parquet(df_sliver_path['customer'], mode='overwrite')

#
language_df = spark.read.load(df_bronze_path['language']
                              , header="true"
                              , inferSchema="true"
                              , format="csv")
language_df = language_df.withColumn('record_load_date', F.current_date())
language_df = language_df.withColumn('record_delete_date', F.lit('null'))
language_df = language_df.withColumn('sourse-data', F.lit(df_bronze_path['language']))
client.makedirs(df_sliver_path['language'])
language_df.write.parquet(df_sliver_path['language'], mode='overwrite')

#
film_df = spark.read.load(df_bronze_path['film']
                          , header="true"
                          , inferSchema="true"
                          , format="csv")
film_df = film_df.withColumn('record_load_date', F.current_date())
film_df = film_df.withColumn('record_delete_date', F.lit('null'))
film_df = film_df.withColumn('sourse-data', F.lit(df_bronze_path['film']))
client.makedirs(df_sliver_path['film'])
film_df.write.parquet(df_sliver_path['film'], mode='overwrite')

#
payment_p2020_01_df = spark.read.load(df_bronze_path['payment_p2020_01']
                                      , header="true"
                                      , inferSchema="true"
                                      , format="csv")
payment_p2020_01_df = payment_p2020_01_df.withColumn('record_load_date', F.current_date())
payment_p2020_01_df = payment_p2020_01_df.withColumn('record_delete_date', F.lit('null'))
payment_p2020_01_df = payment_p2020_01_df.withColumn('sourse-data', F.lit(df_bronze_path['payment_p2020_01']))
client.makedirs(df_sliver_path['payment_p2020_01'])
payment_p2020_01_df.write.parquet(df_sliver_path['payment_p2020_01'], mode='overwrite')

#

payment_p2020_02_df = spark.read.load(df_bronze_path['payment_p2020_02']
                                      , header="true"
                                      , inferSchema="true"
                                      , format="csv")
payment_p2020_02_df = payment_p2020_02_df.withColumn('record_load_date', F.current_date())
payment_p2020_02_df = payment_p2020_02_df.withColumn('record_delete_date', F.lit('null'))
payment_p2020_02_df = payment_p2020_02_df.withColumn('sourse-data', F.lit(df_bronze_path['payment_p2020_01']))
client.makedirs(df_sliver_path['payment_p2020_02'])
payment_p2020_02_df.write.parquet(df_sliver_path['payment_p2020_02'], mode='overwrite')

#
payment_p2020_03_df = spark.read.load(df_bronze_path['payment_p2020_03']
                                      , header="true"
                                      , inferSchema="true"
                                      , format="csv")
payment_p2020_03_df = payment_p2020_03_df.withColumn('record_load_date', F.current_date())
payment_p2020_03_df = payment_p2020_03_df.withColumn('record_delete_date', F.lit('null'))
payment_p2020_03_df = payment_p2020_03_df.withColumn('sourse-data', F.lit(df_bronze_path['payment_p2020_03']))
client.makedirs(df_sliver_path['payment_p2020_03'])
payment_p2020_03_df.write.parquet(df_sliver_path['payment_p2020_03'], mode='overwrite')

#

payment_p2020_04_df = spark.read.load(df_bronze_path['payment_p2020_04']
                                      , header="true"
                                      , inferSchema="true"
                                      , format="csv")
payment_p2020_04_df = payment_p2020_04_df.withColumn('record_load_date', F.current_date())
payment_p2020_04_df = payment_p2020_04_df.withColumn('record_delete_date', F.lit('null'))
payment_p2020_04_df = payment_p2020_04_df.withColumn('sourse-data', F.lit(df_bronze_path['payment_p2020_04']))
client.makedirs(df_sliver_path['payment_p2020_04'])
payment_p2020_04_df.write.parquet(df_sliver_path['payment_p2020_04'], mode='overwrite')

#
payment_p2020_05_df = spark.read.load(df_bronze_path['payment_p2020_05']
                                      , header="true"
                                      , inferSchema="true"
                                      , format="csv")
payment_p2020_05_df = payment_p2020_05_df.withColumn('record_load_date', F.current_date())
payment_p2020_05_df = payment_p2020_05_df.withColumn('record_delete_date', F.lit('null'))
payment_p2020_05_df = payment_p2020_05_df.withColumn('sourse-data', F.lit(df_bronze_path['payment_p2020_05']))
client.makedirs(df_sliver_path['payment_p2020_05'])
payment_p2020_05_df.write.parquet(df_sliver_path['payment_p2020_05'], mode='overwrite')

#
payment_p2020_06_df = spark.read.load(df_bronze_path['payment_p2020_06']
                                      , header="true"
                                      , inferSchema="true"
                                      , format="csv")
payment_p2020_06_df = payment_p2020_06_df.withColumn('record_load_date', F.current_date())
payment_p2020_06_df = payment_p2020_06_df.withColumn('record_delete_date', F.lit('null'))
payment_p2020_06_df = payment_p2020_06_df.withColumn('sourse-data', F.lit(df_bronze_path['payment_p2020_06']))
client.makedirs(df_sliver_path['payment_p2020_06'])
payment_p2020_06_df.write.parquet(df_sliver_path['payment_p2020_06'], mode='overwrite')

#
payment_df = spark.read.load(df_bronze_path['payment']
                             , header="true"
                             , inferSchema="true"
                             , format="csv")
payment_df = payment_df.withColumn('record_load_date', F.current_date())
payment_df = payment_df.withColumn('record_delete_date', F.lit('null'))
payment_df = payment_df.withColumn('sourse-data', F.lit(df_bronze_path['payment']))
payment_df = payment_p2020_01_df.union(payment_p2020_02_df).union(payment_p2020_03_df) \
    .union(payment_p2020_04_df).union(payment_p2020_05_df).union(payment_p2020_06_df)
client.makedirs(df_sliver_path['payment'])
payment_df.write.parquet(df_sliver_path['payment'], mode='overwrite') \
 \
#
rental_df = spark.read.load(df_bronze_path['rental']
                            , header="true"
                            , inferSchema="true"
                            , format="csv")
rental_df = rental_df.withColumn('record_load_date', F.current_date())
rental_df = rental_df.withColumn('record_delete_date', F.lit('null'))
rental_df = rental_df.withColumn('sourse-data', F.lit(df_bronze_path['rental']))
# convert date in string format to timstampe
rental_df = rental_df.withColumn('rental_date', F.to_timestamp(rental_df.rental_date))
rental_df = rental_df.withColumn('return_date', F.to_timestamp(rental_df.return_date))
rental_df = rental_df.withColumn('last_update', F.to_timestamp(rental_df.last_update))
client.makedirs(df_sliver_path['rental'])
rental_df.write.parquet(df_sliver_path['rental'], mode='overwrite')

#
staff_df = spark.read.load(df_bronze_path['staff']
                           , header="true"
                           , inferSchema="true"
                           , format="csv")
staff_df = staff_df.withColumn('record_load_date', F.current_date())
staff_df = staff_df.withColumn('record_delete_date', F.lit('null'))
staff_df = staff_df.withColumn('sourse-data', F.lit(df_bronze_path['staff']))
client.makedirs(df_sliver_path['staff'])
staff_df.write.parquet(df_sliver_path['staff'], mode='overwrite')

#

store_df = spark.read.load(df_bronze_path['store']
                           , header="true"
                           , inferSchema="true"
                           , format="csv")
store_df = store_df.withColumn('record_load_date', F.current_date())
store_df = store_df.withColumn('record_delete_date', F.lit('null'))
store_df = store_df.withColumn('sourse-data', F.lit(df_bronze_path['store']))
client.makedirs(df_sliver_path['store'])
store_df.write.parquet(df_sliver_path['store'], mode='overwrite')


#created list of folders where should save anzlized data
analize_floder =[1,2,3,4,5,6,7]

#path to main folder with analized data
analize_path ='/silver/pagila/analize'

list_analize_path=[]

#list of path on silver/analize
for folder in analize_floder:
    list_analize_path.append(os.path.join(f"{analize_path}/{c_year}/{folder}"))

#There ceated dict with  path, where will stored analized data
df_analize_path = dict(zip(analize_floder, list_analize_path))

#Create folders in hdfs to store analized data
for path in df_analize_path:
    client.makedirs(df_analize_path[path])


# 1.Вывести количество фильмов в каждой категории, отсортировать по убыванию

number_film_in_categoy_df =category_df\
    .join(film_category_df, category_df.category_id==film_category_df.category_id, 'inner' )\
    .select(category_df.name.alias('category_name'),
            film_category_df.film_id.alias('film_id')
           )\
    .groupby('category_name')\
    .count()\
    .sort(F.desc('count'))

#save datfarme to /silver/pagila/analize/year/folder
number_film_in_categoy_df.write.parquet(df_analize_path[1], mode = 'append')


# 2. Вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.

ten_actors_df = actor_df\
    .join(film_actor_df,actor_df.actor_id ==  film_actor_df.actor_id, 'inner' )\
    .join(film_df, film_actor_df.film_id == film_df.film_id, 'inner')\
    .join(inventory_df, film_df.film_id ==inventory_df.film_id, 'inner' )\
    .join(rental_df, inventory_df.inventory_id== rental_df.inventory_id, 'inner')\
    .filter(rental_df.return_date.isNotNull() & rental_df.rental_date.isNotNull())\
    .groupby(actor_df.actor_id, actor_df.first_name, actor_df.last_name)\
    .count()\
    .sort(F.desc('count'))

#save data farme
ten_actors_df.write.parquet(df_analize_path[2], mode = 'append')

#  3. вывести категорию фильмов, на которую потратили больше всего денег.

max_pay_cat_df = payment_df\
    .join(rental_df, payment_df.rental_id == rental_df.rental_id, 'inner')\
    .join(inventory_df, rental_df.inventory_id ==inventory_df.inventory_id, 'inner')\
    .join(film_df, inventory_df.film_id == film_df.film_id, 'inner')\
    .join(film_category_df, film_df.film_id == film_category_df.film_id, 'inner')\
    .join(category_df, film_category_df.category_id == category_df.category_id, 'inner')\
    .select(category_df.name, payment_df.amount.alias('amount'))\
    .groupBy(category_df.name).agg(F.sum('amount').alias('summa'))\
    .sort(F.desc('summa'))

max_pay_cat_df.write.parquet(df_analize_path[3], mode = 'append')

# 4. вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.

no_in_inventory_df = film_df\
    .join(inventory_df, film_df.film_id == inventory_df.film_id, 'left')\
    .select(inventory_df.film_id.alias('i_film_id'),
            film_df.title.alias('f_title'))\
    .filter(inventory_df.film_id.isNull())

no_in_inventory_df.write.parquet(df_analize_path[4], mode = 'append')


# 5. вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”.
children_actor_df = actor_df\
    .join(film_actor_df, actor_df.actor_id == film_actor_df.actor_id, 'inner')\
    .join(film_df, film_actor_df.film_id == film_df.film_id, 'inner')\
    .join(film_category_df, film_df.film_id == film_category_df.film_id, 'inner')\
    .join(category_df, film_category_df.category_id == category_df.category_id, 'inner')\
    .select(actor_df.first_name, actor_df.last_name, category_df.name)\
    .filter(category_df.name=='Children')\
    .groupby(actor_df.first_name, actor_df.last_name)\
    .count()\
    .sort(F.desc('count'))

children_actor_df.write.parquet(df_analize_path[5], mode = 'append')

#6. вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1).
#Отсортировать по количеству неактивных клиентов по убыванию

active_count_df = customer_df\
    .join(address_df, customer_df.address_id == address_df.address_id, 'inner')\
    .join(city_df, address_df.city_id == city_df.city_id, 'inner')\
    .filter(customer_df.active==1)\
    .select(city_df.city, customer_df.active)\
    .groupBy(city_df.city)\
    .count()\
    .sort(F.desc('count'))

active_count_df.write.parquet(df_analize_path[6], mode = 'append')

# 7. вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды
# в городах (customer.address_id в этом city), и которые начинаются на букву “a”.
# То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.

# Задачу решаем в два шага, выполянем join и формируем необходимый датасет
rent_df = customer_df\
    .join(address_df, customer_df.address_id == address_df.address_id, 'inner')\
    .join(city_df, address_df.city_id == city_df.city_id, 'inner')\
    .join(rental_df, customer_df.customer_id== rental_df.customer_id, 'inner')\
    .join(inventory_df, rental_df.inventory_id == inventory_df.inventory_id, 'inner')\
    .join(film_df, inventory_df.film_id == film_df.film_id, 'inner')\
    .join(film_category_df, film_df.film_id == film_category_df.film_id, 'inner')\
    .join(category_df, film_category_df.category_id == category_df.category_id, 'inner')\
    .withColumn("num_hours", rental_df.return_date - rental_df.rental_date)\
    .withColumn("num_days",F.datediff(rental_df.return_date, rental_df.rental_date))\
    .filter(rental_df.return_date.isNotNull() & rental_df.rental_date.isNotNull())\
    .select(city_df.city.alias('city'),
            address_df.address.alias('address'),
            category_df.name.alias('cat_name'),
            "num_days"
           )

#Фильтруем rent_df.city на содержание символа "-", и затем выводим датафрейм с категорией фильмов,
# у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city)

city_cat_film_max_rent_1_df=rent_df\
    .filter(rent_df.city.contains('-'))\
    .groupBy(rent_df.city,  rent_df.address, rent_df.cat_name)\
    .agg(F.sum(rent_df.num_days).alias('sum_days'))\
    .groupBy(rent_df.city,  rent_df.address, rent_df.cat_name)\
    .agg(F.max('sum_days').alias('max_sum_days'))\
    .sort(F.desc(rent_df.address))

city_cat_film_max_rent_1_df.write.parquet(df_analize_path[7], mode = 'append')

#Фильтруем rent_df.city на содержание внчален названия города символ "a", и затем выводим датафрейм с
# категорией фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city)

city_cat_film_max_rent_2_df = rent_df\
    .filter(rent_df.city.like('a%'))\
    .groupBy(rent_df.city,  rent_df.address, rent_df.cat_name)\
    .agg(F.sum(rent_df.num_days).alias('sum_days'))\
    .groupBy(rent_df.city,  rent_df.address, rent_df.cat_name)\
    .agg(F.max('sum_days').alias('max_sum_days'))\
    .sort(F.desc(rent_df.address))

city_cat_film_max_rent_2_df.write.parquet(df_analize_path[7], mode = 'append')

