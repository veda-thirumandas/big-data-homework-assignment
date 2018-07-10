

```python
# Credentials are hidden
import ibmos2spark

```

# Read Review Data
Read the review data from round 11 of the Yelp Dataset challenge. The 5.2 million reviews in this data file include every review from the following 11 cities, except those reviews that Yelp has filtered as not being recommended. 

* Las Vegas, NV
* Phoenix, AZ
* Madison, WI
* Urbana-Champagne, IL
* Cleveland, OH
* Pittsburgh, PA
* Charlotte, NC
* Montreal (Canada)
* Quebec (Canada)
* Edinburg (U.K.)
* Karlsruhe, (Germany)



```python

# Please read the documentation of PySpark to learn more about the possibilities to load data files.
# PySpark documentation: https://spark.apache.org/docs/2.0.1/api/python/pyspark.sql.html#pyspark.sql.SparkSession
# The SparkSession object is already initialized for you.
# The following variable contains the path to your file on your IBM Cloud Object Storage.
path_reviews = cos.url('review.json.bz2', 'spring2018veda7abe03040343431897343076417c41c8')
df_reviews = spark.read.json(path_reviews)
print "number of reviews:", df_reviews.count()
df_reviews.printSchema()
df_reviews.show(5)
df_reviews.select("business_id", "user_id", "review_id").createOrReplaceTempView("reviews")


```

    number of reviews: 5261669
    root
     |-- business_id: string (nullable = true)
     |-- cool: long (nullable = true)
     |-- date: string (nullable = true)
     |-- funny: long (nullable = true)
     |-- review_id: string (nullable = true)
     |-- stars: long (nullable = true)
     |-- text: string (nullable = true)
     |-- useful: long (nullable = true)
     |-- user_id: string (nullable = true)
    
    +--------------------+----+----------+-----+--------------------+-----+--------------------+------+--------------------+
    |         business_id|cool|      date|funny|           review_id|stars|                text|useful|             user_id|
    +--------------------+----+----------+-----+--------------------+-----+--------------------+------+--------------------+
    |0W4lkclzZThpx3V65...|   0|2016-05-28|    0|v0i_UHJMo_hPBq9bx...|    5|Love the staff, l...|     0|bv2nCi5Qv5vroFiqK...|
    |AEx2SYEUJmTxVVB18...|   0|2016-05-28|    0|vkVSCC7xljjrAI4UG...|    5|Super simple plac...|     0|bv2nCi5Qv5vroFiqK...|
    |VR6GpWIda3SfvPC-l...|   0|2016-05-28|    0|n6QzIUObkYshz4dz2...|    5|Small unassuming ...|     0|bv2nCi5Qv5vroFiqK...|
    |CKC0-MOWMqoeWf6s-...|   0|2016-05-28|    0|MV3CcKScW05u5LVfF...|    5|Lester's is locat...|     0|bv2nCi5Qv5vroFiqK...|
    |ACFtxLv8pGrrxMm6E...|   0|2016-05-28|    0|IXvOzsEMYtiJI0CAR...|    4|Love coming here....|     0|bv2nCi5Qv5vroFiqK...|
    +--------------------+----+----------+-----+--------------------+-----+--------------------+------+--------------------+
    only showing top 5 rows
    


# Read User Data
This data includes every user that wrote a review in any of the 11 cities in the review data. 
Please note, the review data does not include every review from each of these users. 




```python
path_user = cos.url('user.json.bz2', 'spring2018veda7abe03040343431897343076417c41c8')
df_users = spark.read.json(path_user)
print "number of users:", df_users.count()
df_users.printSchema()
df_users.show(5, truncate=False)
df_users.select("review_count","user_id").createOrReplaceTempView("users")
```

    number of users: 1326101
    root
     |-- average_stars: double (nullable = true)
     |-- compliment_cool: long (nullable = true)
     |-- compliment_cute: long (nullable = true)
     |-- compliment_funny: long (nullable = true)
     |-- compliment_hot: long (nullable = true)
     |-- compliment_list: long (nullable = true)
     |-- compliment_more: long (nullable = true)
     |-- compliment_note: long (nullable = true)
     |-- compliment_photos: long (nullable = true)
     |-- compliment_plain: long (nullable = true)
     |-- compliment_profile: long (nullable = true)
     |-- compliment_writer: long (nullable = true)
     |-- cool: long (nullable = true)
     |-- elite: array (nullable = true)
     |    |-- element: long (containsNull = true)
     |-- fans: long (nullable = true)
     |-- friends: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- funny: long (nullable = true)
     |-- name: string (nullable = true)
     |-- review_count: long (nullable = true)
     |-- useful: long (nullable = true)
     |-- user_id: string (nullable = true)
     |-- yelping_since: string (nullable = true)
    
    +-------------+---------------+---------------+----------------+--------------+---------------+---------------+---------------+-----------------+----------------+------------------+-----------------+----+-----+----+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----+------+------------+------+----------------------+-------------+
    |average_stars|compliment_cool|compliment_cute|compliment_funny|compliment_hot|compliment_list|compliment_more|compliment_note|compliment_photos|compliment_plain|compliment_profile|compliment_writer|cool|elite|fans|friends                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |funny|name  |review_count|useful|user_id               |yelping_since|
    +-------------+---------------+---------------+----------------+--------------+---------------+---------------+---------------+-----------------+----------------+------------------+-----------------+----+-----+----+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----+------+------------+------+----------------------+-------------+
    |4.67         |0              |0              |0               |0             |0              |0              |0              |0                |1               |0                 |0                |0   |[]   |0   |[cvVMmlU1ouS3I5fhutaryQ, nj6UZ8tdGo8YJ9lUMTVWNw, RTtdEVhAmeWqCSp0IgJ99w, t3UKA1sl4e6LY_xsjuvI0A, s057_BvOfnKNvQquJf7VNg, VYrdepCgdzJ4WaxP7dBGpg, XXLSk6sQQDyr3dZ4zE-O0g, Py8ThfExQaXF2Woqr7kWUw, 233YNvzVtZ1ObkaNkUzNIw, L6iE9NpmHHJQTk0JQlRlSA, Y7XTMgZ_q5Bj5f9KhK1R4Q]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |0    |Johnny|8           |0     |oMy_rEb0UBEmMlu-zcxnoQ|2014-11-03   |
    |3.7          |0              |0              |0               |0             |0              |0              |0              |0                |0               |0                 |0                |0   |[]   |0   |[0njfJmB-7n84DlIgUByCNw, rFn3Xe3RqHxRSxWOU19Gpg, HVUAmApa0fCbHHVJ0ALshw, LBOTb6bJjCdFyDLNswUGmA, cy3d0moQOsrhWo6VAyA_kA, XTDeelhFzFX0h_8YELJDIQ, _erxdUfB8yJA_MCDBAaD_w, 3wMYVoHy15nxOCLZYYIhqA, 74C8Mdn3ISlFIwNZQUPEdg, Vc980a_kTQL9tzfG1ESq_g, UPld_8xdzvYmIMA4YxtG0A, ggppqaSt-4E-Y2XUVv6mMw, vILp0ctTM3fX0ucJeBw9Hg, Gl46kwumd4_f102dxgswDw, i9CDu8j-qcUfZYl67WBwiw, 23lcgZUt6dvtMqV_AZe9KQ, zacONW61-GjaCF9h5_icgA, BclKLmGPYlMEVr3OZ_Uuzw, ol00uKESTC5PR_fETT27AA, o7e8zecvSlPNTrPi1ZQLzg, 00JBHX0npVAC-itaFOUhpQ, im3w0wt-ZeE-lDa1zSdyMg, J_hT3mbtiwHIEWjTBSJnzQ, _54jmZK9vr9CN-SUSjFbMg, 65w6GtrNNlZDowVDAYGZFQ, WJeipqBVdQmVhnYrXlLb7A, vAhyONgVtU-oIaSHRJW0IA, Soa5S7dQsSjlG3lbTVGb2A, xtVcWcIKVORcBKhmdbrFiA, 7CghpxENlXnjTGOyNtRDsg, jI5mX0tNR79J_qoFKZMPdA, MFtiq9lbm95WVg501wNsww, SCICexHST-LMCowvH9h7jQ, SD2VfVLfSkz8m18dvo0HGA, Y8TLRIPm_ecnZPItgAJ3Kg, t9bfJI2zvx-3eFrN2zvQnw, 6yUCCifCroUPfXaVo5fCAA, SGDa-z6MmLmNCamviRsdsA, aZiDtUBOMHh19IsShZvqJg, nT9_vJSUk_Tay2Yx84rzCg, M98bD_uUoC0SJ39keA4OQw, hT3kGKYFnLflIifxyeKWwg, loEGz0M-bBf0XnyjrbEvFQ, Als68VuGzyG3rbn7aLkdBQ, X1Rx-CRMt3BjtW10xSb3Lw, svdwrwuL9YHyF3rOgjbB1w, T0jSTc2uvGtzIHZ843FcHQ, XAWO5aDTxo-LTE4jVdBdVg, Q1dLA0wWGO390tH56uKA4Q, -Ty7mzu-bxd4NmEprFXHEA, LQKbKXzjGtRRTK6v00o4gg, BLnaah22WBlLCDwOC7rJ8Q, IWlU5scMqNaKSWO5ovY_iA, kMPSklg0XL9uYeYUgX2oCg, EUH2OBcw32X7wv6Yg4oC_w, Rkm94t2H5p2aPihBzhqP7w, MetDJXD8zSj9J5FqTYB_3Q, TyJ4xokX87KzbNUS8GX85A, t8H01Z2Vx9GNU8uXVKxr2Q, gv28x4ugM5FHGI_Jkky4kg, Y-AnP_QFDcq6B9BoDUUs0A, PXutYstbsqF3kgj6Tuik7Q, G0XMrknewP2Y3iHt84qZiA, gfYOmCKPDni8L3Dd_jibNQ, M9FL-chYhMA4nZpmTbWIZQ, zdahkrcKI1Mdj7H0i6oJcQ, vl3qiO6v4RbFJBpxVREJPw, RzbEU8OO2wbx67v_005Vlg, T1LHbOLMsvSjgqAJXQJwxA, 3na1OAZAe44z06SVPY_gPA, DwOyBVeoiZP7B4ZNkgbVbw, f-ZbVPvuUIuGGE1cGcTRTQ, 6FmeOxLVL0DVr7KHBC1kVA, BjN4WYNWk1t8qipjCqMJfA, 7rwANCcxfWgZ7K3LNBcQlw, 1kZ3jeyfPDAq7t7sIb89yg, TDo7fo6qn8OjokmvyZzPxw, ZJsUtNX4UXrQNL7ddEegUQ, J-sGGuafu3SFAWlw5D0xBw]|0    |Chris |10          |0     |JJ-aSuM4pCFPdkfoZ34q0Q|2013-09-24   |
    |2.0          |0              |0              |0               |0             |0              |0              |0              |0                |0               |0                 |0                |0   |[]   |0   |[]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |0    |Tiffy |1           |0     |uUzsFQn_6cXDh6rPNGbIFA|2017-03-02   |
    |4.67         |0              |0              |0               |0             |0              |0              |0              |0                |0               |0                 |0                |0   |[]   |0   |[]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |0    |Mark  |6           |0     |mBneaEEH5EMyxaVyqS-72A|2015-03-13   |
    |4.67         |0              |0              |0               |0             |0              |0              |0              |0                |0               |0                 |0                |0   |[]   |0   |[]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |0    |Evelyn|3           |0     |W5mJGs-dcDWRGEhAzUYtoA|2016-09-08   |
    +-------------+---------------+---------------+----------------+--------------+---------------+---------------+---------------+-----------------+----------------+------------------+-----------------+----+-----+----+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----+------+------------+------+----------------------+-------------+
    only showing top 5 rows
    



```python
from pyspark.sql import functions
df_filtered_reviews=df_reviews.select(df_reviews.review_id,df_reviews.user_id,df_reviews.business_id,\
                                      df_reviews.stars,functions.length(df_reviews.text).alias('review_length'))
df_filtered_reviews.show(5)


```

    +--------------------+--------------------+--------------------+-----+-------------+
    |           review_id|             user_id|         business_id|stars|review_length|
    +--------------------+--------------------+--------------------+-----+-------------+
    |v0i_UHJMo_hPBq9bx...|bv2nCi5Qv5vroFiqK...|0W4lkclzZThpx3V65...|    5|          289|
    |vkVSCC7xljjrAI4UG...|bv2nCi5Qv5vroFiqK...|AEx2SYEUJmTxVVB18...|    5|          213|
    |n6QzIUObkYshz4dz2...|bv2nCi5Qv5vroFiqK...|VR6GpWIda3SfvPC-l...|    5|          502|
    |MV3CcKScW05u5LVfF...|bv2nCi5Qv5vroFiqK...|CKC0-MOWMqoeWf6s-...|    5|          373|
    |IXvOzsEMYtiJI0CAR...|bv2nCi5Qv5vroFiqK...|ACFtxLv8pGrrxMm6E...|    4|          523|
    +--------------------+--------------------+--------------------+-----+-------------+
    only showing top 5 rows
    



```python
df_filtered_users=df_users.select(df_users.user_id,df_users.name,df_users.review_count,df_users.average_stars,\
                                 df_users.yelping_since,(functions.size(df_users.elite)>0).alias('is_elite'))
df_filtered_users.show(5)
```

    +--------------------+------+------------+-------------+-------------+--------+
    |             user_id|  name|review_count|average_stars|yelping_since|is_elite|
    +--------------------+------+------------+-------------+-------------+--------+
    |oMy_rEb0UBEmMlu-z...|Johnny|           8|         4.67|   2014-11-03|   false|
    |JJ-aSuM4pCFPdkfoZ...| Chris|          10|          3.7|   2013-09-24|   false|
    |uUzsFQn_6cXDh6rPN...| Tiffy|           1|          2.0|   2017-03-02|   false|
    |mBneaEEH5EMyxaVyq...|  Mark|           6|         4.67|   2015-03-13|   false|
    |W5mJGs-dcDWRGEhAz...|Evelyn|           3|         4.67|   2016-09-08|   false|
    +--------------------+------+------------+-------------+-------------+--------+
    only showing top 5 rows
    



```python
df_agg_rev=df_filtered_reviews.groupBy('user_id').agg({'stars':'avg','review_length':'avg','review_id':'count'})
df_agg_rev.show(5)

df_agg_reviews=df_agg_rev.select('user_id','avg(stars)','avg(review_length)','count(review_id)')\
.toDF('user_id','dataset_avg_stars','avg_review_length','dataset_review_count')
df_agg_reviews.show(5)


```

    +--------------------+----------------+-----------------+------------------+
    |             user_id|count(review_id)|       avg(stars)|avg(review_length)|
    +--------------------+----------------+-----------------+------------------+
    |oKWVVqPWVzq5s6nS4...|              42|4.190476190476191|370.14285714285717|
    |OcB8_br3QOUuX_CCg...|               3|              4.0|233.66666666666666|
    |5a7nRCjFm_QUjF8xy...|               1|              1.0|             408.0|
    |DsPTtxXzLccDutfoJ...|               9|              5.0|             447.0|
    |Y1N3yo62V5eIj_Zcn...|               1|              5.0|              95.0|
    +--------------------+----------------+-----------------+------------------+
    only showing top 5 rows
    
    +--------------------+-----------------+------------------+--------------------+
    |             user_id|dataset_avg_stars| avg_review_length|dataset_review_count|
    +--------------------+-----------------+------------------+--------------------+
    |oKWVVqPWVzq5s6nS4...|4.190476190476191|370.14285714285717|                  42|
    |OcB8_br3QOUuX_CCg...|              4.0|233.66666666666666|                   3|
    |5a7nRCjFm_QUjF8xy...|              1.0|             408.0|                   1|
    |DsPTtxXzLccDutfoJ...|              5.0|             447.0|                   9|
    |Y1N3yo62V5eIj_Zcn...|              5.0|              95.0|                   1|
    +--------------------+-----------------+------------------+--------------------+
    only showing top 5 rows
    



```python
df_users_reviews=df_filtered_users.join(df_agg_reviews,'user_id','left')\
.select(df_filtered_users.user_id,'name','review_count','average_stars',\
       'yelping_since','is_elite','dataset_avg_stars','avg_review_length','dataset_review_count')
df_users_reviews.show(5)
```

    +--------------------+-------------+------------+-------------+-------------+--------+-----------------+-----------------+--------------------+
    |             user_id|         name|review_count|average_stars|yelping_since|is_elite|dataset_avg_stars|avg_review_length|dataset_review_count|
    +--------------------+-------------+------------+-------------+-------------+--------+-----------------+-----------------+--------------------+
    |--CJT4d-S8UhwqHe0...|        Scott|           3|          2.0|   2015-08-18|   false|              1.0|           1956.0|                   1|
    |-0Ji0nOyFe-4yo8BK...|         cubs|          57|         3.28|   2009-05-15|   false|              4.0|             64.0|                   1|
    |-0XPr1ilUAfp-yIXZ...|Fairmount Jil|           3|         1.25|   2007-12-26|   false|              2.0|           1849.0|                   1|
    |-1KKYzibGPyUX-Mwk...|       Nickie|         163|          4.0|   2009-08-07|    true|              4.0|            510.0|                   1|
    |-1zQA2f_syMAdA04P...|         Zach|           4|          3.0|   2014-06-22|   false|              3.0|            289.5|                   4|
    +--------------------+-------------+------------+-------------+-------------+--------+-----------------+-----------------+--------------------+
    only showing top 5 rows
    



```python
df_dataset_percentage=df_users_reviews.select('user_id','name','review_count','average_stars','yelping_since',\
                                             'is_elite','dataset_avg_stars','avg_review_length','dataset_review_count',\
                                             (df_users_reviews.dataset_review_count/df_users_reviews.review_count)\
                                             .alias('dataset_percentage'))
df_dataset_percentage.show()
```

    +--------------------+-------------+------------+-------------+-------------+--------+------------------+------------------+--------------------+--------------------+
    |             user_id|         name|review_count|average_stars|yelping_since|is_elite| dataset_avg_stars| avg_review_length|dataset_review_count|  dataset_percentage|
    +--------------------+-------------+------------+-------------+-------------+--------+------------------+------------------+--------------------+--------------------+
    |--CJT4d-S8UhwqHe0...|        Scott|           3|          2.0|   2015-08-18|   false|               1.0|            1956.0|                   1|  0.3333333333333333|
    |-0Ji0nOyFe-4yo8BK...|         cubs|          57|         3.28|   2009-05-15|   false|               4.0|              64.0|                   1|0.017543859649122806|
    |-0XPr1ilUAfp-yIXZ...|Fairmount Jil|           3|         1.25|   2007-12-26|   false|               2.0|            1849.0|                   1|  0.3333333333333333|
    |-1KKYzibGPyUX-Mwk...|       Nickie|         163|          4.0|   2009-08-07|    true|               4.0|             510.0|                   1|0.006134969325153374|
    |-1zQA2f_syMAdA04P...|         Zach|           4|          3.0|   2014-06-22|   false|               3.0|             289.5|                   4|                 1.0|
    |-2Pb5d2WBPtbyGT_b...|        mindy|           5|          3.2|   2009-04-29|   false|               5.0|             285.0|                   1|                 0.2|
    |-2mPrKWc9UYdvTrOZ...|         Gary|           1|          5.0|   2016-08-21|   false|               5.0|             499.0|                   1|                 1.0|
    |-3bsS2i9xqjNnIA1f...|          Kim|          15|         2.47|   2012-04-10|   false|               2.5|            1520.5|                   2| 0.13333333333333333|
    |-3i9bhfvrM3F1wsC9...|        Linda|         604|         4.06|   2005-08-07|    true|3.4285714285714284|             574.5|                  14|0.023178807947019868|
    |-4Anvj46CWf57KWI9...|       Cookie|           2|          3.5|   2016-08-17|   false|               3.5|             459.0|                   2|                 1.0|
    |-4xyc3OgPwrLshmqH...|         Sean|          38|         4.68|   2016-06-05|   false|               3.0|             128.0|                   1| 0.02631578947368421|
    |-55DgUo52I3zW9Rxk...|         John|          37|         4.46|   2013-04-11|   false| 4.681818181818182| 210.8181818181818|                  22|  0.5945945945945946|
    |-7JSlmBJKUQwREG_y...|    Bénédicte|           8|          4.5|   2015-06-02|   false| 4.333333333333333| 419.6666666666667|                   6|                0.75|
    |-7V6r0PLuBlFVjbLJ...|      Michael|           4|          4.0|   2015-01-01|   false|               1.0|            1062.0|                   1|                0.25|
    |-7fIoUgPboApCfkG0...|       Ashley|           4|         3.25|   2015-08-10|   false|               1.5|             751.0|                   2|                 0.5|
    |-897i_JdWyDsXGUa8...|      Michael|           7|         3.14|   2010-12-04|   false|               2.0|             380.5|                   2|  0.2857142857142857|
    |-8_yETBp70WiqqN-A...|         Lisa|          32|         4.32|   2008-09-29|   false|3.3333333333333335|             561.0|                   3|             0.09375|
    |-9aO6i4fmPWrLyW4i...|       Hudson|           1|          3.0|   2012-07-25|   false|               3.0|             291.0|                   1|                 1.0|
    |-9aj4KK4lLzoP33E5...|       Bianca|           2|          5.0|   2014-07-25|   false|               5.0|             196.0|                   2|                 1.0|
    |-9da1xk7zgnnfO1uT...|         Fran|         930|          4.1|   2012-04-05|    true|  3.74468085106383|1588.2978723404256|                  47| 0.05053763440860215|
    +--------------------+-------------+------------+-------------+-------------+--------+------------------+------------------+--------------------+--------------------+
    only showing top 20 rows
    


# Read Business data
This data includes all data related to the businesses within the 2017 Yelp dataset. Also created a temporary view from the business data called business. 


```python

# Please read the documentation of PySpark to learn more about the possibilities to load data files.
# PySpark documentation: https://spark.apache.org/docs/2.0.1/api/python/pyspark.sql.html#pyspark.sql.SparkSession
# The SparkSession object is already initialized for you.
# The following variable contains the path to your file on your IBM Cloud Object Storage.
path_business = cos.url('business.json.bz2', 'spring2018veda7abe03040343431897343076417c41c8')
df_business= spark.read.json(path_business)
df_business.show()
df_business.select("business_id","name","state","city","is_open","review_count","categories").createOrReplaceTempView("business")

```

    +--------------------+--------------------+--------------------+--------------------+--------------+--------------------+-------+-------------+--------------+--------------------+------------------+-----------+------------+-----+-----+
    |             address|          attributes|         business_id|          categories|          city|               hours|is_open|     latitude|     longitude|                name|      neighborhood|postal_code|review_count|stars|state|
    +--------------------+--------------------+--------------------+--------------------+--------------+--------------------+-------+-------------+--------------+--------------------+------------------+-----------+------------+-----+-----+
    |4855 E Warner Rd,...|[true,null,null,n...|FYWN1wneV18bWNgQj...|[Dentists, Genera...|     Ahwatukee|[7:30-17:00,7:30-...|      1|   33.3306902|  -111.9785992|    Dental by Design|                  |      85044|          22|  4.0|   AZ|
    |  3101 Washington Rd|[null,null,null,n...|He-G7vWjzVUysIKrf...|[Hair Stylists, H...|      McMurray|[9:00-16:00,9:00-...|      1|   40.2916853|   -80.1048999| Stephen Szabo Salon|                  |      15317|          11|  3.0|   PA|
    |6025 N 27th Ave, ...|[null,null,null,n...|KQPW8lFf1y5BT2Mxi...|[Departments of M...|       Phoenix|[null,null,null,n...|      1|   33.5249025|  -112.1153098|Western Motor Veh...|                  |      85017|          18|  1.5|   AZ|
    |5000 Arizona Mill...|[null,null,null,n...|8DShNS-LuFqpEWIp0...|[Sporting Goods, ...|         Tempe|[10:00-21:00,10:0...|      0|   33.3831468|  -111.9647254|    Sports Authority|                  |      85282|           9|  3.0|   AZ|
    |        581 Howe Ave|[null,null,full_b...|PfOCPjBrlQAnz__NX...|[American (New), ...|Cuyahoga Falls|[11:00-1:00,11:00...|      1|   41.1195346|   -81.4756898|Brick House Taver...|                  |      44221|         116|  3.5|   OH|
    |      Richterstr. 11|[null,null,beer_a...|o9eMRCWt5PkpLDE0g...|[Italian, Restaur...|     Stuttgart|[18:00-0:00,18:00...|      1|      48.7272|       9.14795|             Messina|                  |      70567|           5|  4.0|   BW|
    |2620 Regatta Dr, ...|[null,null,null,n...|kCoE3jvEtg6UVz5SO...|[Real Estate Serv...|     Las Vegas|[8:00-17:00,8:00-...|      1|     36.20743|    -115.26846|          BDJ Realty|         Summerlin|      89128|           5|  4.0|   NV|
    |7240 W Lake Mead ...|[null,null,null,n...|OD2hnuuTJI9uotcKy...|[Shopping, Sporti...|     Las Vegas|[11:00-19:00,11:0...|      1|   36.1974844|  -115.2496601|         Soccer Zone|                  |      89128|           9|  1.5|   NV|
    |2612 Brandt Schoo...|[null,null,null,n...|EsMcGiZaQuG1OOvL9...|[Coffee & Tea, Ic...|       Wexford|[null,null,null,n...|      1|40.6151022445|-80.0913487465|    Any Given Sundae|                  |      15090|          15|  5.0|   PA|
    |                    |[null,null,null,n...|TGWhGNusxyMaA4kQV...|[Automotive, Auto...|     Henderson|[9:00-18:00,9:00-...|      1|36.0558252127| -115.04635039|Detailing Gone Mo...|                  |      89014|           7|  5.0|   NV|
    |    737 West Pike St|[null,null,none,n...|XOSRcvtaKc_Q5H1SA...|[Breakfast & Brun...|       Houston|[null,null,null,n...|      0|40.2415480142|-80.2128151059|   East Coast Coffee|                  |      15342|           3|  4.5|   PA|
    |2414 South Gilber...|[null,null,null,n...|Y0eMNa5C-YU1RQOZf...|[Local Services, ...|      Chandler|[9:30-18:00,9:30-...|      1|   33.2717201|  -111.7912569|CubeSmart Self St...|                  |      85286|          23|  5.0|   AZ|
    |    35 Main Street N|[null,null,null,n...|xcgFnd-MwkZeO5G2H...|[Bakeries, Bagels...|       Markham|[null,null,null,n...|      1|   43.8751774|   -79.2601532|T & T Bakery and ...|   Markham Village|    L3P 1X3|          38|  4.0|   ON|
    |    107 Whitaker Str|[true,null,null,n...|NmZtoE3v8RdSJEczY...|[General Dentistr...|     Homestead|[8:00-17:00,8:00-...|      1|   40.4014882|   -79.8879161|Complete Dental Care|                  |      15120|           5|  2.0|   PA|
    |        600 E 4th St|[null,null,null,[...|fNMVV_ZX7CJSDWQGd...|[Restaurants, Ame...|     Charlotte|[7:00-15:00,7:00-...|      1|   35.2216474|   -80.8393449|Showmars Governme...|            Uptown|      28202|           7|  3.5|   NC|
    |       2459 Yonge St|[null,null,full_b...|l09JfMeQ6ynYs5MCJ...|[Italian, French,...|       Toronto|[9:00-22:00,9:00-...|      0|   43.7113993|   -79.3993388|      Alize Catering|Yonge and Eglinton|    M4P 2H6|          12|  3.0|   ON|
    |8411 W Thunderbir...|[null,null,null,n...|IQSlT5jGE6CCDhSG0...|[Beauty & Spas, N...|        Peoria|[9:00-19:00,9:00-...|      1|   33.6086538|  -112.2400118|      T & Y Nail Spa|                  |      85381|          20|  3.0|   AZ|
    |    2518 Ironwood Dr|[null,null,null,n...|b2I2DXtZVnpUMCXp1...|[Tires, Oil Chang...|   Sun Prairie|[7:30-18:00,7:30-...|      1|     43.18508|    -89.262047|Meineke Car Care ...|                  |      53590|           9|  3.5|   WI|
    |    13375 W McDowell|[null,null,null,n...|0FMKDOU8TJT1x87OK...|[Barbers, Beauty ...|      Goodyear|[9:00-19:00,9:00-...|      1|    33.463629|   -112.347038|Senior's Barber Shop|                  |      85395|          65|  5.0|   AZ|
    |9665 Bayview Aven...|[null,null,full_b...|Gu-xs3NIQTj3Mj2xY...|[French, Food, Ba...| Richmond Hill|[11:30-23:00,11:3...|      1|   43.8675648|   -79.4126618|Maxim Bakery & Re...|                  |    L4C 9V4|          34|  3.5|   ON|
    +--------------------+--------------------+--------------------+--------------------+--------------+--------------------+-------+-------------+--------------+--------------------+------------------+-----------+------------+-----+-----+
    only showing top 20 rows
    


# How many businesses are there in each of the nine North American Cities?

- Created a subquery to combine South Carolina and North Carolina since they are a part of the same Metropolitan area. 
    * The subquery creates a temporary table with the outputs of each state
    * Summing the subquery's output gives us the total for NC and SC combined. 
- Finally we counted how many distinct overall businesses there are in the nine North American Cities.


```python
spark.sql("""
SELECT state,
SUM(number_of_businesses) as number_of_businesses
FROM
(SELECT (CASE when state = "NC" or state = "SC" then "NC" else state end) as state,
        COUNT(DISTINCT name) as number_of_businesses
        FROM business
        WHERE 
            state = "AZ" OR
            state = "IL" OR
            state = "NV" OR
            state = "OH" OR
            state = "ON" OR
            state = "PA" OR
            state = "QC" OR
            state = "SC" OR
            state = "NC" 
        GROUP BY state)
GROUP BY state
""").show()

```

    +-----+--------------------+
    |state|number_of_businesses|
    +-----+--------------------+
    |   AZ|               40197|
    |   QC|                7089|
    |   NV|               26309|
    |   NC|               10734|
    |   IL|                1654|
    |   OH|                9732|
    |   PA|                8189|
    |   ON|               23741|
    +-----+--------------------+
    


#  How many continuing locals are there in the data set?       
* Created a temporary view for data frame for continuing users from the dataset percentage dataframe
* The dataset percentage dataframe holds data from the users data so we got a count of each name
* We defined locals as users who had at least 10 reviews and had 50% of their reviews and then filtered the data to only include those


```python
df_dataset_percentage.createOrReplaceTempView("continuing_users")
spark.sql("""
SELECT COUNT(user_id) as continuing_users
FROM continuing_users
WHERE 
     review_count>= 10 AND dataset_percentage>0.5
""").show()
```

    +----------------+
    |continuing_users|
    +----------------+
    |           88301|
    +----------------+
    


# How many reviews are there in each of the nine cities?
- Created a subquery to combine South Carolina and North Carolina since they are a part of the same Metropolitan area. 
    * The subquery creates a temporary table with the outputs of each state
    * Summing the subquery's output gives us the total for NC and SC combined. 
- Finally we counted how many reviews there are in the nine North American cities.


```python
df_filtered_states = spark.sql("""
SELECT *,
    CASE
        WHEN state = "SC" THEN "NC"
        ELSE
          state
    END AS s
FROM business
WHERE     
    state = "AZ" OR
    state = "IL" OR
    state = "WI" OR
    state = "NV" OR
    state = "OH" OR
    state = "ON" OR
    state = "PA" OR
    state = "QC" OR
    state = "SC" OR
    state = "NC" 
""")

df_filtered_states.show()
```

    +--------------------+--------------------+-----+--------------+-------+------------+--------------------+---+
    |         business_id|                name|state|          city|is_open|review_count|          categories|  s|
    +--------------------+--------------------+-----+--------------+-------+------------+--------------------+---+
    |FYWN1wneV18bWNgQj...|    Dental by Design|   AZ|     Ahwatukee|      1|          22|[Dentists, Genera...| AZ|
    |He-G7vWjzVUysIKrf...| Stephen Szabo Salon|   PA|      McMurray|      1|          11|[Hair Stylists, H...| PA|
    |KQPW8lFf1y5BT2Mxi...|Western Motor Veh...|   AZ|       Phoenix|      1|          18|[Departments of M...| AZ|
    |8DShNS-LuFqpEWIp0...|    Sports Authority|   AZ|         Tempe|      0|           9|[Sporting Goods, ...| AZ|
    |PfOCPjBrlQAnz__NX...|Brick House Taver...|   OH|Cuyahoga Falls|      1|         116|[American (New), ...| OH|
    |kCoE3jvEtg6UVz5SO...|          BDJ Realty|   NV|     Las Vegas|      1|           5|[Real Estate Serv...| NV|
    |OD2hnuuTJI9uotcKy...|         Soccer Zone|   NV|     Las Vegas|      1|           9|[Shopping, Sporti...| NV|
    |EsMcGiZaQuG1OOvL9...|    Any Given Sundae|   PA|       Wexford|      1|          15|[Coffee & Tea, Ic...| PA|
    |TGWhGNusxyMaA4kQV...|Detailing Gone Mo...|   NV|     Henderson|      1|           7|[Automotive, Auto...| NV|
    |XOSRcvtaKc_Q5H1SA...|   East Coast Coffee|   PA|       Houston|      0|           3|[Breakfast & Brun...| PA|
    |Y0eMNa5C-YU1RQOZf...|CubeSmart Self St...|   AZ|      Chandler|      1|          23|[Local Services, ...| AZ|
    |xcgFnd-MwkZeO5G2H...|T & T Bakery and ...|   ON|       Markham|      1|          38|[Bakeries, Bagels...| ON|
    |NmZtoE3v8RdSJEczY...|Complete Dental Care|   PA|     Homestead|      1|           5|[General Dentistr...| PA|
    |fNMVV_ZX7CJSDWQGd...|Showmars Governme...|   NC|     Charlotte|      1|           7|[Restaurants, Ame...| NC|
    |l09JfMeQ6ynYs5MCJ...|      Alize Catering|   ON|       Toronto|      0|          12|[Italian, French,...| ON|
    |IQSlT5jGE6CCDhSG0...|      T & Y Nail Spa|   AZ|        Peoria|      1|          20|[Beauty & Spas, N...| AZ|
    |b2I2DXtZVnpUMCXp1...|Meineke Car Care ...|   WI|   Sun Prairie|      1|           9|[Tires, Oil Chang...| WI|
    |0FMKDOU8TJT1x87OK...|Senior's Barber Shop|   AZ|      Goodyear|      1|          65|[Barbers, Beauty ...| AZ|
    |Gu-xs3NIQTj3Mj2xY...|Maxim Bakery & Re...|   ON| Richmond Hill|      1|          34|[French, Food, Ba...| ON|
    |lHYiCS-y8AFjUitv6...|           Starbucks|   ON|       Toronto|      1|          21|[Food, Coffee & Tea]| ON|
    +--------------------+--------------------+-----+--------------+-------+------------+--------------------+---+
    only showing top 20 rows
    



```python
df_filtered_states.createOrReplaceTempView("filtered_states")

df_reviews_by_state=spark.sql("""
SELECT state, SUM(review_count) AS number_of_reviews
FROM filtered_states
GROUP BY state
""")

df_reviews_by_state.show()

df_reviews_by_state.select("state","number_of_reviews").createOrReplaceTempView("state_review_count")

```

    +-----+-----------------+
    |state|number_of_reviews|
    +-----+-----------------+
    |   AZ|          1627693|
    |   SC|            10857|
    |   QC|           146379|
    |   NV|          1824387|
    |   WI|           109737|
    |   NC|           307426|
    |   IL|            36378|
    |   OH|           243677|
    |   PA|           229804|
    |   ON|           634263|
    +-----+-----------------+
    


# Do some cities have a higher share of reviews written by locals who are continuing Yelpers?

We combined user, review, and business data in order to attach state data to users. This allowed us to make continuing user data accessible for each metro area

# Finding all the reviews written by local users

We used the dataset_percentage dataframe to make a new dataframe listing all the local users and their review counts. 


```python
df_dataset_percentage.createOrReplaceTempView("dataset_percentage")
df_local_users = spark.sql("""
SELECT user_id, dataset_review_count, dataset_percentage
FROM dataset_percentage 
WHERE dataset_review_count >= 10 AND dataset_percentage > 0.5
""")
df_local_users.show()

df_local_users.createOrReplaceTempView("local_users")
```

    +--------------------+--------------------+------------------+
    |             user_id|dataset_review_count|dataset_percentage|
    +--------------------+--------------------+------------------+
    |-55DgUo52I3zW9Rxk...|                  22|0.5945945945945946|
    |-NnADpikTs6IWM9tb...|                  21|0.7241379310344828|
    |-dErbI4sHSkRz6oxj...|                  28|0.6086956521739131|
    |-vSUDEQB9j2DaP8_4...|                  25|0.7575757575757576|
    |0-UxxxWLz1muOzPx2...|                  29|0.6744186046511628|
    |0BMTo253SdSZpmZbA...|                  12|0.6666666666666666|
    |0LUt7xSvYgLpL0zBe...|                  16|0.9411764705882353|
    |0M4x1qpXH1-Ssuepp...|                  20|0.7142857142857143|
    |1CHxxE-_z3yLsoHq4...|                  16|               1.0|
    |1Q-tjPYDBaFZJouj8...|                  20|0.7407407407407407|
    |1Qo3Pw-XRPaaNfpGP...|                  54|0.8709677419354839|
    |1dI8Gi0M_6RM0ylnA...|                  12|               0.8|
    |1oBWrKCDvH221hueH...|                  30|0.8571428571428571|
    |2F51bDSLFFgkdPOzu...|                  17|               1.0|
    |2ICzZcgDT0s0RhXhT...|                  10|             0.625|
    |2Ly_E_OZnJu8-fmpQ...|                  35|0.7777777777777778|
    |2OtKt_7AZkfPXioU3...|                  29|0.5686274509803921|
    |2yPRCQMd2u0ka7r5I...|                  17|              0.85|
    |3DwWwdLEty3JcsIz6...|                  18|               0.9|
    |3GYH9ET1VsK359obD...|                  19|0.8636363636363636|
    +--------------------+--------------------+------------------+
    only showing top 20 rows
    



```python
print df_local_users.count()
```

    67846


# Joining Business data with Reviews and Users
At this point we figured out that we needed a common point in which we could connect all our temp views and dataframe. We knew we had to start we reviews as it was the "middle man" between business and users. So we did just that we connect business_id, review_id, user_id. This would allow us to get closer to finding the percentage of total local users by state.


```python
df_business_reviews=spark.sql("""
SELECT business.business_id, state, user_id, review_id
FROM business, reviews
WHERE business.business_id = reviews.business_id
""")

df_business_reviews.show()

df_business_reviews.createOrReplaceTempView("business_reviews")

```

    +--------------------+-----+--------------------+--------------------+
    |         business_id|state|             user_id|           review_id|
    +--------------------+-----+--------------------+--------------------+
    |0W4lkclzZThpx3V65...|   QC|bv2nCi5Qv5vroFiqK...|v0i_UHJMo_hPBq9bx...|
    |AEx2SYEUJmTxVVB18...|   QC|bv2nCi5Qv5vroFiqK...|vkVSCC7xljjrAI4UG...|
    |VR6GpWIda3SfvPC-l...|   QC|bv2nCi5Qv5vroFiqK...|n6QzIUObkYshz4dz2...|
    |CKC0-MOWMqoeWf6s-...|   QC|bv2nCi5Qv5vroFiqK...|MV3CcKScW05u5LVfF...|
    |ACFtxLv8pGrrxMm6E...|   QC|bv2nCi5Qv5vroFiqK...|IXvOzsEMYtiJI0CAR...|
    |s2I_Ni76bjJNK9yG6...|   QC|bv2nCi5Qv5vroFiqK...|L_9BTb55X0GDtThi6...|
    |8QWPlVQ6D-OExqXoa...|   NV|_4iMDXbXZ1p1ONG29...|HRPm3vEZ_F-33TYVT...|
    |9_CGhHMz8698M9-Pk...|   ON|u0LXt3Uea_GidxRW1...|ymAUG8DZfQcFTBSOi...|
    |gkCorLgPyQLsptTHa...|   ON|u0LXt3Uea_GidxRW1...|8UIishPUD92hXtScS...|
    |5r6-G9C4YLbC7Ziz5...|   ON|u0LXt3Uea_GidxRW1...|w41ZS9shepfO3uEyh...|
    |fDF_o2JPU8BR1Gya-...|   ON|u0LXt3Uea_GidxRW1...|WF_QTN3p-thD74hqp...|
    |z8oIoCT1cXz7gZP5G...|   ON|u0LXt3Uea_GidxRW1...|PIsUSmvaUWB00qv5K...|
    |XWTPNfskXoUL-Lf32...|   ON|u0LXt3Uea_GidxRW1...|PdZ_uFjbbkjtm3SCY...|
    |13nKUHH-uEUXVZylg...|   ON|u0LXt3Uea_GidxRW1...|x5oV6wm9_Pb1QQ6jk...|
    |RtUvSWO_UZ8V3Wpj0...|   ON|u0LXt3Uea_GidxRW1...|lsoSqIrrDbQvWpMvs...|
    |Aov96CM4FZAXeZvKt...|   ON|u0LXt3Uea_GidxRW1...|23eqwlZzCWZkADWfd...|
    |0W4lkclzZThpx3V65...|   QC|u0LXt3Uea_GidxRW1...|FunI9om-aK5oMIIJm...|
    |fdnNZMk1NP7ZhL-YM...|   ON|u0LXt3Uea_GidxRW1...|FKu4iU62EmWT6GZXP...|
    |PFPUMF38-lraKzLcT...|   ON|u0LXt3Uea_GidxRW1...|xdu8nXrbNKeaywCX7...|
    |oWTn2IzrprsRkPfUL...|   ON|u0LXt3Uea_GidxRW1...|K7o5jDInfmX3cY5oH...|
    +--------------------+-----+--------------------+--------------------+
    only showing top 20 rows
    


# Joining combined Business+ Reviews dataframe with Local Users dataframe
Once we joined business and review data, we had a user_id to create a join with the local users dataframe from earlier. This created a dataframe with all the reviews written by local users. 


```python
df_business_user_join= spark.sql("""
SELECT business_id, state, local_users.user_id, review_id
FROM business_reviews, local_users
WHERE business_reviews.user_id = local_users.user_id
""")
df_business_user_join.show()

df_business_user_join.createOrReplaceTempView("join_business_user")
```

    +--------------------+-----+--------------------+--------------------+
    |         business_id|state|             user_id|           review_id|
    +--------------------+-----+--------------------+--------------------+
    |WXR0ND0KqbArMZDvF...|   AZ|-55DgUo52I3zW9Rxk...|N_FpnV61L6-6Uf-FV...|
    |3qlqzQrwh8hjBltlg...|   AZ|-55DgUo52I3zW9Rxk...|NFgnyyp2Kl1nlWIEk...|
    |nn4ZkIZTCd7jJhxs-...|   AZ|-55DgUo52I3zW9Rxk...|zqwxKCkD2PfTRRft5...|
    |RezqChEv5DPJC85d2...|   AZ|-55DgUo52I3zW9Rxk...|dqY4HJ4GMwaHsTZUB...|
    |_e3rChyednoh47n2c...|   AZ|-55DgUo52I3zW9Rxk...|KW_RA0roCySRdIHCP...|
    |Mypk1XBD6PQ77zLXy...|   AZ|-55DgUo52I3zW9Rxk...|9m6mrz0QsbHKHiyM_...|
    |F-QY0xyNi9z1p0wjZ...|   AZ|-55DgUo52I3zW9Rxk...|vyUmRDInsAeAvocHV...|
    |qrK73viBBJY_hx3bg...|   AZ|-55DgUo52I3zW9Rxk...|_pMYiNeCJtFbCLLVF...|
    |9O-L6F0cMfNmE5i07...|   AZ|-55DgUo52I3zW9Rxk...|iOTTR1df4vYAtE9Wn...|
    |T-KniGykrZ46ZC9pl...|   AZ|-55DgUo52I3zW9Rxk...|EtVmDwvpG4BLkorpK...|
    |DaVTuhzi6EgWStb2e...|   AZ|-55DgUo52I3zW9Rxk...|QKqwHltxRwC4xC6XX...|
    |LIxNfkk9vgPikLh1W...|   AZ|-55DgUo52I3zW9Rxk...|fd7I18mSy4FaK86ZO...|
    |wHq1efQVz17338k_a...|   AZ|-55DgUo52I3zW9Rxk...|efP_rFGCPoW2eJpSN...|
    |C3VnuPP_flttZPokg...|   AZ|-55DgUo52I3zW9Rxk...|hOgdNEOE0BYNvs6WV...|
    |twoxa0_9sn6ZN4j40...|   AZ|-55DgUo52I3zW9Rxk...|vSelOm7k5YbueR9rq...|
    |VuYF1vGKLjEFaQfL4...|   AZ|-55DgUo52I3zW9Rxk...|p1KItC5NgZEvkJksF...|
    |ZxA3HG1kxD-0ZzKue...|   AZ|-55DgUo52I3zW9Rxk...|LwkOtEQs1PGL3f2GO...|
    |6t98--hqg8suYkm_3...|   AZ|-55DgUo52I3zW9Rxk...|jLA6Qk10sLHpaHYRR...|
    |po-05-AGCVxEme-Sb...|   AZ|-55DgUo52I3zW9Rxk...|3mSDhpX_tGGTuT1au...|
    |3awTUGMdUVrwEBkFF...|   AZ|-55DgUo52I3zW9Rxk...|TlmdmL1IgVfKQ6BpU...|
    +--------------------+-----+--------------------+--------------------+
    only showing top 20 rows
    


# Finding Local Users by State
We already found the aggregated sum of total local users for all states, but for this assignment we have to find local users and assign them to their respective states, to then be able to find the percentage of local reviews from by each city.


```python
df_continuing_users_by_state=spark.sql("""
SELECT state,
SUM(number_of_local_reviews) as number_of_local_reviews
FROM
    (SELECT (CASE when state = "NC" or state = "SC" then "NC" else state end) as state,
        COUNT(*) as number_of_local_reviews
        FROM join_business_user
WHERE 
    state = "AZ" OR
    state = "IL" OR
    state = "NV" OR
    state = "WI" OR
    state = "OH" OR
    state = "ON" OR
    state = "PA" OR
    state = "QC" OR
    state = "SC" OR
    state = "NC" 
GROUP BY state)
GROUP BY state
""")

df_continuing_users_by_state.show()

```

    +-----+-----------------------+
    |state|number_of_local_reviews|
    +-----+-----------------------+
    |   AZ|                 684274|
    |   QC|                  54528|
    |   NV|                 564190|
    |   WI|                  33140|
    |   NC|                 107315|
    |   IL|                   6654|
    |   OH|                  83512|
    |   PA|                  76107|
    |   ON|                 357677|
    +-----+-----------------------+
    



```python
df_continuing_users_by_state.createOrReplaceTempView("continuing_users_by_state")
```


```python
df_compare_local= spark.sql("""
SELECT state_review_count.state, state_review_count.number_of_reviews,continuing_users_by_state.number_of_local_reviews
FROM state_review_count, continuing_users_by_state
WHERE state_review_count.state = continuing_users_by_state.state
""")
df_compare_local.show()

```

    +-----+-----------------+-----------------------+
    |state|number_of_reviews|number_of_local_reviews|
    +-----+-----------------+-----------------------+
    |   AZ|          1627693|                 684274|
    |   QC|           146379|                  54528|
    |   NV|          1824387|                 564190|
    |   WI|           109737|                  33140|
    |   NC|           307426|                 107315|
    |   IL|            36378|                   6654|
    |   OH|           243677|                  83512|
    |   PA|           229804|                  76107|
    |   ON|           634263|                 357677|
    +-----+-----------------+-----------------------+
    



```python
df_compare_local.createOrReplaceTempView("compare_local")

spark.sql("""
SELECT *, (number_of_local_reviews/number_of_reviews)*100 AS percentage_of_local_reviews
FROM compare_local
""").show()
```

    +-----+-----------------+-----------------------+---------------------------+
    |state|number_of_reviews|number_of_local_reviews|percentage_of_local_reviews|
    +-----+-----------------+-----------------------+---------------------------+
    |   AZ|          1627693|                 684274|         42.039500077717356|
    |   QC|           146379|                  54528|          37.25124505564323|
    |   NV|          1824387|                 564190|            30.924907927978|
    |   WI|           109737|                  33140|         30.199476931208252|
    |   NC|           307426|                 107315|          34.90758751699596|
    |   IL|            36378|                   6654|         18.291274946396175|
    |   OH|           243677|                  83512|          34.27159723732646|
    |   PA|           229804|                  76107|           33.1182224852483|
    |   ON|           634263|                 357677|          56.39253748050888|
    +-----+-----------------+-----------------------+---------------------------+
    


# Limitations
Although we were able to achieve a main understanding of what the percentage of each metro area's reviews were written by users who are continuing locals, it is evident that there are limitations of the analysis. The main limitation is involved with who is considered a local user and what metro area they are actually from. We are not guaranteed that the user is local in the metro area because we are using an equation, rather than explicit facts. It is possible that the user may have been on vacation, has moved, or only uses yelp when they are in a certain metro area.
