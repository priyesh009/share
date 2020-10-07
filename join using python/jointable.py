import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Test').getOrCreate()

#Reading Table 1
df_dim=spark.read.csv('it_dim.csv',header=True)
df_dim.show()

#Reading Table 2
df_fact=spark.read.csv('it_fact.csv',header=True)
df_fact.show()

# joining table using pysparkSQL
df_fact.createOrReplaceTempView('t_fact')
df_dim.createOrReplaceTempView('t_dim')
df_joined = spark.sql("select eid,name,city,group,d.brid,brname from t_fact f, t_dim d where f.brid=d.brid")
df_joined.show()

#joining using pyspark join function.

joined=df_fact.join(df_dim, df_fact.brid == df_dim.brid)
joined.show()