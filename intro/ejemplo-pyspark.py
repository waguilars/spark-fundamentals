from pyspark.sql import SparkSession
sc = SparkSession.builder.master("local[3]").appName('spark-app').getOrCreate()

data = [1,2,3,4,5,2,3,4,5,2,3,4,5,2,3,4,5]

rdd = sc.sparkContext.parallelize(data)
print(rdd.count())


df = sc.sparkContext.textFile('data.csv')
# print(df.collect())

df2 = sc.read.options(delimiter=';', inferSchema=True, header=True).csv('data.csv')
df2.printSchema()

# result = df2.select('PRODUCT NAME', 'PRICE')
# result

df3 = sc.read.options(delimiter=',', inferSchema=True, header=True).csv('2016_Accidentes Tránsito_BDD.csv')
df3.printSchema()

print(df3.select('provincia', 'canton').limit(5).collect())