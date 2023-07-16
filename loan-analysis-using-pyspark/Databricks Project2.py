import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql.functions import *


#Create Dataframe
df=spark.read.load('/FileStore/tables/loan.csv',format='csv',sep=',',header='true',escape='"',inferSchema='true')

# Check the count of loaded file
df.count()

#Check the loaded data of df
df.show(2)

#Print the Schema
df.printSchema()

#Change the Datatype and Cleaning of data
from pyspark.sql.functions import regexp_replace,col
df = df.withColumn("Debt",regexp_replace(col("Debt"),"[,]",""))\
       .withColumn("Debt",col("Debt").cast("Int"))



df.show(2)

#Create the Temp view of dataframe
df.createOrReplaceTempView("Loan")

#Select the view
%sql
select * from Loan


# Loan Category
 %sql

 select distinct `Loan Category` from Loan   --Here column Loan Category is having space so have used Backtick symbol
 order by `Loan Category` asc


# Education Loan per gender
 %sql

 select count(case when gender='MALE' then '1' ELSE NULL END) as Male,
        count(case when gender='FEMALE' then '2' ELSE NULL END) as Female,

 `Loan Category` from Loan
 where `Loan Category`='EDUCATIONAL LOAN'
 group by 3


# Total Loan for AGRICULTURE
 %sql

 select
 `Loan Category`, sum(loan) from Loan
 where UPPER(`Loan Category`)='EDUCATIONAL LOAN'
 GROUP BY 1;


# Count the no. of records where income is zero/null
 %sql

 select count(*) from Loan
 where coalesce(income,'NA')='NA'


# Records for each age group with minimum income and loan
 %sql

 select * from (
 select age,income, loan,row_number() over (partition by age  order by income asc) as rn
 from Loan where income is not null) where rn=1;
