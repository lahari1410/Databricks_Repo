#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark


# In[2]:


findspark.init()


# In[3]:


import pyspark


# In[4]:


from pyspark.sql import SparkSession


# In[5]:


spark = SparkSession.builder.getOrCreate()


# In[6]:


df = spark.sql("select 'spark' as hello")
df.show()


# In[76]:


from pyspark.sql.types import StringType, StructField, StructType
fields = [
    StructField("id", StringType()),
    StructField("name", StringType()),
    StructField("json_data", StringType())]
Schema = StructType(fields)


df = spark.read.option("delimiter","|").csv("/Users/ycrag/Downloads/id,name,json_data.csv",Schema, header = True)


# In[45]:


df.show(truncate= False)


# In[82]:


from pyspark.sql.types import StringType, StructField, StructType, IntegerType
from pyspark.sql.functions import from_json

json_schema = StructType([\
                         StructField("age", IntegerType()),\
                         StructField("city", StringType())])

df1 = df.withColumn('new_col', from_json(df.json_data, json_schema))
df1.show(truncate = False)
df1.printSchema()


# In[54]:


df1.schema["new_col"].dataType


# In[63]:


from pyspark.sql.functions import explode
df2 =df1.select(df1.name)


# In[65]:


df2.show()


# In[67]:


df.show()


# In[72]:


arrayData = [
        ('James',['Java','Scala'],{'hair':'black','eye':'brown'}),
        ('Michael',['Spark','Java',None],{'hair':'brown','eye':None}),
        ('Robert',['CSharp',''],{'hair':'red','eye':''}),
        ('Washington',None,None),
        ('Jefferson',['1','2'],{})]

df3 = spark.createDataFrame(data=arrayData, schema = ['name','knownLanguages','properties'])
df3.printSchema()
df3.show(truncate = False)


# In[75]:


df4 = df3.select(df3.name,explode(df3.properties))
df4.printSchema()
df4.show()


# In[80]:


import pyspark.sql.utils


