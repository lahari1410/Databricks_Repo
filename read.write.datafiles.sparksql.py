# Databricks notebook source
df_sp = spark.read.format("json").load("/FileStore/tables/utilization.json").write.mode("overwrite").saveAsTable("Utilization")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from Utilization

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from Utilization

# COMMAND ----------

# MAGIC %sql
# MAGIC select session_count, cpu_utilization from Utilization
# MAGIC order by session_count
# MAGIC limit 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select session_count, server_id from Utilization 
# MAGIC where session_count < 34 
# MAGIC order by server_id
# MAGIC limit 10;

# COMMAND ----------

df_tb1 = spark.read.format("csv").option("header","true").load("/FileStore/tables/location_temp.csv")

# COMMAND ----------

df_tb1.write.mode("overwrite").saveAsTable("Location")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Location
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select server_id, count(server_id) from Utilization 
# MAGIC where session_count>70
# MAGIC group by server_id
# MAGIC order by count(server_id) desc;

# COMMAND ----------

df_ser = spark.read.format("csv").option("header","true").load("/FileStore/tables/server_name.csv")

# COMMAND ----------

df_ser.show()

# COMMAND ----------

df_ser.write.mode("overwrite").saveAsTable("Server_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct u.server_id, st.server_name
# MAGIC from Utilization u 
# MAGIC join Server_table st 
# MAGIC on u.server_id = st.server_id
# MAGIC

# COMMAND ----------

df_ser.describe().show()

# COMMAND ----------

df_tb1.describe().show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select server_id, min(cpu_utilization), max(cpu_utilization), stddev(cpu_utilization) from Utilization
# MAGIC group by server_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select server_id, cpu_utilization, event_datetime,
# MAGIC avg(cpu_utilization) over (partition by server_id) as avg_server_util
# MAGIC from utilization
