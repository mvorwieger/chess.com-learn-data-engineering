from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def orders_products(spark: SparkSession, mysql):
    products = spark.read.format("mongo").load()
    products.show()

    # In[8]:


    mysql_connection = mysql.connector.connect(user="order", password="order", host="orders_mysql",
                                               database="orders_database")

    # In[9]:


    cursor = mysql_connection.cursor()

    # In[10]:


    cursor.execute("select * from orders_database.order")
    rows = cursor.fetchall()
    names = cursor.column_names


    orders_raw = []
    for row in rows:
        obj = {}
        for i in range(len(names)):
            obj[names[i]] = row[i]
        orders_raw.append(obj)


    orders = spark.createDataFrame(orders_raw)
    orders.show()

    products = products.withColumn("product_id", products.product_id.cast("int").cast("string"))
    products.show()

    orders.join(products, orders.product_id == products.product_id).filter(orders.amount > 100).select(
        (orders.amount * products.price).alias('total')).agg(F.avg('total').alias('profit')).show()
