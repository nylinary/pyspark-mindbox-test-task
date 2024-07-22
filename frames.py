from pyspark.sql.session import SparkSession
from pyspark.sql import types


session: SparkSession = SparkSession.builder.appName("Products").getOrCreate()

# Initial data
products = [(1, "milk"),(2, "bread"), (3, "cheese")]
categories = [(1, "dairy"), (2,"bakery"), (3, "sea_food")]
products_categories = [(1,1),  (2,2),]

# Schemas
products_schema = types.StructType(
    [
    types.StructField(name="id", dataType=types.IntegerType(), nullable=True),
    types.StructField(name="name", dataType=types.StringType(), nullable=True),
    ]
)
pc_schema = types.StructType(
    [
    types.StructField(name="product_id", dataType=types.IntegerType(), nullable=False),
    types.StructField(name="category_id", dataType=types.IntegerType(), nullable=False),
    ]
)

# Dataframes
products_df = session.createDataFrame(products, schema=products_schema)
categories_df = session.createDataFrame(categories, schema=products_schema)

products_categories_df = session.createDataFrame(products_categories, schema=pc_schema)