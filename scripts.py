from pyspark.sql import DataFrame


def get_products_with_categories(
        products: DataFrame, categories: DataFrame, products_categories: DataFrame
    ) -> DataFrame:
    products = products.withColumnsRenamed({"id": "product_id", "name": "product_name"})
    categories =  categories.withColumnsRenamed({"id": "category_id", "name": "category_name"})

    joined = products.join(
        products_categories, on="product_id", how="left"
    ).join(categories, on="category_id", how="left")

    return joined.select("product_name", "category_name").orderBy("category_name", ascending=False)

