from scripts import get_products_with_categories
from frames import products_df, categories_df, products_categories_df


result_df = get_products_with_categories(products_df, categories_df, products_categories_df)
result_df.show()