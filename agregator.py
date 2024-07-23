from pyspark.sql import DataFrame
from pyspark.sql import functions as func


def get_product_category_pairs(products_df, categories_df, product_category_df):
    """
    Возвращает датафрейм с парами "Имя продукта - Имя категории" и имена продуктов без категорий.

    Args:
        products_df (pyspark.sql.DataFrame): Датафрейм с продуктами.
        categories_df (pyspark.sql.DataFrame): Датафрейм с категориями.
        product_category_df (pyspark.sql.DataFrame): Датафрейм со связями между продуктами и категориями.

    Returns:
        pyspark.sql.DataFrame: Датафрейм с парами "Имя продукта - Имя категории" и именами продуктов без категорий.
    """

    product_category_joined_df = products_df.join(product_category_df, on="product_id") \
        .join(categories_df, on="category_id")

    product_category_pairs_df = product_category_joined_df.select("product_name", "category_name")

    products_without_categories_df = products_df.join(product_category_df, on="product_id", how="left_anti")

    result_df = product_category_pairs_df.union(
        products_without_categories_df.select("product_name", func.lit(None).alias("category_name")))

    return result_df

