from pyspark.sql.functions import count, when, isnan, isnull


def count_nulls(spark_df, sort=True):
    df = spark_df.select([count(when(isnan(c) | isnull(c), c)).alias(c) for (c, c_type) in spark_df.dtypes if
                          c_type not in ('timestamp', 'string', 'date', 'float')]).toPandas()

    if len(df) == 0:
        return 'No nulls found'

    if sort:
        return df.rename(index={0: 'count'}).T.sort_values("count", ascending=False)

    return df
