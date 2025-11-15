from pyspark.sql import DataFrame
from pyspark.sql.functions import row_number, desc
from pyspark.sql.window import Window

class Transformation:
    def dedup(self, df: DataFrame, dedup_col) -> DataFrame:
        window_spec = Window.partitionBy(dedup_col).orderBy(desc(dedup_col))
        df = df.withColumn('row_num', row_number().over(window_spec))
        deduped_df = df.filter(df.row_num == 1).drop('row_num')
        return deduped_df
