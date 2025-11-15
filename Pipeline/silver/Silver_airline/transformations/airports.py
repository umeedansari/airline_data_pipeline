import dlt
from pyspark.sql.functions import *
from utilities.utils import Transformation

expectations={'Rule1':'airport_id is not null'}

@dlt.view(name='src_airports')
@dlt.expect_all(expectations)
def src_airports():
    df = spark.readStream.format('delta').load('/Volumes/project/bronze/bronze_files/Airports/data/')
    df = (
        df.withColumn('modified_date', current_timestamp())
          .drop('_rescued_data')
    )
    return df

dlt.create_streaming_table("Airports")

dlt.create_auto_cdc_flow(
    target="Airports",
    source="src_airports",  
    keys=["airport_id"],
    sequence_by=col("modified_date"),
    stored_as_scd_type=1
)