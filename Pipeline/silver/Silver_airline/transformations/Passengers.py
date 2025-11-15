import dlt
from pyspark.sql.functions import *
from utilities.utils import Transformation

expectations={'Rule1':'passenger_id is not null'}

@dlt.view(name='src_passengers')
@dlt.expect_all(expectations)
def src_passengers():
    df = spark.readStream.format('delta').load('/Volumes/project/bronze/bronze_files/Passengers/data/')
    df = (
        df.withColumn('modified_date', current_timestamp())
          .drop('_rescued_data')
    )
    return df

dlt.create_streaming_table("Passengers")

dlt.create_auto_cdc_flow(
    target="Passengers",
    source="src_passengers",  
    keys=["passenger_id"],
    sequence_by=col("modified_date"),
    stored_as_scd_type=1
)