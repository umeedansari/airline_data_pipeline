import dlt
from pyspark.sql.functions import *
from utilities.utils import Transformation

expectations={'Rule1':'flight_id is not null'}

@dlt.view(name='src_flights')
@dlt.expect_all(expectations)
def src_flights():
    df = spark.readStream.format('delta').load('/Volumes/project/bronze/bronze_files/Flights/data/')
    df = (
        df.withColumn('flight_date',to_date(col('flight_date')))
          .withColumn('modified_date', current_timestamp())
          .drop('_rescued_data')
    )
    return df

dlt.create_streaming_table("Flights")

dlt.create_auto_cdc_flow(
    target="Flights",
    source="src_flights",  
    keys=["flight_id"],
    sequence_by=col("modified_date"),
    stored_as_scd_type=1
)