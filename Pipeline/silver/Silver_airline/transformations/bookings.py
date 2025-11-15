import dlt
from pyspark.sql.functions import *
from utilities.utils import Transformation

expectations={'Rule1':'booking_id is not null'}

@dlt.view(name='src_bookings')
@dlt.expect_all(expectations)
def src_bookings():
    df = spark.readStream.format('delta').load('/Volumes/project/bronze/bronze_files/Bookings/data/')
    df = (
        df.withColumn('booking_date', to_date(col('booking_date')))
          .withColumn('amount', col('amount').cast('float'))
          .withColumn('modified_date', current_timestamp())
          .drop('_rescued_data')
    )
    return df

dlt.create_streaming_table("Bookings")

dlt.create_auto_cdc_flow(
    target="Bookings",
    source="src_bookings",  
    keys=["booking_id"],
    sequence_by=col("modified_date"),
    stored_as_scd_type=1
)