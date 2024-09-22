import argparse
import typing
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.jdbc import ReadFromJdbc
from apache_beam import coders
from apache_beam.io import WriteToText
from datetime import datetime
import examples.pg_to_oracle.oci_bucket_module as oci_bucket_module

class Trip(typing.NamedTuple):
    tpep_pickup: str
    tpep_dropoff: str
    passenger_count: int
    trip_distance: float
    store_and_fwd_flag: str
    payment_type: int
    fare_amount: float
    extra: float
    mta_tax: float
    tip_amount: float
    total_amount: float


def run_beam_pipeline(argv=None):
  options = PipelineOptions(argv=argv)
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  # row = typing.NamedTuple('row', Trip)
  coders.registry.register_coder(Trip, coders.RowCoder)
  
  with beam.Pipeline(options=options) as p:
    # Process the data (e.g., print it)
    query = """
      SELECT 
        to_char(tpep_pickup_datetime, 'yyyy-mm-dd HH:MM:SS') as tpep_pickup,
        to_char(tpep_dropoff_datetime, 'yyyy-mm-dd HH:MM:SS') as tpep_dropoff,
        coalesce(passenger_count, 1) as passenger_count,
        trip_distance,
        coalesce(store_and_fwd_flag, '0') store_and_fwd_flag,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        total_amount
      from yellow_tripdata_trip c limit 50000
    """
    lines = p | 'Read from PostgreSQL' >> ReadFromJdbc(
                                                table_name='yellow_tripdata_trip',
                                                driver_class_name='org.postgresql.Driver',
                                                jdbc_url='jdbc:postgresql://localhost:5432/ny_taxi',
                                                username='root',
                                                password='root',
                                                query=query,
                                                # fetch_size=50000
                                                # output_parallelization=True
                                            )

    # Format each line into Trip schema
    trips = lines | 'transform to tuple' >> beam.Map(lambda x: Trip(*x)) # it doesn't need with_output_types(Tripe) explicitly
    
    # Or ignore col names and Format each line (NamedTupl) to a list of only values
    # output = lines | 'transform to tuple' >> beam.Map(lambda x: list(x))
    
    def extract_day_from_date(date_str):
      return (datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')).day
       
    
    # Aggregating multiple fields
    output = (trips | 'daily aggregate' >> beam.GroupBy(lambda x: extract_day_from_date(x.tpep_pickup))
                                        .aggregate_field('total_amount', sum, 'daily_total_amount')
                                        .aggregate_field('total_amount', max, 'daily_max_amount'))

    # convert to csv format
    csv = (output | 'to csv' >> beam.Map(lambda x:  ', '.join([str(x.key), str(x.daily_total_amount), str(x.daily_max_amount)])))

    csv | 'Write' >> WriteToText(known_args.output, file_name_suffix='.csv')


if __name__ == '__main__':
  # beam reads bounded data from PostgreSQL, makes some transformations, and writes the output files in csv format
  run_beam_pipeline()
  
  # then we use our OCI Bucket Module to move the csv formatted data to OCI bucket
  bucket = 'bucket-apache-beam'
  files_directory = '/home/opc/beam/Apache-Beam-Practice/examples/pg_to_oracle/data'
  oci_bucket_module.upload_directory(bucket, files_directory)