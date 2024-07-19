import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json


def create_keyspace(session):
    session.execute("""CREATE KEYSPACE IF NOT EXISTS legistar_spark_streams 
                    WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}""")
    
    print("Keyspace created successfully")

def create_table(session):
    session.execute("""CREATE TABLE IF NOT EXISTS legistar_spark_streams.legistar_events (
                    event_id int PRIMARY KEY,
                    event_date text,
                    event_time text,
                    EventInSiteURL text,
                    event_data text
                    )""")

def insert_data(session, **kwargs):
    #insert data into cassandra
    event_id = kwargs.get('event_id')
    event_date = kwargs.get('event_date')
    event_time = kwargs.get('event_time')
    EventInSiteURL = kwargs.get('EventInSiteURL')
    event_data = kwargs.get('event_data')

    try:
        session.execute(f"""INSERT INTO legistar_spark_streams.legistar_events (event_id, event_date, event_time, EventInSiteURL, event_data)
                        VALUES ({event_id}, '{event_date}', '{event_time}', '{EventInSiteURL}', '{event_data}')""")
    except Exception as e:
        logging.error(f"Couldn't insert data into cassandra due to exception: {e}")


def create_spark_connection():
    #create spark connection

    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName("SparkDataStreaming") \
            .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.5.1', 
                    'org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1') \
            .config('spark.cassandra.connection.host', 'broker') \
            .getOrCreate()
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark session created successfully")
    except Exception as e:
        logging.error(f"Couldn't create spark session due to exception: {e}")

    return s_conn

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "broker:9092") \
            .option("subscribe", "board_of_commission_events") \
            .option("startingOffsets", "earliest") \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"Couldn't create kafka dataframe due to exception: {e}")
    
    return spark_df

def create_cassandra_connection():
    #connect to cassandra cluster
    session = None

    try:
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f"Couldn't connect to cassandra cluster due to exception: {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    #create selection dataframe from kafka dataframe
    schema = StructType([
        StructField("event_id", IntegerType(), False),
        StructField("event_date", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("EventInSiteURL", StringType(), False),
        StructField("event_data", StringType(), False)

    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)")\
        .select(from_json(col("value"), schema).alias('data')).select('data.*')
    
    print(sel)
    return sel

if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection

        df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(df)
        session = create_cassandra_connection()

        if session is not None:
            # do nothing
            create_keyspace(session)
            create_table(session)
            # insert_data(session)
            streaming_query = (selection_df.writeStream.format("org.apache.spark.cassandra")\
                .option('checkpointLocation', '/tmp/checkpoint')\
                .option('keyspace', 'legistar_spark_streams')\
                .option('table', 'legistar_events')\
                .start())
            
            streaming_query.awaitTermination()
            

