import logging
from datetime import datetime

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def create_keyspace(session):
    session.execute("""
                    CREATE KEYSPACE IF NOT EXISTS legistar_spark_streams 
                    WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
                    """)
    
    print("Keyspace created successfully")



def create_table(session):
    session.execute("""
                    CREATE TABLE IF NOT EXISTS legistar_spark_streams.legistar_events (
                    id text PRIMARY KEY,
                    event_id int,
                    event_item_title text,
                    event_item_modified text,
                    event_item_sequence text
                    );
                    """)
    
    print("Table created successfully")

def insert_data(session, **kwargs):
    #insert data into cassandra
    print(f"\n\nKWARGS SHIT: {kwargs}\n\n")

    id = kwargs.get('id')
    event_id = kwargs.get('event_id')
    event_item_title = kwargs.get('event_item_title')
    event_item_modified = kwargs.get('event_item_modified')
    event_item_sequence = kwargs.get('event_item_sequence')
    

    try:
        session.execute(f"""INSERT INTO legistar_spark_streams.legistar_events (event_id, event_item_title, event_item_modified, event_item_sequence)
                        VALUES ({event_id}, '{event_item_title}', '{event_item_modified}', '{event_item_sequence}')""")
    except Exception as e:
        logging.error(f"Couldn't insert data into cassandra due to exception: {e}")


def create_spark_connection():
    #create spark connection

    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName("SparkDataStreaming") \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
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
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "board_of_commission_event_items") \
            .option("startingOffsets", "earliest") \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"Couldn't create kafka dataframe due to exception: {e}")
    
    return spark_df

def create_cassandra_connection():
    #connect to cassandra cluster
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
        StructField("id", StringType(), False),
        StructField("event_id", IntegerType(), False),
        StructField("event_item_title", StringType(), False),
        StructField("event_item_modified", StringType(), False),
        StructField("event_item_sequence", StringType(), False)

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

            logging.info("Streaming is being started...")
            # insert_data(session)
            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")\
                .option('checkpointLocation', '/tmp/checkpoint')\
                .option('keyspace', 'legistar_spark_streams')\
                .option('table', 'legistar_events')\
                .start())
            
            streaming_query.awaitTermination()
    