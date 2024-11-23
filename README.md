To use: 
- run `docker compose up -d`
- start BOC_automation DAG from Airflow, or simply run `dags/kafka_stream.py`
- Check Topics in Confluent Control Center
- Run `spark_stream.py` to create Cassandra keyspace and table, and insert data from Kafka
- Enter CQL shell with `docker exec -it cassandra cqlsh -u cassandra -p cassandra localhost 9042`
- Find relevant keyspace with `DESCRIBE keyspaces;`
- `SELECT [...] FROM legistar_spark_streams.legistar_events`


Cassandra CQL Shell:


![image](https://github.com/user-attachments/assets/7d02fb53-9a15-4ddd-a35f-ae1a134e029d)


Confluent Control Center:

![image](https://github.com/user-attachments/assets/41bb5136-ba45-48b1-a07d-fbd672db2e73)



Airflow:

![image](https://github.com/user-attachments/assets/75becc51-80a7-4efe-97c6-e504db4a4ebc)



Notes:
- `spark_stream.py` only creates a table in Cassandra for event items (things that happened in an event); a table to pull from
the Kafka topic containing event details (general event info) can/should be made




