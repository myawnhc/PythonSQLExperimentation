import hazelcast
from hazelcast import HazelcastClient

def create_topic1_mapping(client: HazelcastClient) -> None:
    print("Create mapping and view for Topic-A ... ", end="")
    statement = """
        CREATE OR REPLACE MAPPING "Topic-A" (
                id VARCHAR,
                event_time TIMESTAMP WITH TIME ZONE,
                dataA VARCHAR
            ) 
            TYPE Kafka
            OPTIONS (
                'keyFormat' = 'varchar',
                'valueFormat' = 'json-flat',
                'bootstrap.servers' = '127.0.0.1:9092', 
                'auto.offset.reset' = 'earliest' 
            )
    """
    client.sql.execute(statement).result()

    statement = """
        CREATE OR REPLACE VIEW topica_ordered AS
            SELECT * FROM TABLE(IMPOSE_ORDER(
            TABLE "Topic-A",
            DESCRIPTOR(event_time),
            INTERVAL '30' SECONDS));
    """
    client.sql.execute(statement).result()
    print("OK.")

def create_topic2_mapping(client: HazelcastClient) -> None:
    print("Create mapping and view for Topic-B ... ", end="")
    statement = """
        CREATE OR REPLACE MAPPING "Topic-B" (
                id VARCHAR,
                event_time TIMESTAMP WITH TIME ZONE,
                dataB VARCHAR
            ) 
            TYPE Kafka
            OPTIONS (
                'keyFormat' = 'varchar',
                'valueFormat' = 'json-flat',
                'bootstrap.servers' = '127.0.0.1:9092', 
                'auto.offset.reset' = 'earliest' 
            )
    """
    client.sql.execute(statement).result()

    statement = """
        CREATE OR REPLACE VIEW topicb_ordered AS
            SELECT * FROM TABLE(IMPOSE_ORDER(
            TABLE "Topic-B",
            DESCRIPTOR(event_time),
            INTERVAL '30' SECONDS));
    """
    client.sql.execute(statement).result()
    print("OK.")

def create_topic3_mapping(client: HazelcastClient) -> None:
    print("Create mapping and view for Topic-C ... ", end="")
    statement = """
        CREATE OR REPLACE MAPPING "Topic-C" (
                id VARCHAR,
                event_time TIMESTAMP WITH TIME ZONE,
                dataC VARCHAR
            ) 
            TYPE Kafka
            OPTIONS (
                'keyFormat' = 'varchar',
                'valueFormat' = 'json-flat',
                'bootstrap.servers' = '127.0.0.1:9092', 
                'auto.offset.reset' = 'earliest' 
            )
    """
    client.sql.execute(statement).result()

    statement = """
        CREATE OR REPLACE VIEW topicc_ordered AS
            SELECT * FROM TABLE(IMPOSE_ORDER(
            TABLE "Topic-C",
            DESCRIPTOR(event_time),
            INTERVAL '30' SECONDS));
    """
    client.sql.execute(statement).result()
    print("OK.")


# Connect to Hazelcast cluster.
client = hazelcast.HazelcastClient()

sqlservice = client.sql

create_topic1_mapping(client)
create_topic2_mapping(client)
create_topic3_mapping(client)

# Note: INNER, LEFT, or RIGHT JOIN all work here; FULL (outer) join is not supported
query = """
   SELECT a.id,
     a.event_time,
     a.dataA,
     b.dataB,
     c.dataC
   FROM topica_ordered a
     LEFT JOIN topicb_ordered b ON a.id = b.id 
     LEFT JOIN topicc_ordered c on a.id = c.id 
     WHERE a.event_time BETWEEN b.event_time - INTERVAL '30' SECONDS
                            AND b.event_time + INTERVAL '30' SECONDS
       AND a.event_time BETWEEN c.event_time - INTERVAL '30' SECONDS
                            AND c.event_time + INTERVAL '30' SECONDS
"""

result = sqlservice.execute(query).result()
for row in result:
    print(row)

# Shutdown the client.
client.shutdown()
