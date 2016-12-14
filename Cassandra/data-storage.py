# read from specific kafak cluster and topic	
# write to specific cassandr cluster and topic
# 3. data need to orgnized SSTable

import win_inet_pton
import argparse
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
import logging
import json
import atexit


logging.basicConfig()
logger = logging.getLogger('data-storage')
logger.setLevel(logging.INFO)


def persist_data(stock_data, cassandra_session,table):
      logger.debug("start to get %s" %stock_data)
    
      parsed = json.loads(stock_data)[0]
      symbol = parsed.get('StockSymbol')
      price = float(parsed.get('LastTradePrice'))
      trade_time = parsed.get('LastTradeDateTime') 

      # insert into cassandra
      statement ="INSERT INTO %s (stock_symbol,trade_time,trade_price) VALUES ('%s','%s', %f)" %(table, symbol, trade_time, price)     
      cassandra_session.execute(statement)
      logger.info('Persisted data for sysmol %s, price %f, tradetime %s' % (symbol,price,trade_time)) 


def shutdown_hook(consumer,session):

	 consumer.close()
	 logger.info('kafka consumer closed')
	 session.shutdown()

	 logger.info('cassandra seesion closed')


if __name__ == '__main__':

   
    parser = argparse.ArgumentParser()
    parser.add_argument('topic_name', help='the kafka topic to subscribe from')
    parser.add_argument('kafka_broker', help='the location of the kafka broker')
    parser.add_argument('key_space', help='the keyspace to use in cassandra')
    parser.add_argument('data_table', help='the data table to use')
    parser.add_argument('cassandra_broker', help='the cassandra_broker location')

    args = parser.parse_args()
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker
    key_space = args.key_space
    data_table = args.data_table
    cassandra_broker =args.cassandra_broker

    #- create kafkaconsumer
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=kafka_broker
    )


    cassandra_cluster = Cluster(
        contact_points=cassandra_broker.split(',')
    )

    session = cassandra_cluster.connect()

    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS %s WITH replication ={'class':'SimpleStrategy', 'replication_factor': 3} AND durable_writes ='true' " %key_space
    ) 
    session.set_keyspace(key_space)

    session.execute(

        "CREATE TABLE IF NOT EXISTS %s (stock_symbol text, trade_time timestamp, trade_price float, PRIMARY KEY (stock_symbol,trade_time) )" %data_table

    )
    
    #setup shutdonw hook
    atexit.register(shutdown_hook, consumer, session)
    


    for msg in consumer:
    	persist_data(msg.value, session,data_table)

       
    