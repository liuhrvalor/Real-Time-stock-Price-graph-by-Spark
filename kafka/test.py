import atexit
import logging
import json
import time

from googlefinance import getQuotes
from apscheduler.schedulers.background import BackgroundScheduler

from flask import (
    Flask,
    request,
    jsonify
)

from kafka import KafkaProducer
from kafka.errors import (
    KafkaError,
    KafkaTimeoutError
)


from googlefinance import getQuotes
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError

import argparse
import atexit
import datetime
import logging
import json
import random
import schedule
import time

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-producer')
logger.setLevel(logging.INFO)





def fetch_price(symbol):
    """
    helper function to retrieve stock data and send it to kafka
    :param symbol: symbol of the stock
    :return: None
    """
    logger.debug('Start to fetch stock price for %s', symbol)


    try:

        current_time = time.strftime("%H")*60 + time.strftime("%M")


        payload = json.dumps(getQuotes('AAPL'))
        

        payload = None

        if 570 <= int(current_time) <= 960:
             payload = json.dumps(getQuotes(symbol))
        else:
            price = random.randint(30, 120)
            timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%dT%H:%MZ')
            payload = ('[{"StockSymbol":"%s","LastTradePrice":%d,"LastTradeDateTime":"%s"}]' % (symbol, price, timestamp)).encode('utf-8')

        logger.debug('Retrieved stock info %s', payload)

        #producer.send(topic=topic_name, value=price, timestamp_ms=time.time())
        
        producer.send(topic=topic_name, value=payload, timestamp_ms=time.time())

        logger.info('Sent stock price for %s to Kafka', symbol)

    except KafkaTimeoutError as timeout_error:
        logger.info('level 1')
        logger.warn('Failed to send stock price for %s to kafka, caused by: %s', (symbol, timeout_error.message))
    except Exception:
        logger.info('level 2')
        logger.warn('Failed to fetch stock price for %s', symbol)





if __name__ == '__main__':

    payload = json.dumps(getQuotes('AAPL'))
    log.info(payload)
    print(payload)

