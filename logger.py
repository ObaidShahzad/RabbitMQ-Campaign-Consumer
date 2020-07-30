import logging

logging.basicConfig(filename='/var/log/consumer.log',level=logging.DEBUG,format='%(asctime)s:%(levelname)s:%(message)s')

def msg():
    logging.info(' [*] Waiting for messages. To exit press CTRL+C')

def receivedData(data):
    logging.info(f" [x] Received {data}")

def tableUpdateFailed(error):
    logging.error("Failed to update table record: {}".format(error))