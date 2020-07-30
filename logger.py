import logging

logging.basicConfig(filename='/var/log/consumer.log',level=logging.DEBUG,format='%(asctime)s:%(levelname)s:%(message)s')
generateLogs=logging.getLogger()    
