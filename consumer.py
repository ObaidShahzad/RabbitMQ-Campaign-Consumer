import pika
import mysql.connector
import datetime
import logger

credentials = pika.PlainCredentials('admin', 'root')
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost',5672,'/',credentials))
channel = connection.channel()
channel.queue_declare(queue='hello')

def callback(ch, method, properties, body):
    global cursor
    global sqlconnection
    body=body.decode("utf-8")
    data=eval(body)
    logger.receivedData(data)
    process_id=data["process_id"]
    process_id=int(process_id)+1
    time_now=str(datetime.datetime.now())
    time_now=time_now[:19]
    query=f"UPDATE customers SET `process_id`='{process_id}' , `process_time`= '{time_now}' where id={data['id']}"
    cursor.execute(query)
    sqlconnection.commit()
#------------- Function Definition Ended -------------------------
try:
    sqlconnection = mysql.connector.connect(host='10.0.1.98',database='campaign',user='root',password='awais')
    cursor = sqlconnection.cursor()
    channel.basic_consume(queue='hello', on_message_callback=callback, auto_ack=True)
    logger.msg()
    channel.start_consuming()
except mysql.connector.Error as error:
    logger.tableUpdateFailed(error)
finally:
    if (sqlconnection.is_connected()):
        connection.close()
