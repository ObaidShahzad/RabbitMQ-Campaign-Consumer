#!/usr/bin/env python
import pika
import pickle
import mysql.connector
from mysql.connector import Error
import datetime

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='hello')



def callback(ch, method, properties, body):
    global cursor

    data=pickle.loads(body)

    print(type(data))
    print(data)
    process_id=data["process_id"]
    process_id=int(process_id)+1
    print(process_id)
    time_now=str(datetime.datetime.now())
    time_now=time_now[:19]
    query=f"UPDATE customers SET `process_id`='{process_id}' , `process_time`= '{time_now}' where id={data['id']}"
    print(query)
    cursor.execute(query)
    connection.commit()
try:
    connection = mysql.connector.connect(host='10.0.1.98',
                                        database='campaign',
                                        user='root',
                                        password='awais')
    cursor = connection.cursor()

    channel.basic_consume(
    queue='hello', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
except mysql.connector.Error as error:
    print("Failed to update table record: {}".format(error))
finally:
    if (connection.is_connected()):
        connection.close()
        print("MySQL connection is closed")