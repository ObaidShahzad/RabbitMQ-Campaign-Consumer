import pika
import mysql.connector
import datetime
credentials = pika.PlainCredentials('admin', 'root')
connection = pika.BlockingConnection(pika.ConnectionParameters('10.0.0.121',5672,'/',credentials))
channel = connection.channel()
channel.queue_declare(queue='hello')
def callback(ch, method, properties, body):
    global cursor
    global sqlconnection
    body=body.decode("utf-8")
    data=eval(body)
    print(f" [x] Received {data}")
    process_id=data["process_id"]
    process_id=int(process_id)+1
    time_now=str(datetime.datetime.now())
    time_now=time_now[:19]
    query=f"UPDATE customers SET `process_id`='{process_id}' , `process_time`= '{time_now}' where id={data['id']}"
    cursor.execute(query)
    sqlconnection.commit()
try:
    sqlconnection = mysql.connector.connect(host='10.0.1.98',database='campaign',user='root',password='awais')
    cursor = sqlconnection.cursor()
    channel.basic_consume(queue='hello', on_message_callback=callback, auto_ack=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
except mysql.connector.Error as error:
    print("Failed to update table record: {}".format(error))
finally:
    if (sqlconnection.is_connected()):
        connection.close()
