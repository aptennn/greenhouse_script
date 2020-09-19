import psycopg2
import cfg
import logging
import anyio
from distmqtt.client import open_mqttclient, ClientException
from distmqtt.mqtt.constants import QOS_0,QOS_1, QOS_2

conn = psycopg2.connect(dbname=cfg.b_base, user=cfg.b_login,
                        password=cfg.b_pass, host=cfg.b_host)
cursor = conn.cursor() #иницализация базы, создание курсора, импорт библиотек




logger = logging.getLogger(__name__)

# главная фунцкия
async def uptime_coro():
    async with open_mqttclient() as C:
        await C.connect(f"mqtt://{cfg.m_login}:{cfg.m_pass}@{cfg.m_server}:{cfg.m_port}")
        # подключаемся к серверу
        try:
            await C.subscribe(
                [("test/temp",QOS_0)] # пытаемся подписаться на топик
            )
            logger.info("Subscribed")
            while True: # когда подписались
                message = await C.deliver_message()
                packet = message.publish_packet
                print(packet.variable_header.topic_name, str(packet.payload.data))
                msg = str(packet.payload.data) # получаем и обрабатываем сообщения 
                msg_list = msg.replace('bytearray', '').replace('b', '').replace("'", "").replace('(', '').replace(')', '').split()               
                print(float(msg_list[0]), (float(msg_list[1]))) 
                sql = "INSERT INTO dht (temp, hum) VALUES (%s, %s)"
                val = (float(msg_list[0]), float(msg_list[1]))
                cursor.execute(sql, val) # добавляем в базу
                conn.commit()

            await C.unsubscribe(["test/temp"])
            logger.info("UnSubscribed")
        except ClientException as ce:
            logger.error("Client exception: %r", ce)

# запуск главной функции
if __name__ == "__main__":
    formatter = "[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=formatter)
    anyio.run(uptime_coro)
        
