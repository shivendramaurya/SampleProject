import sys
from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata
from kafka import TopicPartition
import time
from time import gmtime, strftime
import datetime



KAFKA_TOPIC="test"
BOOTSTRAP_SERVERS="localhost:9092"
GROUP_ID="Group1"
SLEEP_TIME = 6*60

def customPrinter(message):
	print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ": "+ str(message))

#consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=BOOTSTRAP_SERVERS, 
#                         auto_offset_reset='earliest',group_id=GROUP_ID, enable_auto_commit=False)
consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=BOOTSTRAP_SERVERS, 
                         auto_offset_reset='earliest',group_id=GROUP_ID, enable_auto_commit=False,
				max_poll_interval_ms=420000)


try:
	customPrinter("Connected. Starting consuming..")	
	for message in consumer:
		customPrinter("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))
		meta = consumer.partitions_for_topic(message.topic)
		partition = TopicPartition(message.topic, message.partition)
		offsets = OffsetAndMetadata(message.offset+1, meta)
		options = {partition: offsets}
		
		customPrinter("Going to sleep for "+str(SLEEP_TIME)+" Sec")
		time.sleep(SLEEP_TIME)
		customPrinter("Sleep Complete")
		consumer.commit(offsets=options)

except KeyboardInterrupt:
	consumer.close() 
	sys.exit()



