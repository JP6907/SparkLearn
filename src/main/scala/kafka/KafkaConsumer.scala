package kafka

import java.util.Properties

import kafka.consumer.{Consumer, ConsumerConfig}

object KafkaConsumer {
  def main(args: Array[String]): Unit = {
    val groupId = "g1"
    val consumerId = "c1"
    val topic = "spark"

    val props = new Properties()
    props.put("zookeeper.connect", "localhost:2181")
    props.put("group.id", groupId)
    props.put("client.id", "test")
    props.put("consumer.id", consumerId)
    props.put("auto.offset.reset", "smallest")
    props.put("auto.commit.enable", "true")
    props.put("auto.commit.interval.ms", "100")

    val consumerConfig = new ConsumerConfig(props)
    val consumer = Consumer.create(consumerConfig)

    val topicCountMap = Map(topic -> 1)
    val consumerMap = consumer.createMessageStreams(topicCountMap)
    val streams = consumerMap.get(topic).get
    for(stream <- streams){
      val it = stream.iterator()
      while(it.hasNext()){
        val messageAndMetadata = it.next()
        val message = s"Topic:${messageAndMetadata.topic}, GroupID:$groupId, Consumer ID:$consumerId, PartitionID:${messageAndMetadata.partition}, " +
          //s"Offset:${messageAndMetadata.offset}, Message Key:${new String(messageAndMetadata.key())}, Message Payload: ${new String(messageAndMetadata.message())}"
          s"Offset:${messageAndMetadata.offset}, Message Key:${messageAndMetadata.key()}, Message Payload: ${new String(messageAndMetadata.message())}"
        System.out.println(message);
      }
    }
  }
}
