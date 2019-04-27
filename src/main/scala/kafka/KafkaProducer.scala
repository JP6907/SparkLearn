package kafka

import java.util.Properties

import scala.math._
import kafka.producer.{KeyedMessage, Partitioner, Producer, ProducerConfig}
import kafka.utils.VerifiableProperties

object KafkaProducer {
  def main(args: Array[String]): Unit = {

    val brokers = "localhost:9092"
    val topic = "test";

    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("partitioner.class", classOf[HashPartitioner].getName)
    props.put("producer.type", "sync")
    props.put("batch.num.messages", "1")
    props.put("queue.buffering.max.messages", "1000000")
    props.put("queue.enqueue.timeout.ms", "20000000")

    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config);

    val sleepFlag = false;

    val message1 = new KeyedMessage[String, String](topic, "1", "test 0");
    producer.send(message1);
    if(sleepFlag) Thread.sleep(5000);

    val message2 = new KeyedMessage[String, String](topic, "1", "test 1");
    producer.send(message2);
    if(sleepFlag) Thread.sleep(5000);

    val message3 = new KeyedMessage[String, String](topic, "1", "test 2");
    producer.send(message3);
    if(sleepFlag) Thread.sleep(5000);

    val message4 = new KeyedMessage[String, String](topic, "4", "test 3");
    producer.send(message4);
    if(sleepFlag) Thread.sleep(5000);
    val message5 = new KeyedMessage[String, String](topic, "4", "test 4");
    producer.send(message5);
    if(sleepFlag) Thread.sleep(5000);

    val message6 = new KeyedMessage[String, String](topic, "4", "test 4");
    producer.send(message6);
    if(sleepFlag) Thread.sleep(5000);

  }

  class HashPartitioner extends Partitioner {
    def this(verifiableProperties: VerifiableProperties) { this }

    override def partition(key: Any, numPartitions: Int): Int = {

      if (key.isInstanceOf[Int]) {
        abs(key.toString().toInt) % numPartitions
      }

      key.hashCode() % numPartitions
    }

  }
}
