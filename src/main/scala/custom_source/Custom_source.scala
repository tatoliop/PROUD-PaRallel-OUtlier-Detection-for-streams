package custom_source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

object Custom_source {

  def main(args: Array[String]): Unit = {

    val parameters: ParameterTool = ParameterTool.fromArgs(args)
    val dataset = parameters.getRequired("dataset") //STK, TAO
    val DEBUG = parameters.get("DEBUG", "false").toBoolean

    val output = System.getenv("KAFKA_TOPIC") //File input or kafka topic
    val kafka_brokers = System.getenv("KAFKA_BROKERS")
    // Properties for Kafka producers
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", kafka_brokers)
    //Create the kafka producer for the output topic (Law & Order)
    val my_producer = new FlinkKafkaProducer[String](properties.getProperty("bootstrap.servers"), output, new SimpleStringSchema())

    //Start context
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val myGenerator = env
      .addSource(new Kafka_flink_source(dataset))
    if(DEBUG) myGenerator.print
    else myGenerator.addSink(my_producer)

    env.execute("Custom Flink non-parallel source")
  }

}
