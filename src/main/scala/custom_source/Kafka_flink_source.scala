package custom_source

import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

class Kafka_flink_source(c_dataset: String) extends RichSourceFunction[String] {

  private val dataset = c_dataset
  private val jsonProducer = new Json_source(dataset)

  @volatile var isRunning = true

  override def run(ctx: SourceContext[String]): Unit = {
    while (isRunning) {
      val record = jsonProducer.get_next()
      ctx.collect(record)
      Thread.sleep(1)
    }
  }

  override def cancel(): Unit = isRunning = false

}

