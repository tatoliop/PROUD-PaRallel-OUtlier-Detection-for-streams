package adapt

import com.redis.RedisClient
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

class MyRedisSource(c_host: String, c_port: Int, c_db: String, c_sleed: Long, c_cancel: Int) extends RichSourceFunction[String] {

  @volatile var isRunning = true
  var client: RedisClient = _
  val redis_host = c_host
  val redis_port = c_port
  val redis_db = c_db
  var data_time: Long = 0
  val sleep: Long = c_sleed
  val cancel_flag: Int = c_cancel
  var cancel_counter: Int = 0

  override def open(parameters: Configuration) = {
    client = new RedisClient(redis_host, redis_port) // init connection etc
    client.connect
  }

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    while (isRunning) {
      val res: String = client.get(redis_db).orNull
      if (res != null) {
        val tmp = res.split(";")(0).toLong
        if (tmp > data_time) {
          data_time = tmp
          ctx.collect(res)
          cancel_counter = 0
        }
      }
      cancel_counter += 1
      if(cancel_counter > cancel_flag) cancel()
      Thread.sleep(sleep)
    }
  }

  override def cancel(): Unit = {
    client.disconnect
    isRunning = false
  }
}
