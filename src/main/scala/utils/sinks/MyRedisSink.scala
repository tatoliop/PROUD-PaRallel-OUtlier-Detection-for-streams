package utils.sinks

import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

class MyRedisSink(c_db: String) extends RedisMapper[String] {

  val redis_db: String = c_db

  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.SET)
  }

  override def getKeyFromData(data: String): String = {
    redis_db
  }

  override def getValueFromData(data: String): String = {
    data
  }
}
