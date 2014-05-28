import scala.collection.JavaConversions._

import java.nio.ByteBuffer
import org.apache.crunch.impl.mr.MRPipeline
import org.apache.crunch.io.hbase.HBaseSourceTarget
import org.apache.hadoop.hbase.client.{Result, Scan}
import scala.collection.JavaConversions._
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.util.{ToolRunner, Tool}

object MainHBase {
  def main(args: Array[String]) {
    ToolRunner.run(new Configuration(), new MainHBase(), args)
  }
}

class MainHBase extends Configured with Tool {
  override def run(args: Array[String]): Int = {
    val pipe = new MRPipeline(classOf[MainHBase], getConf)

    val scan = new Scan()
    scan.setCaching(500)
    scan.setCacheBlocks(false)
    scan.setMaxVersions(10000)

    def getResult(x: Result): Iterable[(String, Long, String, Int)] = {
      val cf = x.getMap()("f".getBytes)

      val uid_col = cf("uid".getBytes)

      // cf("uid".getBytes).foreach(x => println(new String(x._2)))
      val versions = uid_col.keys
      val uid = new String(uid_col.head._2)

      val scoreMap = Map(1 -> 3, 4 -> 1, 101 -> 5)

      val cols = List(
        ("iid".getBytes, (x: Array[Byte]) => new String(x)),
        ("aid".getBytes, (x: Array[Byte]) => scoreMap(ByteBuffer.wrap(x).getInt()))
      )

      val logs = versions.map(v => {
        val vx = cols.map(x => x._2(cf(x._1)(v)) )
        (uid, v.toLong, vx(0).asInstanceOf[String], vx(1).asInstanceOf[Int])
      })

      logs // return value.
    }

    val src = new HBaseSourceTarget("user_action_log", scan)
    pipe.read(src).materialize.take(10).foreach(x => println(getResult(x.second())))

    pipe.done()
    0 // return value
  }
}
