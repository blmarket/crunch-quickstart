import org.apache.crunch.impl.mr.MRPipeline
import scala.collection.JavaConversions._

import org.apache.crunch._
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.util.{ToolRunner, Tool}

object MainText {
  def main(args: Array[String]) {
    ToolRunner.run(new Configuration(), new MainText(), args)
  }
}

class MainText extends Configured with Tool {
  override def run(args: Array[String]): Int = {
    val pipe = new MRPipeline(classOf[MainText], getConf())

    val lines = pipe.readTextFile(args(0))

    lines.materialize().foreach(println(_))
    return 0
  }
}
