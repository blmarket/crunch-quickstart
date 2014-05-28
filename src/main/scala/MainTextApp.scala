import scala.collection.JavaConversions._
import org.apache.crunch.scrunch._

object MainTextApp extends PipelineApp {
  override def run(args: Array[String]) {
    val file = pipeline.readTextFile(args(0))
    file.map(_.split("::")).materialize().take(10).map(_.toList).foreach(println(_))
    pipeline.done
  }
}

