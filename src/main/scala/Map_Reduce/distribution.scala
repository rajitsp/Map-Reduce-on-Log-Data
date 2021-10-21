package Map_Reduce

import HelperUtils.{CreateLogger, ObtainConfigReference}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.slf4j.Logger
import java.util.Iterator
import java.lang
import scala.collection.JavaConverters.*

object distribution {

  class TimeTypeMapper extends Mapper[Object, Text, Text, IntWritable] {

    val no = new IntWritable(1)
    val timeandtype = new Text()
    val logger: Logger = CreateLogger(classOf[TimeTypeMapper])

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {

      val Message_log: Array[String] = value.toString.split(" ")
      logger.info(s"Processing log message - ${Message_log.toList}")

      val t0 = Message_log(0)
      val log_type = Message_log(2)
      val time_and_type = t0 + "-" + log_type
      logger.info(s" The time-interval and type created by the mapper is - ${time_and_type}")

      val key = time_and_type
      timeandtype.set(key)

      context.write(timeandtype, no)

      }
    }


  class TimeTypeReducer extends Reducer[Text, IntWritable, Text, IntWritable] {

    val logger: Logger = CreateLogger(classOf[TimeTypeReducer])

    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {

      val total_no = values.asScala.foldLeft(0)((x, y) => x + y.get())

      logger.info(s"No of log types summed by reducer for values of key ${key} : ${total_no}")

      context.write(key, new IntWritable(total_no))
    }
  }

  def exec(args: Array[String]): Unit = {

    val task_name = s"LogDistribution"

    val configuration = new Configuration
    val jobs = Job.getInstance(configuration, task_name)

    jobs.setJarByClass(this.getClass)
    jobs.setMapperClass(classOf[TimeTypeMapper])
    jobs.setCombinerClass(classOf[TimeTypeReducer])
    jobs.setReducerClass(classOf[TimeTypeReducer])
    jobs.setOutputKeyClass(classOf[Text])
    jobs.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.addInputPath(jobs, new Path(args(0)))
    FileOutputFormat.setOutputPath(jobs, new Path(args(1) + task_name))

    configuration.set("mapred.textoutputformat.separatorText", ",")
    jobs.waitForCompletion(true)
  }

}
