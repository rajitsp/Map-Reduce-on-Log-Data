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

object NoOfChar:

  class CharMapper extends Mapper[Object, Text, Text, IntWritable] {
    val char_length = new IntWritable(0)
    val msg = new Text()
    val logger: Logger = CreateLogger(classOf[CharMapper])


    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      val Message_log: Array[String] = value.toString.split(" ")

      logger.info(s"Processing log message - ${Message_log.toList}")

      val log_desc = Message_log(5)
      val len = log_desc.length()
      val log_type = Message_log(2)

      val key = s"{log_type}"

      msg.set(key)
      char_length.set(len)

      context.write(msg, char_length)

    }
  }

  class MaxLen extends Reducer[Text, IntWritable, Text, IntWritable] {

    val logger: Logger = CreateLogger(classOf[MaxLen])

    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {

      val highest = values.asScala.max
      logger.info(s"Max length of type of log message ${key} : ${highest}")

      context.write(key, highest)

    }
  }

  def exec(args: Array[String]): Unit = {

    val task_name = s"NoOfChar"

    val configuration = new Configuration
    val jobs = Job.getInstance(configuration, task_name)

    jobs.setJarByClass(this.getClass)
    jobs.setMapperClass(classOf[CharMapper])
    jobs.setCombinerClass(classOf[MaxLen])
    jobs.setReducerClass(classOf[MaxLen])
    jobs.setOutputKeyClass(classOf[Text])
    jobs.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.addInputPath(jobs, new Path(args(0)))
    FileOutputFormat.setOutputPath(jobs, new Path(args(1) + task_name))

    configuration.set("mapred.textoutputformat.separatorText", ",")
    jobs.waitForCompletion(true)
  }

  
