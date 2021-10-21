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

object CountMsgType:
  
//  val config = ObtainConfigReference("randomLogGenerator") match{
//    case Some(value) => value
//    case None => throw new RuntimeException("Incorrect config data")
//  }


  class MapperToken extends Mapper[Object , Text, Text, IntWritable] {
    val ones_count = new IntWritable(1)
    val words = new Text()

    val logger: Logger = CreateLogger(classOf[MapperToken])

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {

      val Message_log: Array[String] = value.toString.split(" ")

      logger.info(s"Processing log message - ${Message_log.toList}")

      val Type_log = Message_log(2)
      logger.info(s"Log message of type - ${Type_log}")

      val key = s"${Type_log}"
      logger.info(s" The key created by the mapper s - ${key}")

      words.set(key)

      context.write(words, ones_count)
    }
  }

  class SumInt extends Reducer[Text, IntWritable, Text, IntWritable] {

    val logger: Logger = CreateLogger(classOf[SumInt])

    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {

      val summation = values.asScala.foldLeft(0)((x, y) => x + y.get())
      logger.info(s"Summation by reducer for values of key ${key} : ${summation}")

      context.write(key, new IntWritable(summation))
    }
  }
  def exec(args: Array[String]): Unit = {

    val task_name = s"CountMsgType"

    val configuration = new Configuration
    val jobs = Job.getInstance(configuration, task_name)

    jobs.setJarByClass(this.getClass)
    jobs.setMapperClass(classOf[MapperToken])
    jobs.setCombinerClass(classOf[SumInt])
    jobs.setReducerClass(classOf[SumInt])
    jobs.setOutputKeyClass(classOf[Text])
    jobs.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.addInputPath(jobs, new Path(args(0)))
    FileOutputFormat.setOutputPath(jobs, new Path(args(1) + task_name))

    configuration.set("mapred.textoutputformat.separatorText", ",")
    jobs.waitForCompletion(true)
  }



