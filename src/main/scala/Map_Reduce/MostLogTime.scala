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


object MostLogTime {

  class ErrorMapper extends Mapper[Object, Text, Text, IntWritable] {

    val errorno = new IntWritable(1)
    val time = new Text()
    val logger: Logger = CreateLogger(classOf[ErrorMapper])

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {

      val Message_log: Array[String] = value.toString.split(" ")
      logger.info(s"Processing log message - ${Message_log.toList}")

      val t0 = Message_log(0)
      val log_type = Message_log(2)
      val time_interval = t0.slice(0, 8)
      logger.info(s" The time with interval of 1 second created by the mapper is - ${time_interval}")


      if log_type == "ERROR" then {

        val key = Text(s"${time_interval}")
        logger.info(s" The key created by the mapper is - ${key}")

        time.set(key)

        context.write(time, errorno)

      }
    }
  }

  class ErrorReducer extends Reducer[Text, IntWritable, Text, IntWritable] {

    val logger: Logger = CreateLogger(classOf[ErrorReducer])

    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {

      val Err_no = values.asScala.foldLeft(0)((x, y) => x + y.get())

      logger.info(s"No of error by reducer for values of key ${key} : ${Err_no}")

      context.write(key, new IntWritable(Err_no))
    }
  }

  class Map2 extends Mapper[Text, IntWritable, IntWritable, Text]{

    val logger: Logger = CreateLogger(classOf[Map2])
    val time_interval1 = new Text()


    override def map(key: Text, value: IntWritable, context: Mapper[Text, IntWritable, IntWritable, Text]#Context): Unit = {


      time_interval1.set(key)
      logger.info(s"Output of mapper 2 is key - ${value} and value - ${time_interval1}")
      context.write(value, time_interval1)
    }


  }
  class Reduce2 extends Reducer[IntWritable, Text, IntWritable, Text] {

    val logger: Logger = CreateLogger(classOf[Reduce2])


    override def reduce(key: IntWritable, values: lang.Iterable[Text], context: Reducer[IntWritable, Text, IntWritable, Text]#Context): Unit = {

      val new_times = values.asScala.toList.toString()
      val time_interval = new Text(new_times)
      context.write(key,time_interval)
    }
  }

  def exec(args: Array[String]): Unit = {

    val task_name = s"CountError"

    val configuration = new Configuration
    val jobs = Job.getInstance(configuration, task_name)

    jobs.setJarByClass(this.getClass)
    jobs.setMapperClass(classOf[ErrorMapper])
    jobs.setCombinerClass(classOf[ErrorReducer])
    jobs.setReducerClass(classOf[ErrorReducer])
    jobs.setOutputKeyClass(classOf[Text])
    jobs.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.addInputPath(jobs, new Path(args(0)))
    FileOutputFormat.setOutputPath(jobs, new Path(args(1) + task_name))

    configuration.set("mapred.textoutputformat.separatorText", ",")
    jobs.waitForCompletion(true)


    val task_name1 = s"MaxSortError"

    val configuration1 = new Configuration
    val jobs1 = Job.getInstance(configuration1, task_name1)

    jobs1.setJarByClass(this.getClass)
    jobs1.setMapperClass(classOf[Map2])
    jobs1.setCombinerClass(classOf[Reduce2])
    jobs1.setReducerClass(classOf[Reduce2])
    jobs1.setOutputKeyClass(classOf[IntWritable])
    jobs1.setOutputValueClass(classOf[Text])

    FileInputFormat.addInputPath(jobs1, new Path(args(1)))
    FileOutputFormat.setOutputPath(jobs1, new Path(args(2) + task_name))

    configuration.set("mapred.textoutputformat.separatorText", ",")
    jobs.waitForCompletion(true)
  }

}


