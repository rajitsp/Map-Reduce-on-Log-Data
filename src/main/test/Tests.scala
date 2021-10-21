import Map_Reduce.*

class Tests extends AnyFlatSpec with Matchers {


  val logtest1 = "20:19:12.736 [scala-execution-context-global-119] WARN  HelperUtils.Parameters$ - s%]s,+2k|D}K7b/XCwG&@7HDPR8z"
  val logtest2 = "20:19:12.985 [scala-execution-context-global-119] EROR HelperUtils.Parameters$ - ihu}!A2]*07}|,lc"
  val logtest3 = "20:19:13.014 [scala-execution-context-global-119] INFO  HelperUtils.Parameters$ - CC]>~R#,^#0JWyESarZdETDcvk)Yk'I?"
  val message1: Array[String] = logtest1.split(" ")
  val message2: Array[String] = logtest2.split(" ")

  val config = ObtainConfigReference("randomLogGenerator") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")

  }

  it should "Check if type of value of number of logs in config file is a number" in {
    val t = config.getInt("MaxCount")
    assert(t.getClass == Int)
  }

  it should "throw an exception if you call mapper function in NoOfChar " in {

    try {
      val t = MostLogTime.exec(args = Array("abc"))
    }
    catch {
      case _: Exception =>
     }
  }

  it should "Check if start substring of the input is a time stamp of log" in{
    val message: Array[String] = logtest1.split(" ")
    val time = message(0).replace(":","")
    val time2 = time.replace(".","")
    assert(time2.getClass == Int)
  }

  it should "Check if type of Log is ERROR, WARN, INFO, DEBUG" in{

    val logtype = message2(2)
    assert((logtype == "ERROR") || (logtype == "WARN") || (logtype == "DEBUG")|| (logtype == "INFO"))
  }

  it should "Check if log is String of 6 substrings" in {
    val len = message2.length
    assert(len == 6)
  }

}
