package com.paidy.dar.interview

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, concat_ws, count, first, lead, lit, max, min, row_number, sum, unix_timestamp, when, year}
import org.apache.spark.sql.types.{DataType, DoubleType, StructType}
import org.apache.spark.sql.expressions.Window

import java.sql.Timestamp

object SessionStatistics {

  val TargetTableName: String = "session_statistics"
  val TargetTableSchema: String =
    """
      |    session_id STRING NOT NULL,
      |    user_id STRING NOT NULL,
      |    session_end_at TIMESTAMP,
      |    session_start_at TIMESTAMP,
      |    num_events SHORT,
      |    device_id STRING,
      |    time_spent_in_shopping DOUBLE,
      |    session_end_year SHORT NOT NULL
      |""".stripMargin

  final def main(args: Array[String]): Unit = {
    val start_date = args(0)
    val end_date = args(1)

    val spark = createSparkSession()

    // create target table if not exists
    if (!spark.catalog.tableExists(TargetTableName)) {
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], DataType.fromDDL(TargetTableSchema).asInstanceOf[StructType])
        .write.partitionBy("session_end_year")
        .saveAsTable(TargetTableName)
    }

    // data from the source
    val sourceDF =
      spark.read.parquet("src/test/resources/app_events")
        .filter((col("event_timestamp") >= start_date) && (col("event_timestamp") < end_date))


    // if no activity from a user for 30 mins, then the last event before the period of inactivity represents the
    // end of their session
    val sessionExpirationThresholdMins = 30

    /*
     We will only assess that an event is the last in a session (and therefore the session has ended)
     if the most recent event is more than {sessionExpirationThresholdMins} mins earlier than the
     latest event in the mixpanel period.
      */
    val mostRecentTime = sourceDF.select(max(col("event_timestamp")).as("latest_time"))

    val eventTimeDistWindowSpec = Window.partitionBy("user_id").orderBy(col("event_timestamp").asc)

    val endedSessions = sourceDF.select("user_id", "event_timestamp").dropDuplicates()
      .crossJoin(mostRecentTime)
      .withColumn("next_event_or_max_tdist",
        when(lead(col("event_timestamp"), 1).over(eventTimeDistWindowSpec).isNull, unix_timestamp(col("latest_time")) - unix_timestamp(col("event_timestamp")))
          .otherwise(unix_timestamp(lead(col("event_timestamp"), 1).over(eventTimeDistWindowSpec)) - unix_timestamp(col("event_timestamp"))))
      .filter(col("next_event_or_max_tdist").cast("long").cast("Double").divide(60) > lit(sessionExpirationThresholdMins))
      .select(col("user_id"),
        col("event_timestamp").as("session_end_at"),
        concat_ws("__", col("user_id"), unix_timestamp(col("event_timestamp")).cast("string")).as("session_id"))

    val appEventsSessionLinked = sourceDF.join(endedSessions, Seq("user_id"), "inner")
      .withColumn("event_to_session_end_at_tdist", unix_timestamp(col("session_end_at")) - unix_timestamp(col("event_timestamp")))
      .filter(col("event_to_session_end_at_tdist") >= lit(0))
      .withColumn("session_rank",
        row_number().over(Window.partitionBy( "event_id").orderBy(col("event_to_session_end_at_tdist").asc)))
      .filter((col("session_rank") === 1) &&
        (col("session_end_at") >= start_date) &&
        (col("session_end_at") < end_date))

    val sessionStatsMain =
      appEventsSessionLinked
        .withColumn("page_stay_duration",
          lead(col("event_timestamp"), 1).over(Window.partitionBy(col("session_id")).orderBy(col("event_timestamp"))).cast(DoubleType) - col("event_timestamp").cast(DoubleType))
        .groupBy("session_id", "user_id", "session_end_at")
        .agg(min(col("event_timestamp")).alias("session_start_at"),
          count(col("event_name")).alias("num_events"),
          first(col("device_id")).alias("device_id"),
          sum(when(col("event_name").contains("shop"), col("page_stay_duration")).otherwise(lit(null))).alias("time_spent_in_shopping")
      )
        .withColumn("session_end_year", year(col("session_end_at")))

    // sessionStatsMain.write.insertInto(TargetTableName)


    // TODO refactor for more flexible calculation of fraud features
    

    /* a Spendy user logging into their account from multiple devices in the same day
    Assumption: 'same day' implies same calendar day (not 'within 24hrs')
     */
    // Create a deduped set of the source data for event_names containing 'login'
    val loginEvents = sourceDF.select("user_id", "event_timestamp", "device_id", "event_name")
      .filter(col("event_name").contains("login")).dropDuplicates()

    // True when a user_id has ever used multiple device_id with event_names containing 'login' on the same day
    val multiDvcSameUser = loginEvents
      .withColumn("event_date", to_date(col("event_timestamp")))
      .groupBy("user_id", "event_date")
      .agg(countDistinct(col("device_id")).alias("num_devices"))
      .withColumn("multi_device_uses", (col("num_devices") > 1))
      .groupBy("user_id")
      .agg(max(col("multi_device_uses")).alias("multi_device_user"))

    /*
    a Spendy user logging into their account from a new device
    when they haven't logged into their account in the App for a long time
    Assumption: 'long time' is over 4 weeks, event_name contains 'login',
                new device is different device_id
     */
    // Filter for login events that occurred on a new device
    val timeBetweenLogins = loginEvents
      .withColumn("prev_event_timestamp", lag("event_timestamp", 1).over(eventTimeDistWindowSpec))
      .withColumn("prev_device_id", lag("device_id", 1).over(eventTimeDistWindowSpec))
      .withColumn("time_since_last_login", datediff(col("event_timestamp"), col("prev_event_timestamp")))

    // Filter for logins that occurred more than 4 weeks since the last login
    val longTimeNoLogin = timeBetweenLogins
      .filter((col("time_since_last_login") > 24 * 7 * 4) && (col("device_id") =!= col("prev_device_id")))
      .withColumn("long_time_no_login", lit(true))
      .select("user_id", "long_time_no_login").dropDuplicates()

    /* One device logging into multiple Spendy accounts over a short period of time
    Assumption: 'short period of time' is within 1hr and event_name contains 'login'
     */
    // True when a device_id on a given day has multiple user_ids with event_names containing 'login'
    val multiUserSameDvc = timeBetweenLogins
      .filter((col("time_since_last_login") <= 1))
      .withColumn("event_date", to_date(col("event_timestamp")))
      .groupBy("device_id", "event_date")
      .agg(countDistinct(col("user_id")).alias("num_users"))
      .withColumn("multi_user_device", col("num_users") > 1)
      .groupBy("device_id")
      .agg(max(col("multi_user_device")).alias("multi_user_device"))

    /*
    a Spendy user after logging into their account from a new device,
    immediately perform key actions such as change account information or make expensive purchase
    Assumption: 'short period of time' is within 15min, event_name contains 'account' or 'info',
                new device is different device_id
     */
    //
//    val newDeviceLogins = loginEvents
//      .withColumn("prev_event_timestamp", lag("event_timestamp", 1).over(eventTimeDistWindowSpec))
//      .withColumn("prev_device_id", lag("device_id", 1).over(eventTimeDistWindowSpec))
//      .withColumn("time_since_last_login",
//        datediff(col("event_timestamp"), col("prev_event_timestamp"))
//      ).filter(
//        (col("device_id") =!= col("prev_device_id"))
//      )

//    val acctinfoEvents = sourceDF.select("user_id", "event_timestamp", "device_id", "event_name")
//      .filter(col("event_name").contains("account") || col("event_name").contains("info"))
//      .select("user_id").dropDuplicates()


    /*a Spendy user spending more than 5 minutes looking at pages in the app that explain
    where they can use Spendy to shop (these pages have an event name prefixed with "Shop ")
    Assumption: event_name contains 'shop'
     */
    // Use time_spent_shopping to find user_ids looking at 'shop' event_names for more than 5min
    val longShop = sessionStatsMain
      .withColumn("longer_than_5min_shop", col("time_spent_in_shopping") > 5)
      .groupBy("user_id")
      .agg(max(col("longer_than_5min_shop")).alias("long_shopper"))

    // Join multiple device users column on user_id
    val sessionStatswithMDSU = sessionStatsMain
      .join(multiDvcSameUser, Seq("user_id"), "left")

    // Join long time no login until new device user column on user_id
    val sessionStatswithLNL = sessionStatswithMDSU
      .join(longTimeNoLogin, Seq("user_id"), "left")

    // Join multiple user devices column on device_id
    val sessionStatswithMUSD = sessionStatswithLNL
      .join(multiUserSameDvc, Seq("device_id"), "left")

    // Join long look shop users column on user_id
    val sessionStatswithLongShop = sessionStatswithMUSD
      .join(longShop, Seq("user_id"), "left")

    sessionStatswithLongShop
      .write.partitionBy("session_end_year")
      .mode("overwrite")
      .saveAsTable(TargetTableName)
  }

  private def createSparkSession(): SparkSession = {
    val builder = SparkSession.builder

    builder.enableHiveSupport()
    builder.master("local[4]")

    builder.getOrCreate()
  }
}
