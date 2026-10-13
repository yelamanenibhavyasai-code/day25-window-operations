
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

object Day25WindowOperations {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day25WindowOperations")
      .setMaster("local[*]")

    // Process incoming data every 10 seconds
    val ssc = new StreamingContext(conf, Seconds(10))

    // Required for window-based operations
    ssc.checkpoint("checkpoint/day25")
    ssc.sparkContext.setLogLevel("WARN")

    // Input format: product,amount
    // Example: Laptop,50000
    val lines = ssc.socketTextStream("localhost", 9999)

    val sales: DStream[(String, Double)] = lines.flatMap { line =>
      val parts = line.trim.split(",")

      if (parts.length == 2) {
        try {
          val product = parts(0).trim
          val amount = parts(1).trim.toDouble

          if (product.nonEmpty && amount >= 0)
            Some((product, amount))
          else
            None
        } catch {
          case _: NumberFormatException => None
        }
      } else {
        None
      }
    }

    // 1. Count records in the latest 10-minute window
    val transactionCount = sales.countByWindow(
      Seconds(600),
      Seconds(30)
    )

    transactionCount.foreachRDD { rdd =>
      rdd.collect().foreach { count =>
        println(s"[10-MIN WINDOW] Transaction count: $count")
      }
    }

    // 2. Calculate rolling sales totals per product
    val rollingSales = sales.reduceByKeyAndWindow(
      (a: Double, b: Double) => a + b,
      Seconds(600),
      Seconds(30)
    )

    rollingSales.foreachRDD { rdd =>
      println("\n===== ROLLING SALES: LAST 10 MINUTES =====")
      rdd.collect().sortBy(_._1).foreach {
        case (product, total) =>
          println(f"$product -> $total%.2f")
      }
    }

    // 3. Detect a transaction spike in the latest 10-minute window.
    // Demo threshold: at least 5 transactions.
    transactionCount.foreachRDD { rdd =>
      rdd.collect().foreach { count =>
        if (count >= 5) {
          println(
            s"🚨 SPIKE ALERT: $count transactions in the last 10 minutes!"
          )
        }
      }
    }

    println("Day 25: Window Operations")
    println("Batch interval: 10 seconds")
    println("Window size: 10 minutes")
    println("Sliding interval: 30 seconds")
    println("Socket: localhost:9999")

    ssc.start()
    ssc.awaitTermination()
  }
}
