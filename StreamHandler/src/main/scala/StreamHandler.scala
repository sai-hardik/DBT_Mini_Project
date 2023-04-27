import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.sql.cassandra._

import com.datastax.oss.driver.api.core.uuid.Uuids 
import com.datastax.spark.connector._             

case class DeviceData(device: String, size: Double, fee: Double, volume: Double)

object StreamHandler {
	def main(args: Array[String]) {

		// initialize Spark
		val spark = SparkSession
			.builder
			.appName("Stream Handler")
			.config("spark.cassandra.connection.host", "localhost")
			.getOrCreate()

		import spark.implicits._

		// read from Kafka
		val inputDF = spark
			.readStream
			.format("kafka") // org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5
			.option("kafka.bootstrap.servers", "localhost:9092")
			.option("subscribe", "crypto")
			.load()

		// only select 'value' from the table,
		// convert from bytes to string
		val rawDF = inputDF.selectExpr("CAST(value AS STRING)").as[String]

		// split each row on comma, load it to the case class
		val expandedDF = rawDF.map(row => row.split(","))
			.map(row => DeviceData(
				row(1),
				row(2).toDouble,
				row(3).toDouble,
				row(4).toDouble
			))

		// groupby and aggregate
		val summaryDf = expandedDF
			.groupBy("device")
			.agg(avg("size"), avg("fee"), avg("volume"))

		// create a dataset function that creates UUIDs
		val makeUUID = udf(() => Uuids.timeBased().toString)

		// add the UUIDs and renamed the columns
		// this is necessary so that the dataframe matches the 
		// table schema in cassandra
		val summaryWithIDs = summaryDf.withColumn("uuid", makeUUID())
			.withColumnRenamed("avg(size)", "size")
			.withColumnRenamed("avg(fee)", "fee")
			.withColumnRenamed("avg(volume)", "volume")

		// write dataframe to Cassandra
		val query = summaryWithIDs
			.writeStream
			//.format("console")
			.trigger(Trigger.ProcessingTime("1 seconds"))
			.foreachBatch { (batchDF: DataFrame, batchID: Long) =>
				println(s"Writing to Cassandra $batchID")
				batchDF.write
					.cassandraFormat("crypto_a", "stuff") // table, keyspace
					.mode("append")
					//.format("console")
					.save()
					//.format("console")
					println(s"Batch $batchID")
					batchDF.show(false)
			}
			//.format("console")
			.outputMode("update")
			//.format("console")
			.start()

		// until ^C
		//format("console")
		query.awaitTermination()
	}
}
