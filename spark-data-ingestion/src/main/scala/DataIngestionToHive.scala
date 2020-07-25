import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geosparksql.utils.Adapter

object DataIngestionToHive {

  val dataName: List[String] = List("IND_rrd", "IND_rds")
  var hdfsPath =  "hdfs://localhost:9000/user/Durga.Shanker/geo_data/india_22062020/"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("My app")
    val sc = new SparkContext(conf)

    val spark: SparkSession =
      SparkSession
        .builder().master("local")
        .appName("HR2")
        .enableHiveSupport()
        .getOrCreate()

    spark.sql("SHOW DATABASES").show
    spark.sql("CREATE DATABASE IF NOT EXISTS hackathon_r2")


    dataName.foreach { geoName =>
      val spatialRDD = ShapefileReader.readToGeometryRDD(sc, hdfsPath + geoName)
      val spatialDf = Adapter.toDf(spatialRDD, spark)
      spatialDf.write.saveAsTable("hackathon_r2." + geoName)
      spark.sql("SELECT * FROM hackathon_r2." + geoName).show(true)
      spark.sql("SHOW DATABASES").show
      spark.sql("use hackathon_r2")
      spark.sql("show tables").show()

    }
  }
}
