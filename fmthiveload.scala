import org.apache.spark.sql.SparkSession



object fmthiveload {

  def main(arg: Array[String]): Unit ={
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("spark")
      .config("spark.sql.warehouse.dir","user/hive/warehouse")
      .config("hive.metastore.uris","thrift://localhost:9083")
      .enableHiveSupport()
      .getOrCreate()

    val listOfDBs = sparkSession.sqlContext.sql("show databases")
    val xmlload= sparkSession.sqlContext.read.format("com.databricks.spark.xml")
       .option("rowtag","record")
        .load("file:///home/acadgild/project/data/web/file.xml")
    xmlload.createOrReplaceTempView("webfile")
    xmlload.show()


    //listOfDBs.show(4,false)
    //# load  data into formatted table
    sparkSession.sqlContext.sql("use project")
    sparkSession.sqlContext.sql("show tables").show()
     sparkSession.sqlContext.sql("LOAD DATA LOCAL INPATH '/home/acadgild/project/data/mob/file.txt' into table project.formatted_input")
    sparkSession.sqlContext.sql(" insert into project.formatted_input select user_id,song_id,artist_id,unix_timestamp(timestamp,'yyyy-mm-dd hh:mm:ss') as timestamp," +
    "unix_timestamp(start_ts,'yyyy-mm-dd hh:mm:ss') as start_ts,unix_timestamp(end_ts,'yyyy-mm-dd hh:mm:ss') as end_ts," +
    "geo_cd,station_id,song_end_type,like,dislike from webfile ")

    //# create enrichtable and load the enrich data after filter
    sparkSession.sqlContext.sql("create table if not exists project.enrich_load_data select * from project.formatted_input where 1=2")
    sparkSession.sqlContext.sql("alter table project.enrich_load_data add columns(status STRING)")
    sparkSession.sqlContext.sql("insert into project.enrich_load_data SELECT fmt.user_id,fmt.song_id, sam.artist_id, fmt.time_stamp, fmt.start_ts, fmt.end_ts, stg.geo_cd, fmt.station_id,IF (fmt.song_end_type IS NULL, 3, fmt.song_end_type) AS song_end_type, IF (fmt.likee IS NULL, 0, fmt.likee) AS likee, IF (fmt.dislike IS NULL, 0, fmt.dislike) AS dislike, IF((fmt.likee=1 AND fmt.dislike=1) OR fmt.user_id IS NULL OR fmt.song_id IS NULL  OR fmt.time_stamp IS NULL OR fmt.start_ts IS NULL OR fmt.end_ts IS NULL OR fmt.geo_cd IS NULL  OR fmt.user_id=''  OR fmt.song_id=''  OR fmt.time_stamp='' OR fmt.start_ts='' OR fmt.end_ts='' OR fmt.geo_cd=''  OR stg.geo_cd IS NULL  OR stg.geo_cd='' OR sam.artist_id IS NULL  OR sam.artist_id='', 'False', 'True') AS status FROM project.formatted_input fmt LEFT OUTER JOIN project.station_geo_map stg ON fmt.station_id = stg.station_id LEFT OUTER JOIN project.song_artist_map sam ON fmt.song_id = sam.song_id" )


  }

}
