import org.apache.spark.sql.SparkSession

object spark_hive_dataanalysis {
  def main(arg: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("spark")
      .config("spark.sql.warehouse.dir","user/hive/warehouse")
      .config("hive.metastore.uris","thrift://localhost:9083")
      .enableHiveSupport()
      .getOrCreate()

    sparkSession.sql("use project")
    //sparkSession.sql("INSERT INTO TABLE project.top_10_stations SELECT station_id, count(DISTINCT song_id) AS total_distinct_songs_played, COUNT(DISTINCT user_id) AS distinct_user_count from  project.enrich_load_data WHERE  likee=1 and status='True' GROUP BY station_id ORDER BY total_distinct_songs_played DESC LIMIT 10")
    //sparkSession.sql("select * from project.enrich_load_data").show(40)//INSERT OVERWRITE TABLE top_10_stations
    sparkSession.sql("SELECT CASE WHEN (su.user_id IS NULL OR CAST(eld.time_stamp AS DECIMAL(20,0)) > CAST(su.subscn_end_dt AS DECIMAL(20,0))) THEN 'UNSUBSCRIBED' WHEN (su.user_id IS NOT NULL AND CAST(eld.time_stamp AS DECIMAL(20,0)) <= CAST(su.subscn_end_dt  AS DECIMAL(20,0))) THEN 'SUBSCRIBED' END AS user_type, SUM(ABS(CAST(eld.end_ts AS DECIMAL(20,0))-CAST(eld.start_ts AS DECIMAL(20,0)))) AS duration FROM project.enrich_load_data eld LEFT OUTER JOIN subscribed_users su ON eld.user_id=su.user_id WHERE eld.status='True' GROUP BY CASE WHEN (su.user_id IS NULL OR CAST(eld.time_stamp AS DECIMAL(20,0)) > CAST(su.subscn_end_dt  AS DECIMAL(20,0))) THEN 'UNSUBSCRIBED' WHEN (su.user_id IS NOT NULL AND CAST(eld.time_stamp AS DECIMAL(20,0)) <= CAST (su.subscn_end_dt AS DECIMAL(20,0))) THEN 'SUBSCRIBED' END").show()
    sparkSession.sql("INSERT OVERWRITE TABLE users_behaviour SELECT CASE WHEN (su.user_id IS NULL OR CAST(eld.time_stamp AS DECIMAL(20,0)) > CAST(su.subscn_end_dt AS DECIMAL(20,0))) THEN 'UNSUBSCRIBED' WHEN (su.user_id IS NOT NULL AND CAST(eld.time_stamp AS DECIMAL(20,0)) <= CAST(su.subscn_end_dt  AS DECIMAL(20,0))) THEN 'SUBSCRIBED' END AS user_type, SUM(ABS(CAST(eld.end_ts AS DECIMAL(20,0))-CAST(eld.start_ts AS DECIMAL(20,0)))) AS duration FROM project.enrich_load_data eld LEFT OUTER JOIN subscribed_users su ON eld.user_id=su.user_id WHERE eld.status='True' GROUP BY CASE WHEN (su.user_id IS NULL OR CAST(eld.time_stamp AS DECIMAL(20,0)) > CAST(su.subscn_end_dt  AS DECIMAL(20,0))) THEN 'UNSUBSCRIBED' WHEN (su.user_id IS NOT NULL AND CAST(eld.time_stamp AS DECIMAL(20,0)) <= CAST (su.subscn_end_dt AS DECIMAL(20,0))) THEN 'SUBSCRIBED' END")
    sparkSession.sql("SELECT ua.artist_id, COUNT(DISTINCT ua.user_id) AS user_count FROM ( SELECT user_id, artist_id FROM project.users_artists  LATERAL VIEW explode(artists_array) artists AS artist_id ) ua INNER JOIN ( SELECT artist_id, song_id, user_id FROM project.enrich_load_data  WHERE status='True'  ) eld ON ua.artist_id=eld.artist_id AND ua.user_id=eld.user_id GROUP BY ua.artist_id ORDER BY user_count DESC LIMIT 10").show()
    sparkSession.sql("INSERT OVERWRITE TABLE connected_artists SELECT ua.artist_id, COUNT(DISTINCT ua.user_id) AS user_count FROM ( SELECT user_id, artist_id FROM project.users_artists  LATERAL VIEW explode(artists_array) artists AS artist_id ) ua INNER JOIN ( SELECT artist_id, song_id, user_id FROM project.enrich_load_data  WHERE status='True'  ) eld ON ua.artist_id=eld.artist_id AND ua.user_id=eld.user_id GROUP BY ua.artist_id ORDER BY user_count DESC LIMIT 10")
  sparkSession.sql("SELECT song_id, SUM(ABS(CAST(end_ts AS DECIMAL(20,0))-CAST(start_ts AS DECIMAL(20,0)))) AS duration  FROM project.enrich_load_data WHERE status='True'  AND (likee=1 OR song_end_type=0) GROUP BY song_id ORDER BY duration DESC LIMIT 10").show()
    sparkSession.sql("INSERT OVERWRITE TABLE top_10_royalty_songs SELECT song_id, SUM(ABS(CAST(end_ts AS DECIMAL(20,0))-CAST(start_ts AS DECIMAL(20,0)))) AS duration  FROM project.enrich_load_data WHERE status='True'  AND (likee=1 OR song_end_type=0) GROUP BY song_id ORDER BY duration DESC LIMIT 10")
    sparkSession.sql("SELECT eld.user_id, SUM(ABS(CAST(eld.end_ts AS DECIMAL(20,0))-CAST(eld.start_ts AS DECIMAL(20,0)))) AS duration  FROM project.enrich_load_data eld LEFT OUTER JOIN project.subscribed_users su ON eld.user_id=su.user_id WHERE eld.status='True'  AND (su.user_id IS NULL OR (CAST(eld.time_stamp AS DECIMAL(20,0)) > CAST(su.subscn_end_dt AS DECIMAL(20,0)))) GROUP BY eld.user_id ORDER BY duration DESC LIMIT 10").show()
    sparkSession.sql("INSERT OVERWRITE TABLE top_10_unsubscribed_users SELECT eld.user_id, SUM(ABS(CAST(eld.end_ts AS DECIMAL(20,0))-CAST(eld.start_ts AS DECIMAL(20,0)))) AS duration  FROM project.enrich_load_data eld LEFT OUTER JOIN project.subscribed_users su ON eld.user_id=su.user_id WHERE eld.status='True'  AND (su.user_id IS NULL OR (CAST(eld.time_stamp AS DECIMAL(20,0)) > CAST(su.subscn_end_dt AS DECIMAL(20,0)))) GROUP BY eld.user_id ORDER BY duration DESC LIMIT 10")
  }


}
