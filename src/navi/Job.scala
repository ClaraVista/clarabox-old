package navi

import spark.SparkContext
import spark._
import SparkContext._
import scala.collection.generic.Sorted
import scala.io.Source
import java.lang.Package
import com.mongodb.casbah.Imports._

trait timer {
  def time(f: => Any) = {
    val start = System.currentTimeMillis()
    f
    val end = System.currentTimeMillis()
    println("Total Execution Time: " + (end - start) / 1000f + " s")
  }
}

case class Date(d: String) {
  require(d.matches("\\d{2}/\\d{2}/\\d{4}"))
  def day = d.substring(0, 2).toInt
  def month = d.substring(3, 5).toInt
  def year = d.substring(6, 10).toInt
  def dayNumber = getDayNumber
  def weekNumber = getWeekNumber
  override def toString = day + "/" + month + "/" + year

  val monthDayNumber = Map(
    1 -> 2,
    2 -> 28,
    3 -> 31,
    4 -> 30,
    5 -> 31,
    6 -> 30,
    7 -> 31,
    8 -> 31,
    9 -> 30,
    10 -> 31,
    11 -> 30,
    12 -> 31)

  def getDayNumber = this.day + (1 until this.month).map(monthDayNumber).sum
  def getWeekNumber = getDayNumber / 7

  /*
   * 0 -> Sunday
   * 1 -> Monday
   * 2 -> Tuesday
   * ...
   * 6 -> Saturday
   */
  def getWeekDay(firstDaySWeekday: Int) = (getDayNumber + firstDaySWeekday - 1) % 7

}

object Calender {
  val current = "30/09/2011"
}

/*
     * 0 --> idClient
     * 1 --> idVisite 
	 * 2 --> page 
	 * 3 --> IdCategorie 
	 * 4 --> dateVisit 
	 * 5 --> nbPage
	 * 6 --> isFictif
	 * 7 --> isAuthent -->
     */

case class Row(idClient: String, idVisite: String, page: String, IdCategorie: Int, dateVisit: Date, nbPage: Int, isFictif: Boolean, isAuthent: Boolean) {
  def this(fields: Array[String]) = this(
    fields(0),
    fields(1),
    fields(2),
    fields(3) match { case "\\N" => -1; case x: String => x.toInt },
    new Date(fields(4)),
    fields(5).toInt,
    fields(6).toBoolean,
    fields(7).toBoolean)

  override def toString = idClient + ";" + idVisite + ";" + page + ";" + IdCategorie + ";" + dateVisit + ";" + nbPage + ";" + isFictif + ";" + isAuthent
  def getKey = (idClient, isFictif)
}

object Client {

  def parseToClient(rawFormat: (String, String, Seq[(String, String, String, Seq[(String, String, String)])])) = {
    new Client(rawFormat._1, rawFormat._2.toBoolean,
      rawFormat._3.map(v => Visite(v._1, Date(v._2), v._3.toBoolean,
        v._4.map(a => Affichage(a._1, a._2 match { case "\\N" => -1; case x: String => x.toInt }, a._3.toInt)))))
  }

  def parseToRaw(clt: Client) = {
    (clt.idClient, clt.isFictif, clt.visites.map(v =>
      (v.cookie, v.date, v.isAuthen, v.affichages.map(a =>
        (a.idPage, a.idCategorie, a.freq)))))
  }
}

case class Client(val idClient: String, val isFictif: Boolean, val visites: Seq[Visite]) {

  def C1(recent: Int) = visites
    .count(v =>
      (Date(Calender.current).dayNumber - v.date.dayNumber < recent)
        && (Date(Calender.current).dayNumber - v.date.dayNumber > 0))

  def C2(recent: Int) = visites
    .count(v =>
      (Date(Calender.current).dayNumber - v.date.dayNumber < recent * 7)
        && (Date(Calender.current).dayNumber - v.date.dayNumber > 0))

  def C3(recent: Int) = visites
    .count(v =>
      (Date(Calender.current).dayNumber - v.date.dayNumber < recent * 30.5f)
        && (Date(Calender.current).dayNumber - v.date.dayNumber > 0))

  def heat(day: Int, week: Int, month: Int) = (1 + C1(day)) * (1 + C2(week)) * (1 + C3(month))
}

case class Visite(cookie: String, date: Date, isAuthen: Boolean, affichages: Seq[Affichage]) {
  def nbPageVisited = affichages.map(_.freq).sum
}

case class Affichage(idPage: String, idCategorie: Int, freq: Int)

object Job extends App with timer {

  //  val objFilePath = "s3n://claravista.data.navigation/output/byg_navi_obj"
  //  val tableFilePath = "s3n://claravista.data.navigation/database/byg_clean/"

  val tableFilePath = "hdfs://n1.example.com/user/cloudera/data/sp/"
  val objFilePath = "hdfs://n1.example.com/user/cloudera/data/output/spsp/"
  val resFilePath = "hdfs://n1.example.com/user/cloudera/data/output/ressp/"

  System.setProperty("spark.serializer", "spark.KryoSerializer");
  //  System.setProperty("spark.default.parallelism", "32");
  System.setProperty("mapred.reduce.task", "200");
  System.setProperty("spark.akka.frameSize", "20");

  // local mode
  //  val sc = new SparkContext("local[2]", "job")

  // local cluster mode
  val sc = new SparkContext("spark://n1.example.com:7077", "job", System.getenv("SPARK_HOME"), Seq("/home/cloudera/Apps/job.jar", "/home/cloudera/Desktop/casbah-alldep_2.9.2-2.6.1.jar"))

  // EC2 mode
  //  val sc = new SparkContext("spark://"
  //    + Source.fromFile("/root/spark-ec2/masters").mkString.trim
  //    + ":7077", "job_dist",
  //    System.getenv("SPARK_HOME"), Seq("./target/scala-2.9.3/clarabox_2.9.3-1.0.jar"))

  def toClientAggInRawFormat(clients: spark.RDD[Array[String]]) = {
    clients
      .groupBy(r => (r(0), r(6), r(1), r(4), r(7)))
      .map(p => (p._1, p._2.map(aff => (aff(2), aff(3), aff(5)))))
      .groupBy(p => (p._1._1, p._1._2)) // idClient, isFictif
      .map(p => (p._1._1, p._1._2, p._2.map(v => (v._1._3, v._1._4, v._1._5, v._2))))
  }

  def toClientAgg(clients: spark.RDD[Row]) = {
    clients
      .groupBy(r => (r.idClient, r.isFictif, r.idVisite, r.dateVisit, r.isAuthent))
      .map(p => (p._1, p._2.map(aff => Affichage(aff.page, aff.IdCategorie, aff.nbPage))))
      .groupBy(p => (p._1._1, p._1._2)) // idClient, isFictif
      .map(p => Client(p._1._1, p._1._2, p._2.map(v => Visite(v._1._3, v._1._4, v._1._5, v._2))))
  }

  // read table, generate row based aggregation
  def fromTableToRawAgg = {
    val myFile = sc.textFile(tableFilePath)
    val data = myFile.map(line => line.split('\001')) //.map(new Row(_))
    val client_list = toClientAggInRawFormat(data)
    client_list.saveAsObjectFile(objFilePath)
    client_list
  }

  // load primitive type data and then transform in object Client
  def load = sc.objectFile[(String, String, Seq[(String, String, String, Seq[(String, String, String)])])](objFilePath).map(Client.parseToClient)

  // transform data in primitive type and serialize to S3
  def save(clt: spark.RDD[navi.Client]) = clt.map(Client.parseToRaw).saveAsObjectFile(objFilePath)

  def task = {
    val myFile = sc.textFile(tableFilePath)
    val data = myFile.map(line => line.split('\001')) //.map(new Row(_))
    val client_list = toClientAggInRawFormat(data)

    println(client_list.count)

    client_list.foreachPartition(iter => {
      val mongoColl = MongoClient("localhost", 27017)("clarabox")("navi")
      for (elem <- iter) {
        mongoColl += MongoDBObject(
          "idClient" -> elem._1,
          "isFictif" -> elem._2,
          "visites" -> elem._3.map(v => MongoDBObject(
            "cookie" -> v._1,
            "date" -> v._2,
            "isAuthen" -> v._3,
            "Affichages" -> v._4.map(a => MongoDBObject(
              "idPage" -> a._1,
              "idCategorie" -> a._2,
              "freq" -> a._3)))))
      }
    })
  }

  def runTask = time(task)

  runTask
}