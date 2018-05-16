package com.asiainfo.gnstream

import java.time.{LocalDate, LocalTime}

import com.asiainfo.util.Conf
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.slf4j.LoggerFactory

/**
  * Created by migle on 2016/9/27.
  * 上网日志查询后台数据处理程序
  * 此程序从Kafka中接收并处理数据，处理完成后写入HDFS,后续会有程序通过接口机从集群1传输至集群3中并加载至HBase中供前台查询。
  * spark-submit  --num-executors 12 --executor-memory 40g --executor-cores 10  --class "GnStreamOnHW"   aistream-qcd-spark-app.jar  60 100
  *
  */
object GnStreamOnHW {
  private val log = LoggerFactory.getLogger("GnStreamOnHW")

  def gnStreamContext(chkdir: String, batchDuration: Duration, partitions: Int, nameNo: String, topicStr: String,fileCount: Int) = {
    val topics = topicStr.split(",").toSeq

    val conf = new SparkConf().setAppName("GnStreamOnHW" + nameNo)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf, batchDuration)
    //读取终端维表
    val dimTermInfo = ssc.sparkContext.textFile("/yx_qcd/gnstream/dim/imei_terminal_base_info.dat").map({
      line => {
        val tmp = line.split("\\|")
        if (tmp.length == 3) {
          (tmp(0), (tmp(1), tmp(2)))
        } else {
          log.error("imei_terminal_base_info.dat中有错误数据:" + line)
          (tmp(0), ("", ""))
        }
      }
    }).collect().toMap

    val terminfos = ssc.sparkContext.broadcast(dimTermInfo)

    //kafka连接属性配置
    val kafkaParams = Map[String, String]("metadata.broker.list" -> Conf.kafka, "group.id" -> Conf.groupid)
    //创建kafka连接
    val dstreams = topics.map(topic => {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(topic)).map(x => (topic, x._2))
    })
    val messages = ssc.union(dstreams).map(GnStreamUtil.format(_)).filter(!_.isEmpty)
    //val messages = ssc.union(dstreams).repartition(partitions).map(GnStreamUtil.format(_)).filter(!_.isEmpty)
    val data = messages.mapPartitions(p => {
      p.map(line => {
        //关联处理终端信息
        val tac = if (line.get("imei").isEmpty || line.get("imei").get.length <= 8) "00000000" else line.get("imei").get.substring(0, 8)
        val terminfo = terminfos.value.getOrElse(tac, ("", ""))

        //siteFlag是废弃字段，直接置空，可以考虑去掉，本此仅迁移集群，这个字段仍旧保留
        val siteFlag = ""
        //取url，仅http、rtsp协议有url字段，没有的均置为空，并且仅保留30个字符长度
        val oldurl = line.getOrElse("url", "");
        val url = if (oldurl.length > 30) oldurl.substring(0, 30) else oldurl

        val row = Array(line.getOrElse("busi_id", ""),
          terminfo._1,
          terminfo._2,
          line.getOrElse("flow", ""),
          line.getOrElse("start_time", ""),
          line.getOrElse("end_time", ""),
          line.getOrElse("chargeid", ""),
          line.getOrElse("rat", ""),
          url,
          siteFlag,
          line.getOrElse("server_ip", "")
        )
        //拼装最终结果
        GnStreamUtil.getRowkey(line("msisdn"), line("start_time").substring(0, 13), row.mkString("")) + "|" + row.mkString("|") + "|" + line("sflag")
      })
    })/*.persist(StorageLevel.MEMORY_ONLY_SER)*/

    //写入hdfs、文件系统
    data.foreachRDD(rdd => {
      val fname = "hdfs://hacluster/yx_qcd/gnstream/gndata/" + LocalDate.now().toString + "-" + LocalTime.now().toString.replaceAll(":", "-") + "-" + nameNo + "-dat"
      rdd.coalesce(fileCount).saveAsTextFile(fname, classOf[org.apache.hadoop.io.compress.GzipCodec])
    })
    ssc.checkpoint(chkdir)
    ssc
  }


  def main(args: Array[String]): Unit = {
    val chkdir = "/yx_qcd/chkpoint/gnstream-chkpoint" + args(2).toString
    val chkssc = StreamingContext.getOrCreate(chkdir, () => gnStreamContext(chkdir, Seconds(args(0).toInt), args(1).toInt, args(2).toString, args(3).toString,args(4).toInt))
    chkssc.start()
    chkssc.awaitTermination()
  }
}


