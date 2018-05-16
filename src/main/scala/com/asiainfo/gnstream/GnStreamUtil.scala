package com.asiainfo.gnstream

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}

import com.asiainfo.util.MD5RowKeyGenerator
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

/**
  * Created by migle on 2016/9/27.
  * 流量详单数据格式处理工具类
  */
object GnStreamUtil {
  private val log = LoggerFactory.getLogger("GnStreamUtil")
  private val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS")

  /**
    * 格式化数据，总共传给我们的12个字段
    * @param m 传入map
    * @return
    */
  def format(m: (String, String)): Map[String, String] = {
    log.debug(m.toString())

    if (Some(m._1).isEmpty || Some(m._2).isEmpty) {
      log.warn("error data:" + m.toString())
      return Map[String, String]()
    }
    //TODO 有缺失字段的记录要不要过滤还需要业务方确认？
    val msgs = if (m._2.endsWith("|")) (m._2 + " ").split("\\|") else m._2.split("\\|")

    if (msgs.size < 10 || msgs.size > 12) {
      log.warn("error data format:" + m.toString())
      return Map[String, String]()
    }

    try {
      val pubmap = Map(
        "s_topic" -> m._1,
        "sflag" -> m._1.split("_")(2),
        //格式化手机号码
        "msisdn" -> regPhoneNo(msgs(0)),
        //拼装app大小类
        "busi_id" -> (msgs(1) + "-" + msgs(2)),
        //imei
        "imei" -> msgs(3),
        //合并上下行流量
        "flow" -> (msgs(4).toLong + msgs(5).toLong).toString,
        //开始时间、结束时间
        "start_time" -> LocalDateTime.ofInstant(Instant.ofEpochMilli(msgs(6).substring(0, 13).toLong), ZoneId.of("Asia/Shanghai")).format(dtf),
        "end_time" -> LocalDateTime.ofInstant(Instant.ofEpochMilli(msgs(7).substring(0, 13).toLong), ZoneId.of("Asia/Shanghai")).format(dtf)
      )

      //对不同topic消息分别处理：gn类型的topic和let的topic字段不一致
      val primap = m._1 match {
        case "topic_gn_general"
             | "topic_gn_dns"
             | "topic_gn_mms"
             | "topic_gn_ftp"
             | "topic_gn_email"
             | "topic_gn_voip"
             | "topic_gn_im"
             | "topic_gn_p2p" => Map("chargeid" -> msgs(8),
          "rat" -> msgs(9), "url" -> "", "server_ip" -> msgs(10))

        /*12个字段*/
        case "topic_gn_http" | "topic_gn_rtsp" => Map("chargeid" -> msgs(8),
          "rat" -> msgs(9), "url" -> msgs(10).trim, "server_ip" -> msgs(11))

        /*10个字段,没有chargeid,url*/
        case "topic_lte_general"
             | "topic_lte_dns"
             | "topic_lte_mms"
             | "topic_lte_ftp"
             | "topic_lte_email"
             | "topic_lte_voip"
             | "topic_lte_im"
             | "topic_lte_p2p" => Map("chargeid" -> "", "rat" -> msgs(8), "url" -> "", "server_ip" -> msgs(9))

        /*11个字段,没有chargeid*/
        case "topic_lte_http" | "topic_lte_rtsp" => Map("chargeid" -> "",
          "rat" -> msgs(8), "url" -> msgs(9).trim, "server_ip" -> msgs(10))

        case _ => Map()
      }

      pubmap ++ primap
    } catch {
      case NonFatal(e) => log.warn("error format data:" + m.toString())
        Map()
    }
  }

  /**
    * 原有的rowkey生成算法是(TableRowKeyGenerator.generateByGenRKStep)：
    * 手机号有86/+86的先去掉，
    * md5(phone_no)[1,3,5] + phone_no + end_time(yyyyMMddHHmmss) + md5(line)[1,3,5]
    * 但是原有内容不清楚
    *
    * @param msisdn 生成rowkey
    * @return
    */
  def getRowkey(msisdn: String, time: String, line: String) = {
    val rowkey = new MD5RowKeyGenerator().generatePrefix(msisdn) + msisdn + time + new MD5RowKeyGenerator().generatePrefix(line)
    rowkey
  }

  /**
    * 参照以前的处理方式：取掉手机号前的86|+86
    *
    * @param old 处理86和+86的号码
    * @return
    */
  def regPhoneNo(old: String): String = {
    old.replaceAll("^86|\\+86", "")
  }
}

/** *
  * topic格式，字段间用"|"分隔
  * topic_gn_general	msisdn,app_type,app_sub_type,imei,ul_data,dl_data,start_time,end_time,chargeid,rat,    server_ip
  * topic_gn_dns		msisdn,app_type,app_sub_type,imei,ul_data,dl_data,start_time,end_time,chargeid,rat,    server_ip
  * topic_gn_mms		msisdn,app_type,app_sub_type,imei,ul_data,dl_data,start_time,end_time,chargeid,rat,    server_ip
  * topic_gn_ftp		msisdn,app_type,app_sub_type,imei,ul_data,dl_data,start_time,end_time,chargeid,rat,    server_ip
  * topic_gn_email		msisdn,app_type,app_sub_type,imei,ul_data,dl_data,start_time,end_time,chargeid,rat,    server_ip
  * topic_gn_voip		msisdn,app_type,app_sub_type,imei,ul_data,dl_data,start_time,end_time,chargeid,rat,    server_ip
  * topic_gn_im			msisdn,app_type,app_sub_type,imei,ul_data,dl_data,start_time,end_time,chargeid,rat,    server_ip
  * topic_gn_p2p		msisdn,app_type,app_sub_type,imei,ul_data,dl_data,start_time,end_time,chargeid,rat,    server_ip
  * topic_gn_http		msisdn,app_type,app_sub_type,imei,ul_data,dl_data,start_time,end_time,chargeid,rat,url,server_ip
  * topic_gn_rtsp		msisdn,app_type,app_sub_type,imei,ul_data,dl_data,start_time,end_time,chargeid,rat,url,server_ip
  * *
  * topic_lte_general	msisdn,app_type,app_sub_type,imei,ul_data,dl_data,start_time,end_time,		   rat,    app_server_ip_ipv4
  * topic_lte_dns    	msisdn,app_type,app_sub_type,imei,ul_data,dl_data,start_time,end_time,		   rat,    app_server_ip_ipv4
  * topic_lte_mms    	msisdn,app_type,app_sub_type,imei,ul_data,dl_data,start_time,end_time,		   rat,    app_server_ip_ipv4
  * topic_lte_ftp    	msisdn,app_type,app_sub_type,imei,ul_data,dl_data,start_time,end_time,		   rat,    app_server_ip_ipv4
  * topic_lte_email  	msisdn,app_type,app_sub_type,imei,ul_data,dl_data,start_time,end_time,		   rat,    app_server_ip_ipv4
  * topic_lte_voip   	msisdn,app_type,app_sub_type,imei,ul_data,dl_data,start_time,end_time,		   rat,    app_server_ip_ipv4
  * topic_lte_im     	msisdn,app_type,app_sub_type,imei,ul_data,dl_data,start_time,end_time,		   rat,    app_server_ip_ipv4
  * topic_lte_p2p 		msisdn,app_type,app_sub_type,imei,ul_data,dl_data,start_time,end_time,		   rat,    app_server_ip_ipv4
  * *
  * topic_lte_http   	msisdn,app_type,app_sub_type,imei,ul_data,dl_data,start_time,end_time,		   rat,url,app_server_ip_ipv4
  * topic_lte_rtsp   	msisdn,app_type,app_sub_type,imei,ul_data,dl_data,start_time,end_time,		   rat,url,app_server_ip_ipv4
  * */