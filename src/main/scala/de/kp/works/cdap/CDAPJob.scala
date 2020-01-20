package de.kp.works.cdap
/*
 * Copyright (c) 2019 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 * 
 */
import java.sql.Timestamp
import java.time.Instant

import java.util.{List => JList, Map => JMap}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

case class MetaInfo(maxDataPoints: Double, intervalMs: Double, startTime: Long, endTime: Long)

class CDAPJob {
    
  private val props = CDAPConf.getProps
  private val context = new CDAPContext(props)
  
  
  def getMetrics(request:JMap[String,Object]):JList[JMap[String,Object]] = {
    
    val metaInfo = getMetaInfo(request)
    val targets = getTargets(request)

    val metrics = targets.map(target => getMetric(target._1, target._2, metaInfo))
    metrics.asJava
    
  }
  
  private def getMetaInfo(request:JMap[String,Object]):MetaInfo = {
    /*
     * The number of data points is a control information
     * to make sure that we do not exceed this UI limit
     */
    val maxDataPoints = request.get("maxDataPoints").asInstanceOf[Double]
    /*
     * This parameter describes the time distance of the data
     * points to return
     */
    val intervalMs = request.get("intervalMs").asInstanceOf[Double]
    /*
     * The time range object that specifies start and end
     * of the time metric
     */
    val timeRange = request.get("range").asInstanceOf[JMap[String,Object]]
    
    val from = timeRange.get("from").asInstanceOf[String]
    val to = timeRange.get("to").asInstanceOf[String]
    
    val startTime = Timestamp.from(Instant.parse(from)).getTime
    val endTime = Timestamp.from(Instant.parse(to)).getTime
    
    MetaInfo(maxDataPoints, intervalMs, startTime, endTime)
    
  }
  
  private def getTargets(request:JMap[String,Object]): List[(String, String)] = {
    
    val targets = request.get("targets").asInstanceOf[JList[JMap[String,Object]]]
    
    val tables = targets.map(target => {

      val tableName = target.get("target").asInstanceOf[String]
      /*
       * - table
       * - timeserie
       */
      val tableType = target.get("type").asInstanceOf[String]
      
      (tableName, tableType)

    }).toList
    
    tables
    
  }
  
  private def getMetric(tableName:String,tableType:String,metaInfo:MetaInfo):JMap[String,Object] = {
    /*
     * Build dummy response to check whether interaction
     * with Grafana is working as required
     */
    
    if (tableType == "table") {
      throw new Exception("not implemented yet")

    } else if (tableType == "timeserie") {
      
      /*
       * Build data points
       */
      val rand = scala.util.Random
      val datapoints = (0 until metaInfo.maxDataPoints.toInt).map(point => {
        
        val ts = metaInfo.startTime + point * metaInfo.intervalMs.toLong
        val v = rand.nextInt(100).toDouble
        
        List(v,ts).asJava
        
      }).toList.asJava
      
      Map("target" -> tableName, "datapoints" -> datapoints).asJava
      
    } else {
      throw new Exception(s"[CDAPJob] target type '${tableType}' is not supported.")
    }
    
  }
  
  /**
   * Delegate retrieval of datasets of type 'table' 
   * to CDAP context
   */
  def getTables(request:JMap[String,Object]):List[String] = {
    
    val tableType = "co.cask.cdap.api.dataset.table.Table"    
    val datasets = context.getDatasets()
    
    if (datasets.isEmpty) return List.empty[String]
    
    /* Restrict datasets to tables */
    val tables = datasets.filter(dataset => dataset.datasetType == tableType)
    tables.map(table => table.datasetName) 
   
  }
  
}

object CDAPJob {
  
  def main(args:Array[String]) {
    
    CDAPConf.init()
    
    val job = new CDAPJob()
    
  }
}