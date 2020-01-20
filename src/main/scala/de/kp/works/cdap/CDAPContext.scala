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

import java.util.{Date, Properties}
import java.time.Duration

import co.cask.cdap.client._
import co.cask.cdap.client.config._
import co.cask.cdap.client.proto._

import co.cask.cdap.security.authentication.client.basic._
import co.cask.cdap.proto.ProgramType
import co.cask.cdap.proto.QueryResult
import co.cask.cdap.proto.artifact.AppRequest
import co.cask.cdap.proto.id.{ApplicationId,ArtifactId,DatasetId,NamespaceId,ProgramId}

import com.google.gson.{Gson,JsonObject}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.mutable.{ArrayBuffer, HashMap}

/*
 * https://docs.cask.co/cdap/6.0.0-SNAPSHOT/en/reference-manual/java-client-api.html#application-client
 */

class CDAPContext(props:Properties) {
  
  private val USERNAME_PROP_NAME = "security.auth.client.username"
  private val PASSWORD_PROP_NAME = "security.auth.client.password"
  /*
   * Extract CDAP configuration and build CDAP context
   * either secure or non-secure
   */
  private val host = props.getProperty("host")
  private val port = props.getProperty("port").toInt
  
  private val sslEnabled = {
    
    val prop = props.getProperty("sslEnabled")
    if (prop == "yes") true else false
  
  }
  
  private val alias = {
    
    val prop = props.getProperty("alias")
    if (prop.isEmpty) None else Some(prop)
    
  }
  private val password = {
    
    val prop = props.getProperty("password")
    if (prop.isEmpty) None else Some(prop)
    
  }
  
  private val namespace = {

    val prop = props.getProperty("namespace")
    if (prop.isEmpty) "default" else prop

  }
  
  
  private val connConfig = new ConnectionConfig(host, port, sslEnabled)

  private val clientConfig = if (sslEnabled) {
    /*
     * Build client configuration for a secure CDAP instance
     */
    if (alias.isDefined == false || password.isDefined == false)
      throw new Exception("[ERROR] SSL connection requires alias and password.")
    
    val props = new Properties()
    props.setProperty(USERNAME_PROP_NAME, alias.get)
    props.setProperty(PASSWORD_PROP_NAME, password.get)
    
    val authClient = new BasicAuthenticationClient()
    authClient.configure(props)
    
    authClient.setConnectionInfo(host, port, sslEnabled)
    val accessToken = authClient.getAccessToken()

    ClientConfig.builder()
      .setConnectionConfig(connConfig)
      .setAccessToken(accessToken)
      .build()
    
  } else {
    /*
     * Build client configuration for a non-secure CDAP instance
     */
    ClientConfig.builder()
      .setConnectionConfig(connConfig)
      .build()    
  }
  /*
   * Build dataset client to access CDAP dataset REST service
   */
  private val datasetClient = new DatasetClient(clientConfig)
  /*
   * Build metrics client to access CDAP metrics REST service
   */
  private val metricsClient = new MetricsClient(clientConfig)
  /*
   * Build monitor client to access CDAP monitor REST service
   */
  private val monitorClient = new MonitorClient(clientConfig)
  /*
   * Build query client to access CDAP query REST service
   */
  private val queryClient = new QueryClient(clientConfig)
  
  /********************************
   * 
   * DATASET SUPPORT
   * 
   *******************************/
  
  /**
   * A public method to retrieve all dataset that are assigned to 
   * a specific namespace
   */
  def getDatasets():List[CDAPDataset] = {
    
    val nsID = NamespaceId.fromIdParts(List(namespace))
    val datasets = datasetClient.list(nsID)
    
    datasets.map(dataset => {
      
      val datasetName = dataset.getName
      val datasetDesc = dataset.getDescription
      
      val datasetType = dataset.getType
      val datasetProps = dataset.getProperties.asScala.toMap
      
      CDAPDataset(namespace=namespace, datasetName=datasetName, datasetDesc=datasetDesc,datasetType=datasetType,datasetProps=datasetProps)
      
    }).toList
    
  }
  /**
   * A public method to retrieve the specification of a certain dataset
   */
  def getDataset(namespace:String,datasetName:String):CDAPDatasetSpec = {
    
    val dsID = getDatasetId(namespace,datasetName)
    val dataset = datasetClient.get(dsID)
    
    val spec = dataset.getSpec
    val datasetDesc = spec.getDescription

    val datasetType = spec.getType
    val datasetProps = spec.getProperties.asScala.toMap
    
    val hiveTableName = dataset.getHiveTableName
    val ownerPrincipal = dataset.getOwnerPrincipal
     
    CDAPDatasetSpec(
      namespace=namespace, 
      datasetName=datasetName, 
      datasetDesc=datasetDesc,
      datasetType=datasetType,
      datasetProps=datasetProps,
      hiveTableName=hiveTableName,
      ownerPrincipal=ownerPrincipal
    )

  }
  
  def getDatasetId(namespace:String,datasetName:String):DatasetId = {
    new DatasetId(namespace,datasetName)
  }
  
  /********************************
   * 
   * METRIC SUPPORT
   * 
   *******************************/
  
  /**
   * This method retrieves the internal metric names that are available 
   * for application related metrics context   
   */
  def getMetrics(namespace:String,context:java.util.Map[String,String]):CDAPMetrics = {

    val tags = getMetricTags(namespace,context)
    if (tags == null)
      throw new Exception("[ERROR] Provided context is not valid")
    
    val metrics = getMetrics(tags)

    CDAPMetrics(metrics=metrics,metricTags=tags)
    
  }
  
  def getMetricTags(namespace:String,context:java.util.Map[String,String]):Map[String,String] = {
    
    if (context == null || context.isEmpty)
      /*
       * Retrieve metric context for all applications
       * of a certain namespace
       */
      return MetricsUtil.getAllApp(namespace)

    /*
     * Determine whether this context specifies a certain 
     * metric type  
     */
    if (context.contains("metricType") == false) {
      /*
       * This request its restricted to a certain application
       * and must provide the respective app name
       */
      if (context.contains("appName") == false)
        return null
        
      else {
        /* Retrieve tags for a certain application 
         * without further context information
         */
        val appName = context("appName")
        return MetricsUtil.getApp(namespace, appName)
      }  
    
    } else {
      
      val metricType = context("metricType")
      
      metricType match {
        case "dataset" => {
          if (context.contains("datasetName") == false)
            /*
             * Retrieve metrics tags for all dataset of 
             * a namespace
             */          
            return MetricsUtil.getAllDataset(namespace)
          
          else {
            val datasetName = context("datasetName")
            if (context.contains("appName") == false) {
              /*
               * Retrieve metrics tags for a certain dataset 
               * of a namespace
               */
              return MetricsUtil.getDataset(namespace, datasetName)
           }
            else {
               val appName = context("appName")
               /*
                * Retrieve metrics tags for a certain dataset of an 
                * application
                */
               return MetricsUtil.getAppDataset(namespace, appName, datasetName)
            }
          }
        }
        case "flow" => {
          if (context.contains("appName") == false)
            return null
            
          else {
            val appName = context("appName")
            if (context.contains("flowName") == false) 
              /*
               * Retrieve metrics tags for all flows of 
               * an application
               */
              return MetricsUtil.getAllFlow(namespace, appName)
              
            else {
              /*
               * Retrieve metrics tags for a certain flow 
               * of an application
               */
              val flowName = context("flowName")
              return MetricsUtil.getFlow(namespace, appName, flowName)
            }
          }          
        }   
        case "mapreduce" => {
          if (context.contains("appName") == false)
            return null
            
          else {
            val appName = context("appName")
            if (context.contains("mapreduceName") == false)
              /*
               * Retrieve metrics tags for all mapreduce programs 
               * of an application
               */
              return MetricsUtil.getAllMapReduce(namespace, appName)
              
             else {
               /*
                * Retrieve metrics tags for a certain mapreduce 
                * of an application
                */
               val mapreduceName = context("mapreduceName")
               return MetricsUtil.getMapReduce(namespace, appName, mapreduceName)
             }
          }          
        }                                
        case "service" => {
          if (context.contains("appName") == false)
            return null
            
          else {
            val appName = context("appName")
            if (context.contains("serviceName") == false)
              /*
               * Retrieve metrics tags for all services of 
               * an application
               */            
              return MetricsUtil.getAllService(namespace, appName)
              
            else {
              /*
               * Retrieve metrics tags for a certain service 
               * of an application
               */
               val serviceName = context("serviceName")
               return MetricsUtil.getService(namespace, appName, serviceName)
            }
          }          
        }                
        case "spark" => {
          if (context.contains("appName") == false)
            return null
            
          else {
            val appName = context("appName")
            if (context.contains("sparkName") == false)
              /*
               * Retrieve metrics tags for all spark programs 
               * of an application
               */            
              return MetricsUtil.getAllSpark(namespace, appName)
              
            else {
              /*
               * Retrieve metrics tags for a certain spark program 
               * of an application
               */
               val sparkName = context("sparkName")
               return MetricsUtil.getSpark(namespace, appName, sparkName)
            }
          }          
        }
        case "worker" => {
          /*
           * Retrieve metrics tags for all workers of an application
           */
          if (context.contains("appName") == false)
            return null
            
          else {
            val appName = context("appName")
            if (context.contains("workerName") == false)
              /*
               * Retrieve metrics tags for all workers 
               * of an application
               */            
              return MetricsUtil.getAllWorker(namespace, appName)
              
            else {
              /*
               * Retrieve metrics tags for a certain worker 
               * of an application
               */
               val workerName = context("workerName")
               return MetricsUtil.getWorker(namespace, appName, workerName)
            }
          }          
        }
        case _ => return null
      }
      
    }
      
    null
  
  }
  
  /**
   * This method retrieves the internal metric names that are available 
   * for a certain metrics context described as a tag map   
   */
  private def getMetrics(tags:Map[String,String]):List[String] = {
    
    val metrics = metricsClient.searchMetrics(tags)
    metrics.asScala.toList
    
  }
  /**
   * This method retrieves metric data for a single metric
   * and associated metric context
   */
  def queryMetric(tags:java.util.Map[String,String],metric:String):CDAPMetricResult = {
    
    val result = metricsClient.query(tags, metric)
    
    val startTime = result.getStartTime
    val endTime = result.getEndTime
    
    val resolution = result.getResolution
    val series = result.getSeries.map(timeseries => {

      val metric = timeseries.getMetricName
      val data = timeseries.getData.map(timeValue =>
        CDAPTimeValue(timeValue.getTime,timeValue.getValue)
      )
      
      CDAPTimeSeries(name=metric,data=data.toList)
      
    }).toList
    
    CDAPMetricResult(startTime=startTime,endTime=endTime,resolution=resolution,series=series)
    
  }
  
  /********************************
   * 
   * MONITOR SUPPORT
   * 
   *******************************/
   
  /**
   * This method retrieves all system services
   */
  def getSystemServices():List[CDAPSystemServiceMeta] = {
    
    val services = monitorClient.listSystemServices
    services.map(service => {

      CDAPSystemServiceMeta(
        name = service.getName,
        description = service.getDescription,
        status = service.getStatus,
        logs = service.getLogs,
        minInstances = service.getMinInstances,
        maxInstances = service.getMaxInstances,
        instancesRequested = service.getInstancesRequested,
        instancesProvisioned = service.getInstancesProvisioned
      )
      
    }).toList
    
  }
  /**
   * This method retrieves live info of a certain system service
   */
  def getSystemServiceLiveInfo(serviceName:String):List[CDAPContainerInfo] = {
    
    val info = monitorClient.getSystemServiceLiveInfo(serviceName)
    val containers = info.getContainers
    
    if (containers.isEmpty) return List.empty[CDAPContainerInfo]
    containers.map(container => {
      
      CDAPContainerInfo(
	      name = container.getName,
	      `type` = container.getType.name,
	      instance = container.getInstance,
	      container = container.getContainer,
	      host = container.getHost,
	      memory = container.getMemory,
	      virtualCores = container.getVirtualCores,
	      debugPort = container.getDebugPort
      )
    }).toList
    
  }
  /**
   * This method retrieves the service of a certain system service
   */
  def getSystemServiceStatus(serviceName:String):String = {
    monitorClient.getSystemServiceStatus(serviceName)
  }
  def  getAllSystemServiceStatus:List[CDAPSystemServiceStatus] = {
    
    val statuses = monitorClient.getAllSystemServiceStatus
    
    statuses.map(e => {
      CDAPSystemServiceStatus(name = e._1, status = e._2)
    }).toList
    
  }
	/**
	 * This method retrieves the number of instances the system 
	 * service is running on.
	 */
	def getSystemServiceInstances(serviceName:String):Int = {
	  monitorClient.getSystemServiceInstances(serviceName)
	}
  
  /********************************
   * 
   * QUERY SUPPORT
   * 
   *******************************/
  
  /**
   * This method executes an ad-hoc SQL-like query
   * to explore the content of a certain dataset
   */
  def executeQuery(namespace:String,datasetName:String,query:String):CDAPQueryResult = {
    /*
     * Query statements are used to explore the content 
     * of dataset with ad-hoc SQL-like queries. 
     * 
     * Queries can be run over streams and certain types 
     * of datasets. Enabling exploration for a dataset results 
     * in the creation of a SQL table in the Explore system. 
     * 
     * The name of this table is, by default, the same as the 
     * name of the dataset, prefixed with dataset_. 
     */
    val table = s"dataset_${datasetName}"
    val statement = query.replace(datasetName, table)   
    
    val nsID = NamespaceId.fromIdParts(List(namespace))
    val executionResult = queryClient.execute(nsID, statement).get
    
    /* The # of rows returned */
    val count = executionResult.getFetchSize
    
    /* Operations status of query request */
    val status = executionResult.getStatus.getStatus.name
    
    val schema = executionResult.getResultSchema.map(col => {
      
      CDAPColumnDesc(
        colName=col.getName.replace("dataset_",""),
        colType=col.getType,
        colComment=col.getComment,
        colPos=col.getPosition)
    
    }).toList
    
    /* Retrieve column data */
    
    val iter = executionResult.asScala
    val rows = ArrayBuffer.empty[List[_]]
    
    while (iter.hasNext) {
      
      val row = iter.next()
      val values = row.getColumns.asScala.toList
      
      rows += values
      
    }
    
    CDAPQueryResult(
      namespace=namespace,
      datasetName=datasetName,
      query=query,
      count=count,
      status=status,
      schema=schema,
      rows=rows.toList
    )

  }
  
}
