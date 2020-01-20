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
import co.cask.cdap.proto.QueryResult
import co.cask.cdap.proto.id.{DatasetId,NamespaceId}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.mutable.{ArrayBuffer}

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
