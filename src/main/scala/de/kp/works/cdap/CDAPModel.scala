package de.kp.works.cdap
/*
 * Copyright 2019, Dr. Krusche & Partner PartG.
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
 */

import co.cask.cdap.client.proto.PluginPropertyField

case class CDAPException(
  message:String,
  trace:Option[String] = None
)

case class CDAPColumnDesc(
  colName:String,
  colType:String,
  colComment:String,
  colPos:Int
)

case class CDAPContainerInfo(
	name:String,
	`type`:String,
	instance:Int,
	container:String,
	host:String,
	memory:Int,
	virtualCores:Int,
	debugPort:Int
)

case class CDAPDataset(
  namespace:String,
  datasetName:String,
  datasetDesc:String,
	datasetType:String,
	datasetProps:Map[String,String]    
)

case class CDAPDatasetSpec(
  namespace:String,
  datasetName:String,
  datasetDesc:String,
	datasetType:String,
	datasetProps:Map[String,String],
	hiveTableName:String,
	ownerPrincipal:String
)

case class CDAPMetrics(
  metrics:List[String],
  metricTags:Map[String,String]
)

case class CDAPQueryResult(
  namespace:String,
  datasetName:String,
  query:String,
  /* The # of rows within the result */
  count:Int,
  /* The status of the query request */
  status:String,
  schema:List[CDAPColumnDesc],
  /* Row column values */
  rows:List[List[Any]]
)

case class CDAPRunMetrics(
  pid:String,
  /* Status */
  status:String,
  /* Start time in seconds */
  startTime:Long,
  /* Duration in seconds */
  duration:Long,
  metrics:List[(String,Long)]
)

case class CDAPRunRecord(
  pid:String,
  startTs:Long,
  runTs:Long,
  stopTs:Long,
	suspendTs:Long,
	resumeTs:Long,
	status:String,
  properties:Map[String,String]
)

case class CDAPSystemServiceMeta(
  name:String,
  description:String,
  status:String,
  logs:String,
  minInstances:Int,
  maxInstances:Int,
  instancesRequested:Int,
  instancesProvisioned:Int
)

case class CDAPSystemServiceStatus(
  name:String,
  status:String
)

case class CDAPTimeValue(
  time:Long,
  value:Long
)

case class CDAPTimeSeries(
  /* Name of the metric */
  name:String,
  data:List[CDAPTimeValue]
)

case class CDAPMetricResult(
  startTime:Long, endTime:Long,resolution:String,series:List[CDAPTimeSeries]
)
