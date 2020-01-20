package de.kp.works.grafana
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

import java.util.{Map => JMap}
import java.lang.reflect.Type

import akka.actor.Actor

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken

import scala.util.Try
import scala.collection.JavaConverters._

import de.kp.works.cdap.CDAPJob

class QueryActor(job:CDAPJob) extends Actor {
    
  import QueryActor._
    
  private val gson = new Gson()
  private	val requestType = new TypeToken[JMap[String, Object]](){}.getType()


  def receive = {
    /*
     * This request refers to Grafana's connection test mechanism
     */
    case ping:PingReq => {
      
      sender ! PingRsp(Try({
        "[Grafana Connect] Data spource successfully connected."
        
      }).recover {
        case t:Exception => {
          throw new Exception(t.getMessage)
        }
      })
      
    }
    /*
     * This request retrieves a data of a time series
     */
    case query:QueryReq => {
      /*
       * Gson has issues with native Scala types; therefore 
       * conversion to Java type is necessary here
       */
      val request = gson.fromJson(query.params,requestType).asInstanceOf[JMap[String,Object]]
      
      val metrics = job.getMetrics(request)
      val resp = gson.toJson(metrics)
      
      println(resp)
      
      sender ! QueryRsp(Try(resp)
          .recover {
            case t:Exception => {
              throw new Exception(t.getMessage)
            }
          }
      )
      
    }
    /*
     * This request retrieves the list of available metrics
     * (or tables) that contain time series data
     */
    case search:SearchReq => {
      /*
       * Gson has issues with native Scala types; therefore 
       * conversion to Java type is necessary here
       */
      val request = gson.fromJson(search.params,requestType).asInstanceOf[JMap[String,Object]]

      val tables = job.getTables(request)    
      val resp = gson.toJson(tables.asJava)
      
      sender ! SearchRsp(Try(resp)
          .recover {
            case t:Exception => {
              throw new Exception(t.getMessage)
            }
          }
      )  
      
    }
    case _ => {
      
    }
  }

}

object QueryActor {
  
  /** PING **/
  case class PingReq()
  
  case class PingRsp(res:Try[String])
  
  /** QUERY **/
  case class QueryReq(params:String)
  
  case class QueryRsp(res:Try[String])
  
  /** SEARCH **/
  case class SearchReq(params:String)
  
  case class SearchRsp(res:Try[String])
  
}