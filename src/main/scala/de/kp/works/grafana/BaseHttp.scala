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

import akka.actor._
import akka.util.Timeout

/* JSON support */
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json._

import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.server.directives.Credentials
import akka.http.scaladsl.server.Route

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor,Future}

import scala.collection.JavaConverters._

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
 
  implicit object AnyJsonFormat extends JsonFormat[Any] {
  
    def write(x: Any) = x match {
      /* Boolean */
      case b: Boolean           => JsBoolean(b)
      
      /* Number */
      case n: Byte              => JsNumber(n)
      case n: Double            => JsNumber(n)
      case n: Float             => JsNumber(n)
      case n: Int               => JsNumber(n)
      case n: Long              => JsNumber(n)
      case n: Short             => JsNumber(n)
      
      /* String */
      case d:java.sql.Date      => JsString(d.toString)
      case s: String            => JsString(s)
      case t:java.sql.Timestamp => JsString(t.toString)
    }
    
    def read(value: JsValue) = value match {
      /* Boolean */
      case JsTrue  => true
      case JsFalse => false
      /* Number */
      case JsNumber(n) => {
        if (n.isValidByte) 
          n.byteValue
        
        /*
         * To test whether a `BigDecimal` number can be converted to a `Double`, 
         * test with `isDecimalDouble`, `isBinaryDouble`, or `isExactDouble`
         */
        
        else if (n.isExactDouble)
          n.doubleValue
        
        else if (n.isExactFloat)
          n.floatValue
        
        else if (n.isValidInt)
          n.intValue
        
        else if (n.isValidLong)
          n.longValue
        
        else if (n.isValidShort)
          n.shortValue
          
      }
      /* String */
      case JsString(s) => {
        
        try {
          /*
           * The provided [String] can be transformed into a [Date]
           */
          java.sql.Date.valueOf(s)
        }
        catch {
          case t:Throwable => {
            /*
             * The provided [String] is either a [Timestamp] or 
             * a genuine [String]
             */
            try {
              /*
               * The provided [String] can be transformed into a [Timestamp]
               */
              java.sql.Timestamp.valueOf(s)
            }
            catch {
              case t:Throwable => s
            }
          }
        }
        
      }
      case _ => throw new Exception("[ERROR] Data type not support")
    }
  }
  
  /*
   * OAuth2 authentication support
   */
  implicit def OAuth2TokenFormat:RootJsonFormat[OAuth2Token] = jsonFormat2(OAuth2Token)  
   
}

trait BaseHttp extends JsonSupport {
  /*
   * Common timeout for all Akka connection  
   */
  implicit val timeout: Timeout = Timeout(30.seconds)
  implicit def executor: ExecutionContextExecutor
  
  val supervisor:ActorRef
  
  /**
   * Collects all inner routes that must be protected by authorization
   */
  def routes(token:OAuth2Token):Route

  def getException(t:Throwable):Map[String,Any] = {
    
      Map(
        "status"  -> "failure", 
        "message" -> t.getLocalizedMessage, 
        "data"    -> Map.empty[String,Any]
      )
    
  }
  
}