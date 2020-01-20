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

import akka.pattern.ask

import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives._

import scala.concurrent.Await
import scala.util.{Failure, Success}

trait QueryHttp extends BaseHttp {

  import QueryActor._
  
  override def routes(token:OAuth2Token):Route = {
    ping(token) ~ 
    query(token) ~
    search(token)
  }

 def ping(token:OAuth2Token): Route = {
    path("dataset") {
      get {
        complete {
          val future = supervisor ? new PingReq()
          Await.result(future, timeout.duration) match {
            case PingRsp(Failure(e)) => getException(e)
            case PingRsp(Success(answer)) => answer
          }
        }
      }
    }
  }

 def query(token:OAuth2Token): Route = {
   path("dataset" / "query") {
      post {
        entity(as[String]) {json => {
          complete {
          val future = supervisor ? new QueryReq(json)
          Await.result(future, timeout.duration) match {
            case QueryRsp(Failure(e)) => getException(e)
            case QueryRsp(Success(answer)) => answer
          }
          }
        }}
      }
     
   }
 }

 def search(token:OAuth2Token): Route = {
   path("dataset" / "search") {
      post {
        entity(as[String]) {json => {
          complete {
          val future = supervisor ? new SearchReq(json)
          Await.result(future, timeout.duration) match {
            case SearchRsp(Failure(e)) => getException(e)
            case SearchRsp(Success(answer)) => answer
          }
          }
        }}
      }
     
   }
 }
 
 
}
