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

import akka.actor.{ActorSystem,Props}
import akka.stream.{ActorMaterializer,Materializer}
import akka.event.Logging

import akka.routing.RoundRobinPool

import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route

import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions

import de.kp.works.cdap.CDAPJob

case class AkkaBinding(host:String, port:Int,server:String)

class AkkaLauncher(binding:AkkaBinding) {
 
  private val cfg = GrafanaConf.getConfig.withFallback(ConfigFactory.load("reference.conf"));  
  
  private val server = binding.server
  implicit val system: ActorSystem = ActorSystem.create(server,cfg)
  
  implicit val executor = system.dispatcher
  implicit val materializer = ActorMaterializer()  
  
  private val logger = Logging(system, getClass)  
  private val oauth2 = new OAuth2(logger,new NoTokenVerifier())

  private val job = new CDAPJob()
  
  def launch():Unit = {

    logger.info(s"Launch Grafana Connect with host '${binding.host}' and port '${binding.port}'")

    implicit val actors = Map(
      AkkaController.QueryActor -> system.actorOf(Props(new QueryActor(job)),  AkkaController.QueryActor)                
    )
    
    val controller = new AkkaController(actors,system,oauth2)
    
    val routes:Route = controller.getHttpRoutes
    Http().bindAndHandle(routes , binding.host, binding.port)
    
  }

  def shutdown: Unit = {
    synchronized {
      system.shutdown
    }
  }
}
