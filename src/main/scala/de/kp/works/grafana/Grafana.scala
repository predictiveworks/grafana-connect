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

import com.typesafe.config.ConfigFactory
import de.kp.works.cdap.CDAPConf

object Grafana {

  def main(args:Array[String]):Unit = {
    
    val cfgstr = try {
      
      val fname = System.getProperty("config.file")
      val path = new java.io.File("").getAbsolutePath + "/conf/" + fname 
      
      scala.io.Source.fromFile(path).getLines.mkString("\n")
      
    } catch {
      case t:Throwable => null
    }
    
    if (cfgstr == null) {
        
      println("[INFO] ---------------------------------------------------")
      println("[INFO] Launch Grafana Connect with internal configuration.")
      println("[INFO] ---------------------------------------------------")
      /*
       * Initialize Grafana & CDAP configuration
       */
      GrafanaConf.init()
      CDAPConf.init()
        
      val binding = getHttpBindings.head
      launch(binding)
      
    } else {
        
      println("[INFO] ---------------------------------------------------")
      println("[INFO] Launch Grafana Connect with external configuration.")
      println("[INFO] ---------------------------------------------------")
        
      try {
        /*
       	 * Initialize Grafana & CDAP configuration from externally
       	 * provided configuration file
       	 */
        val config = ConfigFactory.parseString(cfgstr)

        GrafanaConf.init(Option(config))
        CDAPConf.init(Option(config))
        
        val binding = getHttpBindings.head
        launch(binding)
          
      } catch {
        case e:Exception => {
          println("")
          println("[ERROR] Grafana Connect could not be started: " + e.getMessage)
          println("")
        }
      }
     
    }
   
  } 
  
  def launch(binding:AkkaBinding) {
    
    new AkkaLauncher(binding).launch()

  }

  private def getHttpBindings:Seq[AkkaBinding] = {
    
    val cfg = GrafanaConf.bindingConfig
    
    val controlList = cfg.getString("control.brokerList").split(",")
    
    var count = 0
    val bindings = controlList.map{control => {
      
      val Array(host,port) = control.split(":")
      
      val server = s"grafana-connect-${count}"
      count += 1
      
      AkkaBinding(host = host, port = port.toInt, server = server)
      
    }}

    bindings
    
  }
  
}