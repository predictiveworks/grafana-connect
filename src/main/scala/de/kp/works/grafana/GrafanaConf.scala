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

import com.typesafe.config.{Config,ConfigFactory}
/**
 * Read file 'reference.conf' from classpath and
 * initialize configuration parameters
 */
object GrafanaConf extends Serializable {

  private var config: Option[Config] = None

  def init(cfg:Option[Config] = None):Unit = {
    
    if (config.isDefined == false) {
      
      if (cfg.isDefined) config = cfg
      else {
        
        val path = "reference.conf"
        config = Option(ConfigFactory.load(path))
        
      }
      
    }
    
  }
  
  def getConfig = {
    if (config.isDefined) config.get else null
  }
  
  def bindingConfig = {
    if (config.isDefined) config.get.getConfig("binding") else null
  }
  
  def cdapConfig = {
    if (config.isDefined) config.get.getConfig("cdap") else null
  }
  
}
