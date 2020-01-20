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

import akka.event.LoggingAdapter

import akka.http.scaladsl.model.headers.{Authorization,OAuth2BearerToken}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._

case class OAuth2Token(token:String,username:String)
/**
 * The current implementation of Grafana Connect is NOT
 * authentication enabled, but OAuth2 support is prepared 
 * already for future requirements.
 */
class OAuth2(logger:LoggingAdapter,tokenVerifier:TokenVerifier) {

  def authorized:Directive1[OAuth2Token] = {

    getBearerToken.flatMap {
      case Some(token) =>
        /*
         * Strategy: Token verification returns the registered
         * user name, and, combined with the bearer token, is
         * provided for the inner route
         */
        onComplete(tokenVerifier.verifyToken(token)).flatMap {
          _.map(username => provide(OAuth2Token(token,username)))
            .recover {
              case ex =>
                logger.error(ex, "Couldn't log in using provided authorization token")
                reject(AuthorizationFailedRejection).toDirective[Tuple1[OAuth2Token]]
            }
            .get
        }
      case None => {
        /*
         * The current version does not expect any bearer token, 
         * and, does not support any token validiation
         */
        val token = "*"
        
        onComplete(tokenVerifier.verifyToken(token)).flatMap {
          _.map(username => provide(OAuth2Token(token,username)))
            .recover {
              case ex =>
                logger.error(ex, "Couldn't log in using provided authorization token")
                reject(AuthorizationFailedRejection).toDirective[Tuple1[OAuth2Token]]
            }
            .get
            
        //reject(AuthorizationFailedRejection)
        }
      }
    }
  
  }
  /**
   * This directive supports cookie & header provisioning
   * of the bearer token
   */
  private def getBearerToken:Directive1[Option[String]] =
    for {
      authBearerHeader  <- optionalHeaderValueByType(classOf[Authorization]).map(extractBearerToken)
      xAuthCookie       <- optionalCookie("X-Authorization-Token").map(_.map(_.value))
    } yield authBearerHeader.orElse(xAuthCookie)

  private def extractBearerToken(authHeader:Option[Authorization]): Option[String] =
    authHeader.collect {
      case Authorization(OAuth2BearerToken(token)) => token
    }

}