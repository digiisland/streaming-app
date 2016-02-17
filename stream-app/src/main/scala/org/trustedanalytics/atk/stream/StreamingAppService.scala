/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package org.trustedanalytics.atk.stream

import akka.actor.Actor
import spray.routing._
import spray.http._
import MediaTypes._
import akka.event.Logging
import scala.concurrent._
import ExecutionContext.Implicits.global
//import org.trustedanalytics.atk.spray.json.AtkDefaultJsonProtocol
import scala.util.{ Failure, Success }

/**
 * We don't implement our route structure directly in the service actor because
 * we want to be able to test it independently, without having to spin up an actor
 *
 * @param streamAppService the service to delegate to
 */
class StreamingAppServiceActor(val streamAppService: StreamingAppService) extends Actor with HttpService {

  /**
   * the HttpService trait defines only one abstract member, which
   * connects the services environment to the enclosing actor or test
   */
  override def actorRefFactory = context

  /**
   *
   * This actor only runs our route, but you could add other things here, like
   * request stream processing or timeout handling
   */
  def receive = runRoute(streamAppService.serviceRoute)
}

/**
 * Defines our service behavior independently from the service actor
 */
class StreamingAppService(simulator: Simulator) extends Directives {

  def homepage = {
    respondWithMediaType(`text/html`) {
      complete {
        <html>
          <body>
            <h1>Welcome to the Streaming simulator</h1>
          </body>
        </html>
      }
    }
  }

  lazy val description = {
    new ServiceDescription(name = "Streaming simulator",
      identifier = "ia",
      versions = List("v1"))
  }

  /**
   * Main Route entry point to the streaming simulator
   */
  val serviceRoute: Route = logRequest("streaming simulator", Logging.InfoLevel) {
    path("") {
      get {
        homepage
      }
    }
  }
}
case class ServiceDescription(name: String, identifier: String, versions: List[String])
