/*
// Copyright (c) 2016 Intel Corporation 
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

import akka.actor.{ ActorSystem, Props }
import akka.io.IO
import kafka.producer.{ KeyedMessage, Producer, ProducerConfig }
import kafka.utils.Logging
import kafka.utils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import spray.can.Http
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import com.typesafe.config.{ Config, ConfigFactory }

/**
 * Streaming simulator Application - a REST application to simulate streaming
 */

object StreamProducer extends App {

  var config: Config = null

  override def main(args: Array[String]): Unit = {

    val config = ConfigFactory.load()

    val broker: String = config.getString("trustedanalytics.simulate-streaming.broker")
    val topic: String = config.getString("trustedanalytics.simulate-streaming.topic")
    var dataFile: String = config.getString("trustedanalytics.simulate-streaming.stream-data")

    System.out.println("Brokers: " + broker)
    System.out.println("Topic: " + topic)
    System.out.println("SourceFile: " + dataFile)

    val simulator = new Simulator(new StreamParams(broker, topic, dataFile))

    createActorSystemAndBindToHttp(new StreamingAppService(simulator))
    new Thread(simulator).start
  }

  /**
   * We need an ActorSystem to host our application in and to bind it to an HTTP port
   */
  private def createActorSystemAndBindToHttp(streamingService: StreamingAppService): Unit = {
    // create the system
    implicit val system = ActorSystem("simulate-streaming")
    implicit val timeout = Timeout(5.seconds)
    val service = system.actorOf(Props(new StreamingAppServiceActor(streamingService)), "simulate-streaming")
    // Bind the Spray Actor to an HTTP Port
    // start a new HTTP server with our service actor as the handler
    IO(Http) ? Http.Bind(service, interface = config.getString("trustedanalytics.atk.simulate-streaming.host"), port = config.getInt("trustedanalytics.atk.simulate-streaming.port"))
  }
}
