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

import java.io._
import java.net.URI
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FSDataInputStream, Path }
import org.trustedanalytics.hadoop.config.client.helper.Hdfs
import scala.io.{ BufferedSource, Source }
import kafka.producer.ProducerConfig
import java.util.Properties
import kafka.producer.Producer
import kafka.producer.KeyedMessage
import kafka.utils.Logging

case class StreamParams(broker: String, topic: String, dataFile: String)

class Simulator(params: StreamParams) extends Runnable with Logging {

  var bufferedReader: BufferedReader = null

  def run(): Unit = {

    try {
      val events = 10000

      val topic = params.topic
      val brokers = params.broker
      val props = new Properties()
      println("brokers uris: " + brokers)
      props.put("metadata.broker.list", brokers)
      props.put("serializer.class", "kafka.serializer.StringEncoder")
      props.put("producer.type", "async")

      val config = new ProducerConfig(props)
      val producer = new Producer[String, String](config)
      val t = System.currentTimeMillis()
      initializeFile
      //TODO make it an infinite loop
      for (nEvents <- Range(0, events)) {
        val data = new KeyedMessage[String, String](topic, readNextLine)
        producer.send(data)
      }
      producer.close()
    }
    finally {
      IOUtils.closeQuietly(bufferedReader)
    }
  }

  /**
   * Initialize the file reader
   */
  private def initializeFile = {

      var inputStreamReader: InputStreamReader = null

      if (params.dataFile.startsWith("hdfs://")) {
        val hdfsFileSystem: org.apache.hadoop.fs.FileSystem = org.apache.hadoop.fs.FileSystem.get(new URI(params.dataFile), new Configuration())
        //val hdfsFileSystem = Hdfs.newInstance().createFileSystem() //needed for kerberos will uncomment it for a kerberos tap instance
        var fileInputStream = hdfsFileSystem.open(new Path(params.dataFile))
        inputStreamReader = new InputStreamReader(fileInputStream)
      }
      else
        inputStreamReader = new InputStreamReader(new DataInputStream(new FileInputStream(params.dataFile)))

      bufferedReader = new BufferedReader(inputStreamReader)
  }

  /**
   * Reads the testdata file returns the next line
   * @return String
   */
  private def readNextLine: String = {

    var string = bufferedReader.readLine
    System.out.println(string)

    if (string == null) { //we've reached the end of file
      IOUtils.closeQuietly(bufferedReader)
      initializeFile
      string = bufferedReader.readLine
    }
    string
  }
}
