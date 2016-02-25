package org.apache.samoa;

import java.io.File;
import java.io.IOException;
/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2014 - 2015 Apache Software Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.stram.cli.DTCli.ExternalHelper;
import com.datatorrent.stram.client.StramAppLauncher;
import com.datatorrent.stram.client.StramAppLauncher.AppFactory;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;
import org.apache.samoa.topology.impl.ApexSamoaUtils;
import org.apache.samoa.topology.impl.ApexTask;
import org.apache.samoa.topology.impl.ApexTopology;

public class ApexDoTask
{

  public static ApexTopology apexTopo;
  /**
   * The main method.
   * 
   * @param args
   *          the arguments
   */
  public static void main(String[] args)
  {

    List<String> tmpArgs = new ArrayList<String>(Arrays.asList(args));

    args = tmpArgs.toArray(new String[0]);
    System.out.println("Arguments to ApexDoTask: " + Arrays.asList(args));

    // convert the arguments into Storm topology
    apexTopo = ApexSamoaUtils.argsToTopology(args);
    String topologyName = apexTopo.getTopologyName();

    try {
      Configuration config = new Configuration();
      

      AppFactory appFactory = new StramAppLauncher.AppFactory()
      {

        @Override
        public String getName()
        {
          return "SAMOA Application on APEX";
        }

        @Override
        public String getDisplayName()
        {
          return "SAMOA Application on APEX";
        }

        @Override
        public LogicalPlan createApp(LogicalPlanConfiguration conf)
        {
          LogicalPlan dag = new LogicalPlan();
          conf.prepareDAG(dag, new ApexTask(apexTopo), getName());
          return dag;
        }
      };
      
      String launchCommand = "launch -libjars /tmp/lib/activation-1.1.jar,/tmp/lib/ant-1.9.2.jar,/tmp/lib/ant-launcher-1.9.2.jar,/tmp/lib/aopalliance-1.0.jar,/tmp/lib/apex-api-3.4.0-incubating-SNAPSHOT.jar,/tmp/lib/apex-bufferserver-3.4.0-incubating-SNAPSHOT.jar,/tmp/lib/apex-common-3.4.0-incubating-SNAPSHOT.jar,/tmp/lib/apex-engine-3.4.0-incubating-SNAPSHOT.jar,/tmp/lib/asm-3.1.jar,/tmp/lib/async-http-client-1.7.20.jar,/tmp/lib/avro-1.7.7.jar,/tmp/lib/bval-core-0.5.jar,/tmp/lib/bval-jsr303-0.5.jar,/tmp/lib/commons-beanutils-1.8.3.jar,/tmp/lib/commons-beanutils-core-1.8.0.jar,/tmp/lib/commons-cli-1.2.jar,/tmp/lib/commons-codec-1.10.jar,/tmp/lib/commons-collections-3.2.1.jar,/tmp/lib/commons-compress-1.4.1.jar,/tmp/lib/commons-configuration-1.6.jar,/tmp/lib/commons-daemon-1.0.13.jar,/tmp/lib/commons-digester-1.8.jar,/tmp/lib/commons-el-1.0.jar,/tmp/lib/commons-httpclient-3.1.jar,/tmp/lib/commons-io-2.4.jar,/tmp/lib/commons-lang3-3.1.jar,/tmp/lib/commons-lang-2.5.jar,/tmp/lib/commons-logging-1.1.1.jar,/tmp/lib/commons-math-2.1.jar,/tmp/lib/commons-net-3.1.jar,/tmp/lib/gmbal-api-only-3.0.0-b023.jar,/tmp/lib/grizzly-framework-2.1.2.jar,/tmp/lib/grizzly-http-2.1.2.jar,/tmp/lib/grizzly-http-server-2.1.2.jar,/tmp/lib/grizzly-http-servlet-2.1.2.jar,/tmp/lib/grizzly-rcm-2.1.2.jar,/tmp/lib/guava-17.0.jar,/tmp/lib/guice-3.0.jar,/tmp/lib/guice-servlet-3.0.jar,/tmp/lib/hadoop-annotations-2.2.0.jar,/tmp/lib/hadoop-auth-2.2.0.jar,/tmp/lib/hadoop-common-2.2.0.jar,/tmp/lib/hadoop-common-2.2.0-tests.jar,/tmp/lib/hadoop-hdfs-2.2.0.jar,/tmp/lib/hadoop-yarn-api-2.2.0.jar,/tmp/lib/hadoop-yarn-client-2.2.0.jar,/tmp/lib/hadoop-yarn-common-2.2.0.jar,/tmp/lib/hibernate-validator-4.1.0.Final.jar,/tmp/lib/hibernate-validator-annotation-processor-4.1.0.Final.jar,/tmp/lib/httpclient-4.3.5.jar,/tmp/lib/httpcore-4.3.2.jar,/tmp/lib/jackson-core-asl-1.9.13.jar,/tmp/lib/jackson-jaxrs-1.8.3.jar,/tmp/lib/jackson-mapper-asl-1.9.13.jar,/tmp/lib/jackson-xc-1.8.3.jar,/tmp/lib/jasper-compiler-5.5.23.jar,/tmp/lib/jasper-runtime-5.5.23.jar,/tmp/lib/javacliparser-0.5.0.jar,/tmp/lib/javax.inject-1.jar,/tmp/lib/javax.servlet-3.1.jar,/tmp/lib/javax.servlet-api-3.0.1.jar,/tmp/lib/jaxb-api-2.2.2.jar,/tmp/lib/jaxb-impl-2.2.3-1.jar,/tmp/lib/jcip-annotations-1.0.jar,/tmp/lib/jersey-apache-client4-1.9.jar,/tmp/lib/jersey-client-1.9.jar,/tmp/lib/jersey-core-1.9.jar,/tmp/lib/jersey-grizzly2-1.9.jar,/tmp/lib/jersey-guice-1.9.jar,/tmp/lib/jersey-json-1.9.jar,/tmp/lib/jersey-server-1.9.jar,/tmp/lib/jersey-test-framework-core-1.9.jar,/tmp/lib/jersey-test-framework-grizzly2-1.9.jar,/tmp/lib/jets3t-0.6.1.jar,/tmp/lib/jettison-1.1.jar,/tmp/lib/jetty-6.1.26.jar,/tmp/lib/jetty-http-8.1.10.v20130312.jar,/tmp/lib/jetty-io-8.1.10.v20130312.jar,/tmp/lib/jetty-util-6.1.26.jar,/tmp/lib/jetty-util-8.1.10.v20130312.jar,/tmp/lib/jetty-websocket-8.1.10.v20130312.jar,/tmp/lib/jline-0.9.94.jar,/tmp/lib/jsch-0.1.42.jar,/tmp/lib/jsp-api-2.1.jar,/tmp/lib/kryo-2.24.0.jar,/tmp/lib/log4j-1.2.16.jar,/tmp/lib/log4j-over-slf4j-1.7.13.jar,/tmp/lib/management-api-3.0.0-b012.jar,/tmp/lib/mbassador-1.1.9.jar,/tmp/lib/metrics-core-2.2.0.jar,/tmp/lib/miniball-1.0.3.jar,/tmp/lib/minlog-1.2.jar,/tmp/lib/netlet-1.2.0.jar,/tmp/lib/netty-3.7.0.Final.jar,/tmp/lib/objenesis-2.1.jar,/tmp/lib/paranamer-2.3.jar,/tmp/lib/protobuf-java-2.5.0.jar,/tmp/lib/samoa-api-0.4.0-incubating-SNAPSHOT.jar,/tmp/lib/samoa-instances-0.4.0-incubating-SNAPSHOT.jar,/tmp/lib/servlet-api-2.5.jar,/tmp/lib/slf4j-api-1.6.1.jar,/tmp/lib/snappy-java-1.0.5.jar,/tmp/lib/stax-api-1.0.1.jar,/tmp/lib/validation-api-1.0.0.GA.jar,/tmp/lib/xbean-asm5-shaded-4.3.jar,/tmp/lib/xmlenc-0.52.jar,/tmp/lib/xz-1.0.jar,/tmp/lib/zip4j-1.3.2.jar -force target/samoa-apex-0.4.0-incubating-SNAPSHOT.apa";
      ExternalHelper.execute(launchCommand, appFactory);
    } catch (Exception e1) {
      e1.printStackTrace();
    }
  }

}
