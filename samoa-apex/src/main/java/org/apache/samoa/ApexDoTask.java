package org.apache.samoa;

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
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.client.StramAppLauncher;
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
    apexTopo = ApexSamoaUtils.argsToTopology(args);
    try {
      startLaunch();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
//  public static void launch() {
//    try {
//      String launchCommand = "launch -force target/samoa-apex*.apa SAMOA-on-Apache-Apex";
//      ExternalHelper.execute(launchCommand);
//    } catch (Exception e1) {
//      e1.printStackTrace();
//    }
//  }

  public static void startLaunch() throws Exception
  {
    ApexTask streamingApp = new ApexTask(apexTopo);
    streamingApp.setLocalMode(true);
    launch(streamingApp, "Apex App");
  }

  public static void launch(StreamingApplication app, String name, String libjars) throws Exception {
    Configuration conf = new Configuration(true);
    if (libjars != null) {
        conf.set(StramAppLauncher.LIBJARS_CONF_KEY_NAME, libjars);
    }
    StramAppLauncher appLauncher = new StramAppLauncher(name, conf);
    appLauncher.loadDependencies();
    StreamingAppFactory appFactory = new StreamingAppFactory(app, name);
    appLauncher.launchApp(appFactory);
    }

  public static void launch(StreamingApplication app, String name) throws Exception {
    launch(app, name, null);
  }
  public static ApexTopology getTopology(){
    return apexTopo;
  }

}
