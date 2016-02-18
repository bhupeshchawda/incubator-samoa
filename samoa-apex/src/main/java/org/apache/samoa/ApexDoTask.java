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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.StramUtils;
import com.datatorrent.stram.client.StramAppLauncher;
import com.datatorrent.stram.client.StramAppLauncher.AppFactory;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;

import org.apache.samoa.topology.impl.ApexSamoaUtils;
import org.apache.samoa.topology.impl.ApexTask;
import org.apache.samoa.topology.impl.ApexTopology;

public class ApexDoTask
{
  private static final Logger logger = LoggerFactory.getLogger(ApexDoTask.class);

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

    // convert the arguments into Storm topology
    apexTopo = ApexSamoaUtils.argsToTopology(args);
    String topologyName = apexTopo.getTopologyName();

    try {
      StramAppLauncher submitApp = new StramAppLauncher(topologyName, new Configuration());
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
      submitApp.launchApp(appFactory);
    } catch (Exception e1) {
      e1.printStackTrace();
    }
  }
}
