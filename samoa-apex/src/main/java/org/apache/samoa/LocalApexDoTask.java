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

import com.datatorrent.api.LocalMode;
import com.datatorrent.stram.plan.logical.LogicalPlan;

import org.apache.samoa.topology.impl.ApexSamoaUtils;
import org.apache.samoa.topology.impl.ApexTask;
import org.apache.samoa.topology.impl.ApexTopology;

public class LocalApexDoTask {

  private static final Logger logger = LoggerFactory.getLogger(LocalApexDoTask.class);

  public static void main(String[] args) {

    List<String> tmpArgs = new ArrayList<String>(Arrays.asList(args));

    args = tmpArgs.toArray(new String[0]);

    // convert the arguments into Storm topology
    ApexTopology apexTopo = ApexSamoaUtils.argsToTopology(args);
    String topologyName = apexTopo.getTopologyName();

    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    conf.set("dt.loggers.level", "com.datatorrent.*:DEBUG");

    try {
      lma.prepareDAG(new ApexTask(apexTopo), conf);
      System.out.println("Dag Set in lma: " + lma.getDAG());
      ((LogicalPlan) lma.getDAG()).validate();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);

    lc.runAsync();
  }
}
