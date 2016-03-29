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
    apexTopo = ApexSamoaUtils.argsToTopology(args);
    launch();
  }
  
  public static void launch() {
    try {
      String launchCommand = "launch -force -local target/samoa-apex*.apa IterationExample";
      ExternalHelper.execute(launchCommand);
    } catch (Exception e1) {
      e1.printStackTrace();
    }
  }
  
  public static ApexTopology getTopology(){
    return apexTopo;
  }

}
