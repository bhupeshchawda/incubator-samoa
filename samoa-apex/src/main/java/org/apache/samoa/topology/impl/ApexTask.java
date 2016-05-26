package org.apache.samoa.topology.impl;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2014 - 2016 Apache Software Foundation
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

import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.samoa.core.ContentEvent;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.common.util.DefaultDelayOperator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.InputPortMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.OutputPortMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class ApexTask implements StreamingApplication {

  LogicalPlan dag;
  String appName = "SAMOA-Apex-Application";
  List<OperatorMeta> visited = Lists.newArrayList();
  Set<StreamMeta> loopStreams = Sets.newHashSet();
  boolean localMode = false;

  public ApexTask(ApexTopology apexTopo) {
    this.dag = (LogicalPlan) apexTopo.getDAG();
    appName = apexTopo.getTopologyName();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void populateDAG(DAG dag, Configuration conf) {

    LogicalPlan dag2 = new LogicalPlan();
    for (OperatorMeta o : this.dag.getAllOperators()) {
      dag2.addOperator(o.getName(), o.getOperator());
    }
    for (StreamMeta s : this.dag.getAllStreams()) {
      for (InputPortMeta i : s.getSinks()) {
        Operator.OutputPort<Object> op = (OutputPort<Object>) s.getSource().getPortObject();
        Operator.InputPort<Object> ip = (InputPort<Object>) i.getPortObject();
        dag2.addStream(s.getName(), op, ip);
      }
    }

    detectLoops(dag2, conf);

    // Reconstruct Dag
    for (OperatorMeta o : this.dag.getAllOperators()) {
      dag.addOperator(o.getName(), o.getOperator());
      System.out.println("Added Operator: " + o.getName());
      for (Entry<Attribute<?>, Object> attr : o.getAttributes().entrySet()) {
        dag.setAttribute(o.getOperator(), (Attribute) attr.getKey(), attr.getValue());
      }
    }
    for (StreamMeta s : this.dag.getAllStreams()) {
      if (loopStreams.contains(s)) {
        // Add delay Operator
        DefaultDelayOperator<ContentEvent> d = dag.addOperator("Delay" + s.getName(),
            new DefaultDelayOperator<ContentEvent>());
        dag.addStream("Delay" + s.getName() + "toDelay",
            (DefaultOutputPort<ContentEvent>) s.getSource().getPortObject(), d.input);
        dag.addStream("Delay" + s.getName() + "fromDelay", d.output,
            (DefaultInputPort<ContentEvent>) s.getSinks().get(0).getPortObject());
        continue;
      }
      for (InputPortMeta i : s.getSinks()) {
        DefaultOutputPort<Object> op = (DefaultOutputPort<Object>) s.getSource().getPortObject();
        DefaultInputPort<Object> ip = (DefaultInputPort<Object>) i.getPortObject();
        Preconditions.checkArgument(op != null && ip != null);
        dag.addStream(s.getName(), op, ip);
      }
    }

    //    dag.setAttribute(DAGContext.STREAMING_WINDOW_SIZE_MILLIS, 100);
    System.out.println(dag.getAttributes());
    dag.setAttribute(Context.DAGContext.APPLICATION_NAME, appName);
  }

  public void detectLoops(DAG dag, Configuration conf) {
    List<OperatorMeta> inputOperators = Lists.newArrayList();
    for (OperatorMeta om : this.dag.getAllOperators()) {
      if (om.getOperator() instanceof InputOperator) {
        inputOperators.add(om);
      }
    }

    for (OperatorMeta o : inputOperators) {
      visited.clear();
      List<OperatorMeta> visited = Lists.newArrayList();
      dfs(o, visited);
    }
  }

  public void dfs(OperatorMeta o, List<OperatorMeta> visited) {
    visited.add(o);

    for (Entry<OutputPortMeta, StreamMeta> opm : o.getOutputStreams().entrySet()) {
      // Samoa won't allow one output port to multiple input port kind of streams
      OperatorMeta downStreamOp = opm.getValue().getSinks().get(0).getOperatorWrapper();
      if (visited.contains(downStreamOp)) {
        loopStreams.add(opm.getValue());
      } else {
        List<OperatorMeta> v2 = Lists.newArrayList();
        v2.addAll(visited);
        dfs(downStreamOp, v2);
      }
    }
  }

  public void setLocalMode(boolean localMode) {
    this.localMode = localMode;
  }

  public boolean isLocalMode() {
    return localMode;
  }

}
