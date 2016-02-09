package org.apache.samoa.topology.impl;

import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.samoa.core.ContentEvent;
import org.jboss.netty.channel.socket.DatagramChannelConfig;

import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.common.util.DefaultDelayOperator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.engine.OperatorContext;
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
  List<OperatorMeta> visited = Lists.newArrayList();
  Set<StreamMeta> loopStreams = Sets.newHashSet();

	public ApexTask(ApexTopology apexTopo) {
		this.dag = (LogicalPlan) apexTopo.getDAG();
		System.out.println("Dag Set in Apex Task" + this.dag);
	}

	@SuppressWarnings("unchecked")
  @Override
	public void populateDAG(DAG dag, Configuration conf) {

	  conf.set("dt.loggers.level","com.datatorrent.*:DEBUG");
    LogicalPlan dag2 = new LogicalPlan();
    for(OperatorMeta o: this.dag.getAllOperators()){
			System.out.println("Adding Operator: " + o.getName());
			dag2.addOperator(o.getName(), o.getOperator());
		}
		for(StreamMeta s: this.dag.getAllStreams()) {
			System.out.println("Stream: " + s.getName());
			for(InputPortMeta i: s.getSinks()) {
				System.out.println(s.getSource().getOperatorMeta().getName()+":"+s.getSource().getPortName()+" --- "+ i.getOperatorWrapper().getName()+":"+i.getPortName());
				Operator.OutputPort<Object> op = (OutputPort<Object>) s.getSource().getPortObject();
				Operator.InputPort<Object> ip = (InputPort<Object>) i.getPortObject();
				dag2.addStream(s.getName(), op, ip);
			}
		}

		System.out.println("Dag Set in Populate DAG" + dag2);
		detectLoops(dag2, conf);
		System.out.println("Loop Streams: " + loopStreams);
		
		// Reconstruct Dag
    for(OperatorMeta o: this.dag.getAllOperators()){
      System.out.println("Adding Operator: " + o.getName());
      dag.addOperator(o.getName(), o.getOperator());
    }
    for(StreamMeta s: this.dag.getAllStreams()) {
      System.out.println("Stream: " + s.getName());
      if(loopStreams.contains(s)) {
        // Add delay Operator
        DefaultDelayOperator<ContentEvent> d = dag.addOperator("Delay" + s.getName(), new DefaultDelayOperator<ContentEvent>());
        dag.addStream("Delay" + s.getName() + "toDelay", (DefaultOutputPort<ContentEvent>)s.getSource().getPortObject(), d.input);
        dag.addStream("Delay" + s.getName() + "fromDelay", d.output, (DefaultInputPort<ContentEvent>)s.getSinks().get(0).getPortObject());
        continue;
      }
      for(InputPortMeta i: s.getSinks()) {
        System.out.println(s.getSource().getOperatorMeta().getName()+":"+s.getSource().getPortName()+" --- "+ i.getOperatorWrapper().getName()+":"+i.getPortName());
        DefaultOutputPort<Object> op = (DefaultOutputPort<Object>) s.getSource().getPortObject();
        DefaultInputPort<Object> ip = (DefaultInputPort<Object>) i.getPortObject();
        Preconditions.checkArgument(op != null && ip != null);
        dag.addStream(s.getName(), op, ip);
      }
    }
    dag.setAttribute(DAGContext.STREAMING_WINDOW_SIZE_MILLIS, 100);
	}

	public void detectLoops(DAG dag, Configuration conf) {
	  List<OperatorMeta> inputOperators = Lists.newArrayList();
	  for(OperatorMeta om : this.dag.getAllOperators()) {
	    if(om.getOperator() instanceof InputOperator) {
	      inputOperators.add(om);
	    }
	  }
	
	  for(OperatorMeta o: inputOperators) {
	    visited.clear();
	    List<OperatorMeta> visited = Lists.newArrayList();
	    dfs(o, visited);
	  }
	}

	public void dfs(OperatorMeta o, List<OperatorMeta> visited) {
    visited.add(o);

    for(Entry<OutputPortMeta, StreamMeta>  opm: o.getOutputStreams().entrySet()) {
      // Samoa won't allow one output port to multiple input port kind of streams
      OperatorMeta downStreamOp = opm.getValue().getSinks().get(0).getOperatorWrapper();
      if(visited.contains(downStreamOp)) {
        loopStreams.add(opm.getValue());
      }
      else {
        List<OperatorMeta> v2 = Lists.newArrayList();
        v2.addAll(visited);
        dfs(downStreamOp, v2);
      }
    }
	}
}
