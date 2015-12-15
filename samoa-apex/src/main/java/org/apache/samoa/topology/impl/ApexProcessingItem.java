package org.apache.samoa.topology.impl;

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

import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;
import com.google.common.collect.Lists;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
import org.apache.samoa.topology.AbstractProcessingItem;
import org.apache.samoa.topology.ProcessingItem;
import org.apache.samoa.topology.Stream;
import org.apache.samoa.utils.PartitioningScheme;

/**
 * ProcessingItem implementation for Storm.
 * 
 * @author Arinto Murdopo
 * 
 */
class ApexProcessingItem extends AbstractProcessingItem implements ApexTopologyNode {
	private final ApexOperator operator;
	private DAG dag;
	
	// TODO: should we put parallelism hint here?
	// imo, parallelism hint only declared when we add this PI in the topology
	// open for dicussion :p

	// Constructor
	ApexProcessingItem(Processor processor, int parallelismHint) {
		this(processor, UUID.randomUUID().toString(), parallelismHint);
	}

	// Constructor
	ApexProcessingItem(Processor processor, String friendlyId, int parallelismHint) {
		super(processor, parallelismHint);
		this.operator = new ApexOperator(processor, parallelismHint);
		this.setName(friendlyId);
	}
	
	@Override
	protected ProcessingItem addInputStream(Stream inputStream, PartitioningScheme scheme) {
		ApexStream apexStream = (ApexStream) inputStream;
		
		// Setup stream codecs here
		switch(scheme) {
		case SHUFFLE:
			break;
		case BROADCAST:
			break;
		case GROUP_BY_KEY:
			break;
		default:
			// Should never occur
		}
		this.operator.addInputStream(apexStream);
		dag.addStream(apexStream.getStreamId(), apexStream.outputPort, apexStream.inputPort);
		return this;
	}

	@Override
	public void addToTopology(ApexTopology topology, int parallelismHint) {
		DAG dag = topology.getDAG();
		this.dag = dag;
		this.operator.instances = parallelismHint;
		dag.addOperator(this.getName(), this.operator);
	}

	@Override
	public ApexStream createStream() {
		return operator.createStream(this.getName());
	}

	@Override
	public String getId() {
		return this.getName();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(super.toString());
		sb.insert(0, String.format("id: %s, ", this.getName()));
		return sb.toString();
	}

	private final static class ApexOperator extends BaseOperator {

		private static final long serialVersionUID = -6637673741263199198L;
		private final Processor processor;
		private int instances = 1; // Default
		
		public boolean[] usedInputPorts = new boolean[]{false, false, false};
		public boolean[] usedOutputPorts = new boolean[]{false, false, false};

		@InputPortFieldAnnotation(optional=true)
		public transient DefaultInputPort<ContentEvent> inputPort0 = new DefaultInputPort<ContentEvent>() {
			@Override
			public void process(ContentEvent tuple) {
				processor.process(tuple);
			}
		};
		@InputPortFieldAnnotation(optional=true)
		public transient DefaultInputPort<ContentEvent> inputPort1 = new DefaultInputPort<ContentEvent>() {
			@Override
			public void process(ContentEvent tuple) {
				processor.process(tuple);
			}
		};
		@InputPortFieldAnnotation(optional=true)
		public transient DefaultInputPort<ContentEvent> inputPort2 = new DefaultInputPort<ContentEvent>() {
			@Override
			public void process(ContentEvent tuple) {
				processor.process(tuple);
			}
		};

		@OutputPortFieldAnnotation(optional=true)
		public transient DefaultOutputPort<ContentEvent> outputPort0 = new DefaultOutputPort<ContentEvent>();
		@OutputPortFieldAnnotation(optional=true)
		public transient DefaultOutputPort<ContentEvent> outputPort1 = new DefaultOutputPort<ContentEvent>();
		@OutputPortFieldAnnotation(optional=true)
		public transient DefaultOutputPort<ContentEvent> outputPort2 = new DefaultOutputPort<ContentEvent>();

		ApexOperator(Processor processor, int parallelismHint) {
			this.processor = processor;
			this.instances = parallelismHint;
		}
		
		public ApexStream createStream(String id) {
			ApexStream stream = new ApexStream(id);
			if(!usedOutputPorts[0]) {
				stream.outputPort = outputPort0;
				usedOutputPorts[0] = true;
			}
			else if(!usedOutputPorts[1]) {
				stream.outputPort = outputPort1;
				usedOutputPorts[1] = true;
			}
			else if(!usedOutputPorts[2]) {
				stream.outputPort = outputPort2;
				usedOutputPorts[2] = true;
			}
			else {
				throw new RuntimeException("Need more input ports for ApexOperator");
			}
			return stream;
		}

		public void addInputStream(ApexStream stream) {
			if(!usedInputPorts[0]) {
				stream.inputPort = inputPort0;
				usedInputPorts[0] = true;
			}
			else if(!usedInputPorts[1]) {
				stream.inputPort = inputPort1;
				usedInputPorts[1] = true;
			}
			else if(!usedInputPorts[2]) {
				stream.inputPort = inputPort2;
				usedInputPorts[2] = true;
			}
			else {
				throw new RuntimeException("Need more input ports for ApexOperator");
			}
		}
	}
}
