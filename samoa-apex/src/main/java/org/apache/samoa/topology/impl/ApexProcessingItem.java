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

import java.util.UUID;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;

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
		
		private DefaultInputPort<ContentEvent> inputPort = new DefaultInputPort<ContentEvent>() {
			@Override
			public void process(ContentEvent tuple) {
				processor.process(tuple);
			}
		};
		
		private DefaultOutputPort<ContentEvent> outputPort = new DefaultOutputPort<ContentEvent>();

		public DefaultInputPort<ContentEvent> getInputPort() {
			DefaultInputPort<ContentEvent> port = new DefaultInputPort<ContentEvent>() {
				@Override
				public void process(ContentEvent tuple) {
					processor.process(tuple);
				}
			};
			return port;
		}
		
		ApexOperator(Processor processor, int parallelismHint) {
			this.processor = processor;
			this.instances = parallelismHint;
		}
		
		public ApexStream createStream(String id) {
			ApexStream stream = new ApexStream(id);
			stream.outputPort = outputPort;
			return stream;
		}

		public void addInputStream(ApexStream stream) {
			stream.inputPort = inputPort;
		}
	}
}
