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

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.stram.plan.physical.StreamMapping;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.topology.Stream;

/**
 * Storm Stream that connects into Bolt. It wraps Storm's outputCollector class
 * 
 * @author Arinto Murdopo
 * 
 */
class ApexStream implements Stream, java.io.Serializable {

	private static final long serialVersionUID = -5712513402991550847L;

	private String streamId = "";
	public transient DefaultInputPort<ContentEvent> inputPort = null;
	public transient DefaultOutputPort<ContentEvent> outputPort = null;

	public ApexStream(String id) {
		streamId = UUID.randomUUID()+"";
	}
	
	@Override
	public void put(ContentEvent contentEvent) {
		outputPort.emit(contentEvent);
	}

	@Override
	public String getStreamId() {
		return streamId;
	}

	@Override
	public void setBatchSize(int batchsize) {}
}
