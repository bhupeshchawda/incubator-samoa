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

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Values;

import com.datatorrent.api.DefaultOutputPort;
import org.apache.samoa.core.ContentEvent;

/**
 * Storm Stream that connects into Bolt. It wraps Storm's outputCollector class
 * 
 * @author Arinto Murdopo
 * 
 */
class ApexBoltStream extends ApexStream {

  /**
	 * 
	 */
  private static final long serialVersionUID = -5712513402991550847L;

  private DefaultOutputPort<ContentEvent> outputCollector;

  ApexBoltStream(String stormComponentId) {
    super(stormComponentId);
  }

  @Override
  public void put(ContentEvent contentEvent) {
    outputCollector.emit(contentEvent);
  }

  // @Override
  // public void setStreamId(String streamId) {
  // // TODO Auto-generated method stub
  // //this.outputStreamId = streamId;
  // }

  @Override
  public String getStreamId() {
    // TODO Auto-generated method stub
    return null;
  }
}