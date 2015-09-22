package org.apache.samoa.apex;

import org.apache.samoa.core.ContentEvent;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.FieldSerializer.Bind;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

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

public class ApexEvent {

  String key;
  @Bind(JavaSerializer.class)
  ContentEvent value;
  
  public ApexEvent() {
  }
  
  public ApexEvent(ContentEvent event) {
    key = event.getKey();
    value = event;
  }
  
  public ContentEvent get() {
    return value;
  }
}
