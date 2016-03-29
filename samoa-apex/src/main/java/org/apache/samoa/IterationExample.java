package org.apache.samoa;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.common.util.DefaultDelayOperator;

@ApplicationAnnotation(name="IterationExample")
public class IterationExample implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RandomInput input = dag.addOperator("Input", RandomInput.class);
    Process process = dag.addOperator("Process", Process.class);
    DefaultDelayOperator<KeyedEvent> delay = dag.addOperator("Delay", DefaultDelayOperator.class);
    ConsoleOutput console = dag.addOperator("Console", ConsoleOutput.class);

    dag.addStream("Input to Process", input.output, process.input);
    dag.addStream("Process to Delay", process.loopOut, delay.input);
    dag.addStream("Delay to Process", delay.output, process.loopIn);
    dag.addStream("Process to Console", process.output, console.input);

    dag.getAttributes().put(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS, 10);
  }

  public static class KeyedEvent
  {
    int key;
    double data;
    public KeyedEvent()
    {
    }
    public KeyedEvent(int key, double data)
    {
      this.key = key;
      this.data = data;
    }
    @Override
    public String toString()
    {
      return "{" + key + "," + data + "}";
    }
  }

  public static class RandomInput extends BaseOperator implements InputOperator
  {
    Random r;
    int i;

    public final transient DefaultOutputPort<KeyedEvent> output = new DefaultOutputPort<>(); 

    @Override
    public void setup(OperatorContext context)
    {
      r = new Random();
      i = 0;
    }

    @Override
    public void emitTuples()
    {
      KeyedEvent event = new KeyedEvent(i++, r.nextDouble()*1000000);
      output.emit(event);
    }
  }

  public static class Process extends BaseOperator
  {
    public final transient DefaultInputPort<KeyedEvent> input = new DefaultInputPort<KeyedEvent>()
    {
      @Override
      public void process(KeyedEvent tuple)
      {
        loopOut.emit(tuple);
      }
    };

    public final transient DefaultInputPort<KeyedEvent> loopIn = new DefaultInputPort<KeyedEvent>()
    {
      @Override
      public void process(KeyedEvent tuple)
      {
        if(tuple.data <= 10) {
          output.emit(tuple);
        } else {
          tuple.data = Math.sqrt(tuple.data);
          loopOut.emit(tuple);
        }
      }
    };

    public final transient DefaultOutputPort<KeyedEvent> output = new DefaultOutputPort<>();
    public final transient DefaultOutputPort<KeyedEvent> loopOut = new DefaultOutputPort<>();
  }

  public static class ConsoleOutput extends BaseOperator
  {
    public final transient DefaultInputPort<KeyedEvent> input = new DefaultInputPort<KeyedEvent>()
    {
      @Override
      public void process(KeyedEvent tuple)
      {
        System.out.println(tuple);
      }
    };
  }
}
