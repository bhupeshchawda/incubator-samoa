package org.apache.samoa.topology.impl;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.samoa.core.ContentEvent;

import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Partitioner.Partition;
import com.datatorrent.stram.plan.logical.DefaultKryoStreamCodec;
import com.google.common.collect.Lists;

public class StreamUtils
{
  public static class KeyBasedStreamCodec<T extends ContentEvent> extends DefaultKryoStreamCodec<T>
  {
    /**
     * KeyBasedStreamCodec - Distributes tuples to down stream operators such that each tuple with same key goes to same
     * partition
     */
    private static final long serialVersionUID = -7144877905889718517L;

    @Override
    public int getPartition(T t)
    {
      return t.getKey().hashCode();
    }
  }

  public static class RandomStreamCodec<T extends ContentEvent> extends DefaultKryoStreamCodec<T>
  {
    /**
     * RandomStreamCodec - Distributes tuples to down stream partitions randomly
     */
    private static final long serialVersionUID = -7522462490354605783L;
    private Random r;

    public RandomStreamCodec()
    {
      r = new Random();
    }

    @Override
    public int getPartition(T t)
    {
      return r.nextInt();
    }
  }

  public static class AllPartitioner<T> implements Partitioner<T>, Serializable
  {
    /**
     * 
     */
    private static final long serialVersionUID = 7203316682043269713L;
    private int newPartitionCount;
    
    public AllPartitioner(int n)
    {
      newPartitionCount = n;
    }

    @Override
    public Collection<com.datatorrent.api.Partitioner.Partition<T>> definePartitions(
            Collection<com.datatorrent.api.Partitioner.Partition<T>> partitions,
            com.datatorrent.api.Partitioner.PartitioningContext context)
    {
      //Get a partition
      DefaultPartition<T> partition = (DefaultPartition<T>)partitions.iterator().next();
      Collection<Partition<T>> newPartitions;

      // first call to define partitions
      newPartitions = Lists.newArrayList();

      for (int partitionCounter = 0; partitionCounter < newPartitionCount; partitionCounter++) {
        newPartitions.add(new DefaultPartition<T>(partition.getPartitionedInstance()));
      }
      List<InputPort<?>> inputPortList = context.getInputPorts();
      if (inputPortList != null && !inputPortList.isEmpty()) {
        DefaultPartition.assignPartitionKeys(newPartitions, inputPortList.iterator().next());
      }

      return newPartitions;
    }

    @Override
    public void partitioned(Map<Integer, com.datatorrent.api.Partitioner.Partition<T>> partitions)
    {
    }

    public int getNewPartitionCount()
    {
      return newPartitionCount;
    }

    public void setNewPartitionCount(int newPartitionCount)
    {
      this.newPartitionCount = newPartitionCount;
    }
  }

}
