package com.pralay.HbaseFullLoad;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;


class HBasePartitioner extends Partitioner {

    /**/ private static Logger LOGGER = Logger.getLogger(HBasePartitioner.class);
    /**/ private boolean partitionUsed[];

    private TreeMap<String, Integer> partitionMap = new TreeMap<String, Integer>();
    private int numPartitions;

    public HBasePartitioner(List<HRegionInfo> regions) {
        for (HRegionInfo hregionInfo : regions) {
            if (!hregionInfo.isSplit()) {
                byte[] startKey = hregionInfo.getStartKey();
                if (startKey != null && startKey.length > 0) {
                    String startKeyAsString = Bytes.toString(startKey);
                    partitionMap.put(startKeyAsString, new Integer(0));
                }
            }
        }
        /**/
        StringBuilder startKeys = null;
        if (LOGGER.isDebugEnabled()) {
            startKeys = new StringBuilder("null");
        }
        int i = 1;
        for (Iterator<Map.Entry<String, Integer>> it = partitionMap.entrySet().iterator(); it.hasNext(); i++) {
            Map.Entry<String, Integer> entry = it.next();
            entry.setValue(i);
            /**/
            if (LOGGER.isDebugEnabled()) {
                startKeys.append(", " + entry.getKey());
            }
        }
        numPartitions = partitionMap.size() + 1;

        LOGGER.debug("HBasePartitioner created with " + numPartitions + " partition(s).\n");
        LOGGER.debug(startKeys == null ? "" : ("Start indices: " + startKeys.toString() + "\n"));
        if (LOGGER.isDebugEnabled()) {
            partitionUsed = new boolean[numPartitions];
            for (int partInd = 0; partInd < numPartitions; partInd++) {
                partitionUsed[partInd] = false;
            }
        }

    }

    @Override
    public int numPartitions() {
        return numPartitions;
    }

    @Override
    public int getPartition(Object key) {
        int partition = 0;
        String keyAsString = null;
        if (key instanceof String) {
            keyAsString = (String) key;
        } else if (key instanceof byte[]) {
            keyAsString = Bytes.toString((byte[]) key);
        }
        Map.Entry<String, Integer> entry = partitionMap.floorEntry(keyAsString);
        if (entry != null) {
            partition = entry.getValue().intValue();
        }
        if (LOGGER.isDebugEnabled()) {
            if (!partitionUsed[partition]) {
                partitionUsed[partition] = true;
                LOGGER.debug("Partition " + partition + " received first key: " + keyAsString + "\n");
            }
        }
        return partition;
    }
}
