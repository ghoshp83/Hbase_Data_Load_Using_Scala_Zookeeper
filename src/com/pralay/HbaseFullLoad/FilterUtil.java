package com.pralay.HbaseFullLoad;

import java.io.Serializable;

import org.apache.log4j.Logger;

public class FilterUtil implements Serializable {

    private static final Logger LOGGER = Logger.getLogger(FilterUtil.class);

    public static final int DataType1_OLD_COLUMN_SIZE = 4;
    public static final int DataType1_NEW_COLUMN_SIZE = 5;

    public static final int DataType2_OLD_COLUMN_SIZE = 2;
    public static final int DataType2_NEW_COLUMN_SIZE = 4;


    public FilterUtil() {
    }

    public Boolean confirmEdcrSplit(Integer size) {
        ++nSplit; // Number of function calls ~ number of records
        if (size.equals(DataType2_OLD_COLUMN_SIZE) || size.equals(DataType2_NEW_COLUMN_SIZE)) {
            return true;
        } else {
            ++splitDrop; // Number of filtered records because of wrong number of splits
            return false;
        }
    }

    public Boolean confirmEsrSplit(Integer size) {
        ++nSplit; // Number of function calls ~ number of records
        if (size.equals(DataType1_OLD_COLUMN_SIZE) || size.equals(DataType1_NEW_COLUMN_SIZE)) {
            return true;
        } else {
            ++splitDrop; // Number of filtered records because of wrong number of splits
            return false;
        }
    }

    public Boolean confirmJsonParsing() {
        // TODO
        return true;
    }

    public Boolean confirmMoImsi(String str) {
        ++nMoImsi;
        if (str.isEmpty()) {
            ++moImsiDrop;
            // System.out.println("mo imsi drop");
            return false;
        } else {
            // System.out.println("has mo imsi: " + str);
            return true;
        }
    }

    public Boolean confirmMtImsi(String str) {
        ++nMtImsi;
        if (str.isEmpty()) {
            ++mtImsiDrop;
            return false;
        } else {
            return true;
        }
    }

    public Integer getSplitDrop() {
        return splitDrop;
    }

    public Integer getNSplit() {
        return nSplit;
    }

    public Integer getMoImsiDrop() {
        return moImsiDrop;
    }

    public Integer getNMoImsi() {
        return nMoImsi;
    }

    public Integer getMtImsiDrop() {
        return mtImsiDrop;
    }

    public Integer getNMtImsi() {
        return nMtImsi;
    }

    public void resetCounters() {
        splitDrop = 0;
        nSplit = 0;
        moImsiDrop = 0;
        nMoImsi = 0;
        mtImsiDrop = 0;
        nMtImsi = 0;
    }

    static private Integer splitDrop = 0;
    static private Integer nSplit = 0;
    static private Integer moImsiDrop = 0;
    static private Integer nMoImsi = 0;
    static private Integer mtImsiDrop = 0;
    static private Integer nMtImsi = 0;
}
