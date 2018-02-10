package com.pralay.HbaseFullLoad;

import java.io.Serializable;

public class MccMnc implements Serializable {
    private int mcc;
    private int mnc;

    public MccMnc(int mcc, int mnc) {
        this.mcc = mcc;
        this.mnc = mnc;
    }

    public MccMnc() {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MccMnc mccMnc = (MccMnc) o;

        if (mcc != mccMnc.mcc) return false;
        return mnc == mccMnc.mnc;

    }

    @Override
    public int hashCode() {
        int result = mcc;
        result = 31 * result + mnc;
        return result;
    }

    @Override
    public String toString() {
        return "MccMnc{" +
                "mcc=" + mcc +
                ", mnc=" + mnc +
                '}';
    }

    public int getMcc() {
        return mcc;
    }

    public void setMcc(int mcc) {
        this.mcc = mcc;
    }

    public int getMnc() {
        return mnc;
    }

    public void setMnc(int mnc) {
        this.mnc = mnc;
    }
}
