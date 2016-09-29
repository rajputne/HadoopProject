/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Business;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author n
 */
public class Ip implements Comparable<Ip> {

    private String ip;
    private int count;

    public Ip() {

    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return ip;
    }

    @Override
    public int compareTo(Ip myIp) {
        if (this.ip.equals(myIp.getIp())) {
            return 0;
        }
        if (myIp.getCount() > this.getCount()) {
            return 1;
        }
        return -1;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Ip otherIp = (Ip) obj;
        if (!this.ip.equals(otherIp.ip)) {
            return false;
        }
        return true;
    }
}
