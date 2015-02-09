package com.infynyxx.storm.demo.wikipedia;

import static com.google.common.base.Preconditions.checkNotNull;

public class RankableObject implements Rankable {

    private final Object obj;
    private final long count;

    public RankableObject(Object obj, long count) {
        checkNotNull(obj, "rankable obj can't be null");
        this.obj = obj;
        this.count = count;
    }

    @Override
    public Object getObject() {
        return obj;
    }

    @Override
    public long getCount() {
        return count;
    }

    @Override
    public Rankable copy() {
        return new RankableObject(obj, count);
    }

    @Override
    public int compareTo(Rankable other) {
        long delta = this.getCount() - other.getCount();
        if (delta > 0) {
            return 1;
        } else if (delta < 0) {
            return -1;
        } else {
            return 0;
        }
    }

    @Override
    public String toString() {
        return String.valueOf(obj);
    }
}
