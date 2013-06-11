package com.intel.hbase.test.util;

public final class TimeCounter {

    protected long _start;
    protected long _end;
    protected long _pin = 0;
    protected long _time = 0;

    public void begin() {
        _start = TimeUtil.now();
    }

    public void end() {
        _end = TimeUtil.now();
    }

    public void enter() {
        _pin = TimeUtil.now();
    }

    public long leave() {
        long t = getCurrentCounter();
        _time += t;
        return t;
    }

    public void beginAndEnter() {
        begin();
        enter();
    }

    public void leaveAndEnd() {
        leave();
        end();
    }

    public long getCounter() {
        return _time;
    }

    public long getCurrentCounter() {
        return TimeUtil.now() - _pin;
    }

    public long getTimePeriod() {
        return _end - _start;
    }

    public String toString() {
        return "[" + TimeUtil.longToDateString(_start) + " -> "
                + TimeUtil.longToDateString(_end) + "("
                + TimeUtil.timeMillisToString(_end - _start) + ")]"
                + TimeUtil.timeMillisToString(_time);
    }

    public String getStartString() {
        return TimeUtil.longToDateString(_start);
    }

    public String getEndString() {
        return TimeUtil.longToDateString(_end);
    }

    public String getTimeString() {
        return TimeUtil.timeMillisToString(_time);
    }

}
