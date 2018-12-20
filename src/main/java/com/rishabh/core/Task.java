package com.rishabh.core;

import java.util.List;

public class Task {

    private List<Long> totalWork;
    private int start, end;
    private long batch_size;
    private int threadNum;
    private int current = 0;
    private boolean all_task_done = false;

    public Task(List<Long> skips, int start, int end, int threadNum, long batch_size) {
        this.totalWork = skips;
        this.start = start;
        this.current = this.start;
        this.end = end;
        this.threadNum = threadNum;
        this.batch_size = batch_size;
    }

    @Override
    public String toString() {
        String s = "";
        s += "range(" + String.valueOf(this.totalWork.get(this.start)) + ", " + String.valueOf(this.totalWork.get(this.end)) + ", "+String.valueOf(this.batch_size) + ")";
        return s;
    }

    public long getNextElement() {
        if(this.current == this.end) {
            all_task_done = true;
            return -1;
        }
        long next = this.totalWork.get(this.current);
        this.current += 1;
        return next;
    }
    
    public boolean getStatus() {
        return this.all_task_done;
    }
}