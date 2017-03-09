package com.tech.task;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

public class CombinerImpl<T> extends Combiner<T> implements Runnable {

    private static final double MAX_PRTY = 100000.0;
    private static final long MAX_TIMEOUT = 100000;

    private List<InputSlot<T>> slots;
    private InputSlot<T> slotBeingPolled;
    private boolean delSlotBeingPolled;
    private double totalWeight;
    private boolean notDone;

    public CombinerImpl(SynchronousQueue<T> outputQueue) {
        super(outputQueue);
        slots = new LinkedList<InputSlot<T>>();
        totalWeight = 0.0D;
        notDone = true;
        slotBeingPolled = null; // used to avoid deleting the slot which is being polled for input item
        delSlotBeingPolled = false;
    }

    @Override
    public void addInputQueue(BlockingQueue<T> queue, double priority, long isEmptyTimeout, TimeUnit timeUnit) throws Combiner.CombinerException {
        if(priority <= 0.0 || priority > MAX_PRTY) {
            throw new Combiner.CombinerException("Priority out of valid range: ]0.." + MAX_PRTY + ']');
        }
        if(isEmptyTimeout <= 0 || isEmptyTimeout > MAX_TIMEOUT) {
            // TODO: exact range must depend on timeUnit
            throw new Combiner.CombinerException("isEmptyTimeout out of valid range: ]0.." + MAX_TIMEOUT + ']');
        }
        InputSlot<T> slot = new InputSlot<T>(queue, priority, isEmptyTimeout, timeUnit);
        addToSlots(slot);
    }

    @Override
    public void removeInputQueue(BlockingQueue<T> queue) throws Combiner.CombinerException {
        if( ! deleteFromSlots(queue)) {
            throw new Combiner.CombinerException("Queue to delete not found:" + queue);
        };
    }

    @Override
    public boolean hasInputQueue(BlockingQueue<T> queue) {
        return isInSlots(queue);
    }

    @Override
    public void run() {
        while(notDone) {
            InputSlot<T> slot = pickSlot();
            T cargo = slot.poll();
            boolean timeout = cargo == null;
            postDeletePolledSlot(timeout);
            if( ! timeout) {
                try {
                    outputQueue.put(cargo);
                } catch(InterruptedException ignored) {
                }
            }
        }
    }

    public void stopProcessing() {
        notDone = false;
    }

    /**
     * adds the slot to slots list
     * We allow the same queue to be used multiple times, on discretion of client
     */
    synchronized private void addToSlots(InputSlot<T> slot) {
        slots.add(slot);
        totalWeight += slot.priority;
    }

    synchronized private boolean isInSlots(BlockingQueue<T> queue) {
        for(InputSlot<T> slot: slots) {
            if(slot.queue == queue) {   // compare references
                return true;
            }
        }
        return false;
    }

    synchronized private boolean deleteFromSlots(BlockingQueue<T> queue) {
        for(InputSlot<T> slot: slots) {
            if(slot.queue == queue) {   // compare references
                slots.remove(slot);
                totalWeight -= slot.priority;
                return true;
            }
        }
        return false;
    }

    /**
     * Decides to delete slot with recently polled BlockingQueue.
     * The delete occurs if deleteDueToTimeout == true
     * The delete occurs if during the poll a client asked to delete polled queue,
     * this use case is detected with delSlotBeingPolled
     */
    synchronized private void postDeletePolledSlot(boolean deleteDueToTimeout) {
        if(deleteDueToTimeout || delSlotBeingPolled) {
            totalWeight -= slotBeingPolled.priority;
            slots.remove(slotBeingPolled);
        }
        slotBeingPolled = null;
        delSlotBeingPolled = false;
    }

    synchronized private InputSlot<T> pickSlot() {
        double weight = Math.random() * totalWeight;
        for(InputSlot<T> slot: slots) {
            weight -= slot.priority;
            if(weight < 0.0) {
                slotBeingPolled = slot;     // this slot can not be deleted
                return slot;
            }
        }
        return null;    // on empty list
    }


    /**
     * Stores one InputQueue in the local list
     *
     * As it is used only internally, keep it simple and do not use setters/getters
     */
    private static class InputSlot<T> {
        public BlockingQueue<T> queue;
        public double priority;
        public long isEmptyTimeout;
        public TimeUnit timeUnit;

        public InputSlot(BlockingQueue<T> queue, double priority, long isEmptyTimeout, TimeUnit timeUnit) {
            this.queue = queue;
            this.priority = priority;
            this.isEmptyTimeout = isEmptyTimeout;
            this.timeUnit = timeUnit;
        }

        public T poll() {
            T cargo = null;
            try {
                cargo = queue.poll(isEmptyTimeout, timeUnit);
            } catch(InterruptedException ignored) {
            }
            return cargo;
        }
    }
}
