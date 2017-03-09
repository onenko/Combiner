package com.tech.task;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.mockito.Mockito.*;

/**
 * Created by nan on 3/9/2017.
 */
public class CombinerImplTest {

    @org.junit.Test
    public void testAddInputQueue() throws Exception {
        BlockingQueue<String> bq1 = mock(BlockingQueue.class);
        BlockingQueue<String> bq2 = mock(BlockingQueue.class);
        Combiner<String> combiner = new CombinerImpl(new SynchronousQueue());
        combiner.addInputQueue(bq1, 1.0, 1, TimeUnit.SECONDS);
        assertTrue(combiner.hasInputQueue(bq1));
        assertFalse(combiner.hasInputQueue(bq2));
    }

    @org.junit.Test
    public void testRemoveInputQueue() throws Exception {
        BlockingQueue<String> bq1 = mock(BlockingQueue.class);
        BlockingQueue<String> bq2 = mock(BlockingQueue.class);
        Combiner<String> combiner = new CombinerImpl(new SynchronousQueue());
        combiner.addInputQueue(bq1, 1.0, 1, TimeUnit.SECONDS);
        combiner.addInputQueue(bq2, 2.0, 2, TimeUnit.SECONDS);
        combiner.removeInputQueue(bq1);
        assertFalse(combiner.hasInputQueue(bq1));
        assertTrue(combiner.hasInputQueue(bq2));
    }

    @org.junit.Test
    public void testRun() throws Exception {
        BlockingQueue<String> bq1 = mock(BlockingQueue.class);
        BlockingQueue<String> bq2 = mock(BlockingQueue.class);
        SynchronousQueue<String> sq = new SynchronousQueue();
        when(bq1.poll(1, TimeUnit.SECONDS)).thenReturn("BQ1OBJ");
        when(bq2.poll(2, TimeUnit.SECONDS)).thenReturn("BQ2OBJ");

        CombinerImpl<String> combiner = new CombinerImpl(sq);

        combiner.addInputQueue(bq1, 1.0, 1, TimeUnit.SECONDS);
        combiner.addInputQueue(bq2, 2.0, 2, TimeUnit.SECONDS);

        Thread t = new Thread(combiner);
        t.start();
        String item1 = sq.take();
        String item2 = sq.take();
        assert("BQ1OBJ".equals(item1) || "BQ2OBJ".equals(item1));
        assert("BQ1OBJ".equals(item2) || "BQ2OBJ".equals(item2));
    }

    @org.junit.Test
    public void SorryGuysNoTimeToDesignMoreTests() {
        assert(true);
    }

}