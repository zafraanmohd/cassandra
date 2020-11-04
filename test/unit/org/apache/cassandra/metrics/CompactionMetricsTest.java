/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.metrics;

import java.util.UUID;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.ArrayList;

import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CompactionInfo;
import java.util.List;
import org.apache.cassandra.db.ColumnFamilyStore;
import java.util.Map;
import java.util.Set;
import java.util.Collection;

import org.apache.cassandra.db.compaction.CompactionIterator;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import java.util.HashSet;
import org.apache.cassandra.schema.MockSchema;
import org.junit.Test;
import org.junit.Assert;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertNotNull;

public class CompactionMetricsTest extends CQLTester
{
    @Test
    public void testSimpleCompactionMetricsForCompletedTasks() throws Throwable {
        final long initialCompletedTasks = CompactionManager.instance.getCompletedTasks();
        Assert.assertEquals(0L, CompactionManager.instance.getTotalCompactionsCompleted());
        Assert.assertEquals(0L, CompactionManager.instance.getTotalBytesCompacted());
        this.createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY (pk, ck))");
        this.getCurrentColumnFamilyStore().disableAutoCompaction();
        for (int i = 0; i < 10; ++i) {
            this.execute("INSERT INTO %s (pk, ck, a, b) VALUES (" + i + ", 2, 3, 4)", new Object[0]);
        }
        this.getCurrentColumnFamilyStore().forceBlockingFlush();
        this.getCurrentColumnFamilyStore().forceMajorCompaction();
        Assert.assertEquals(1L, CompactionManager.instance.getTotalCompactionsCompleted());
        Assert.assertTrue(CompactionManager.instance.getTotalBytesCompacted() > 0L);
        Assert.assertTrue(initialCompletedTasks < CompactionManager.instance.getCompletedTasks());
    }

    @Test
    public void testCompactionMetricsForPendingTasks() throws InterruptedException {
        final ColumnFamilyStore cfs = MockSchema.newCFS();
        final List<SSTableReader> sstables = this.createSSTables(cfs, 10, 0);
        final Set<SSTableReader> toMarkCompacting = new HashSet<SSTableReader>(sstables.subList(0, 3));
        final CompactionMetricsTest.TestCompactionTask tct = new CompactionMetricsTest.TestCompactionTask(cfs, (Set)toMarkCompacting);
        try {
            final long initialPendingTasks = (int)CompactionManager.instance.getMetrics().pendingTasks.getValue();
            tct.start();
            Assert.assertTrue(((Map)CompactionManager.instance.getMetrics().pendingTasksByTableName.getValue()).containsKey(cfs.keyspace.getName()));
            Assert.assertTrue((int)CompactionManager.instance.getMetrics().pendingTasks.getValue() > initialPendingTasks);
            CompactionMetricsTest.logger.info("finish testing");
        }
        finally {
            tct.abort();
        }
    }

    @Test
    public void testCompactionMetricsForFailedTasks() throws Throwable {
        final ColumnFamilyStore cfs = MockSchema.newCFS();
        final List<SSTableReader> sstables = this.createSSTables(cfs, 10, 0);
        final Set<SSTableReader> toMarkCompacting = new HashSet<SSTableReader>(sstables.subList(0, 3));
        final CompactionMetricsTest.TestCompactionTask tct = new CompactionMetricsTest.TestCompactionTask(cfs, (Set)toMarkCompacting);
        try {
            tct.start();
            final List<CompactionInfo.Holder> activeCompactions = this.getActiveCompactionsForTable(cfs);
            Assert.assertEquals(1L, (long)activeCompactions.size());
            Assert.assertEquals((Object)activeCompactions.get(0).getCompactionInfo().getSSTables(), (Object)toMarkCompacting);
            cfs.runWithCompactionsDisabled(() -> null, sstable -> !toMarkCompacting.contains(sstable), false, false, true);
            Assert.assertEquals(1L, (long)activeCompactions.size());
            Assert.assertFalse(activeCompactions.get(0).isStopRequested());
            activeCompactions.forEach(x -> CompactionMetricsTest.logger.info("metro \n\n{}", (Object)x.isStopRequested()));
            tct.abort();
        }
        finally {
            tct.abort();
        }
    }

    private List<SSTableReader> createSSTables(ColumnFamilyStore cfs, int count, int startGeneration)
    {
        List<SSTableReader> sstables = new ArrayList<>();
        for (int i = 0; i < count; i++)
        {
            long first = i * 10;
            long last  = (i + 1) * 10 - 1;
            sstables.add(MockSchema.sstable(startGeneration + i, 0, true, first, last, cfs));
        }
        cfs.disableAutoCompaction();
        cfs.addSSTables(sstables);
        return sstables;
    }

    private List<CompactionInfo.Holder> getActiveCompactionsForTable(ColumnFamilyStore cfs)
    {
        return CompactionManager.instance.active.getCompactions()
                                                .stream()
                                                .filter(holder -> holder.getCompactionInfo().getTable().orElse("unknown").equalsIgnoreCase(cfs.name))
                                                .collect(Collectors.toList());
    }

    private static class TestCompactionTask
    {
        private ColumnFamilyStore cfs;
        private final Set<SSTableReader> sstables;
        private LifecycleTransaction txn;
        private CompactionController controller;
        private CompactionIterator ci;
        private List<ISSTableScanner> scanners;

        public TestCompactionTask(ColumnFamilyStore cfs, Set<SSTableReader> sstables)
        {
            this.cfs = cfs;
            this.sstables = sstables;
        }

        public void start()
        {
            scanners = sstables.stream().map(SSTableReader::getScanner).collect(Collectors.toList());
            txn = cfs.getTracker().tryModify(sstables, OperationType.COMPACTION);
            assertNotNull(txn);
            controller = new CompactionController(cfs, sstables, Integer.MIN_VALUE);
            ci = new CompactionIterator(txn.opType(), scanners, controller, FBUtilities.nowInSeconds(), UUID.randomUUID());
            CompactionManager.instance.active.beginCompaction(ci);
        }

        public void abort()
        {
            if (controller != null)
                controller.close();
            if (ci != null)
                ci.close();
            if (txn != null)
                txn.abort();
            if (scanners != null)
                scanners.forEach(ISSTableScanner::close);
            CompactionManager.instance.active.finishCompaction(ci);

        }

        public void cancelAndAbort()
        {
            if (controller != null)
                controller.close();
            if (ci != null)
                ci.close();
            if (txn != null)
                txn.abort();
            if (scanners != null)
                scanners.forEach(ISSTableScanner::close);

            CompactionManager.instance.incrementAborted();
            CompactionManager.instance.incrementCompactionsReduced();
            CompactionManager.instance.incrementSstablesDropppedFromCompactions(sstables.size());

        }
    }
}