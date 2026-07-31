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

package org.apache.cassandra.db.compaction;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongUnaryOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.timeseries.WindowRoutingIterator;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.WrappedLifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TimeUUID;

/**
 * TSCS T3's local re-freeze for legacy SPANNING sstables (design spec sections 4 and 10): rewrites
 * exactly one sstable whose write timestamps straddle window boundaries into one output sstable per
 * window, each fully contained in its window. Once split, each window classifies FROZEN (single
 * contained sstable) or FREEZING on its own in later rounds - this task never fires
 * {@code WindowFrozenListener} itself (plan D7: freeze completion, and its event, belong to the
 * regular {@link FreezeCompactionTask} path).
 *
 * Write choreography follows anticompaction (CompactionManager.antiCompactGroup): several
 * {@link SSTableRewriter}s over one shared {@link LifecycleTransaction}, committed together after
 * the originals are obsoleted. Spanning sstables cannot be produced by TSCS itself any more (the
 * flush/streaming writer splits at boundaries); they exist only from pre-T3 data or a strategy
 * switch, so this path is a one-shot migration per sstable.
 */
public class SplitRefreezeCompactionTask extends AbstractCompactionTask
{
    private static final Logger logger = LoggerFactory.getLogger(SplitRefreezeCompactionTask.class);

    private final long gcBefore;
    private final LongUnaryOperator windowStartOfMillis;
    private final TimeUnit tableResolution;

    public SplitRefreezeCompactionTask(ColumnFamilyStore cfs,
                                       LifecycleTransaction txn,
                                       long gcBefore,
                                       LongUnaryOperator windowStartOfMillis,
                                       TimeUnit tableResolution)
    {
        super(cfs, txn);
        this.gcBefore = gcBefore;
        this.windowStartOfMillis = windowStartOfMillis;
        this.tableResolution = tableResolution;
    }

    private ActiveCompactionsTracker activeCompactions;

    @Override
    protected void executeInternal(ActiveCompactionsTracker activeCompactions)
    {
        this.activeCompactions = activeCompactions == null ? ActiveCompactionsTracker.NOOP : activeCompactions;
        run();
    }

    @Override
    protected void runMayThrow()
    {
        Set<SSTableReader> originals = transaction.originals();
        SSTableReader spanning = originals.iterator().next();
        assert originals.size() == 1 : "split-refreeze rewrites exactly one spanning sstable, got " + originals;

        long nowInSec = FBUtilities.nowInSeconds();
        long maxDataAge = CompactionTask.getMaxDataAge(originals);
        Map<Long, SSTableRewriter> rewriters = new TreeMap<>();
        Throwable err = null;

        try (SharedTxn shared = new SharedTxn(transaction);
             ISSTableScanner scanner = spanning.getScanner();
             CompactionController controller = new CompactionController(cfs, originals, gcBefore);
             CompactionIterator ci = new CompactionIterator(OperationType.COMPACTION, List.of(scanner), controller,
                                                            nowInSec, TimeUUID.Generator.nextTimeUUID()))
        {
            activeCompactions.beginCompaction(ci);
            try
            {
                while (ci.hasNext())
                {
                    try (UnfilteredRowIterator partition = ci.next())
                    {
                        for (Map.Entry<Long, UnfilteredRowIterator> entry : WindowRoutingIterator.slices(partition, windowStartOfMillis, tableResolution).entrySet())
                            rewriterFor(rewriters, entry.getKey(), shared, maxDataAge, originals).append(entry.getValue());
                    }
                }

                for (SSTableRewriter rewriter : rewriters.values())
                    rewriter.prepareToCommit();
                transaction.checkpoint();
                transaction.obsoleteOriginals();
                transaction.prepareToCommit();
                Throwable t = null;
                for (SSTableRewriter rewriter : rewriters.values())
                    t = rewriter.commit(t);
                t = transaction.commit(t);
                Throwables.maybeFail(t);
                logger.info("Split spanning sstable {} of {}.{} into {} window-contained sstable(s)",
                            spanning, cfs.getKeyspaceName(), cfs.getTableName(), rewriters.size());
            }
            finally
            {
                activeCompactions.finishCompaction(ci);
            }
        }
        catch (Throwable t)
        {
            err = t;
            throw t;
        }
        finally
        {
            // Committed rewriters no-op on close; on the error path close() aborts them.
            err = Throwables.close(err, rewriters.values());
            if (err != null)
                Throwables.maybeFail(err);
        }
    }

    private SSTableRewriter rewriterFor(Map<Long, SSTableRewriter> rewriters,
                                        long windowStart,
                                        ILifecycleTransaction shared,
                                        long maxDataAge,
                                        Set<SSTableReader> originals)
    {
        SSTableRewriter rewriter = rewriters.get(windowStart);
        if (rewriter == null)
        {
            rewriter = SSTableRewriter.constructWithoutEarlyOpening(shared, false, maxDataAge);
            rewriter.switchWriter(createWriter(shared, originals));
            rewriters.put(windowStart, rewriter);
        }
        return rewriter;
    }

    private SSTableWriter createWriter(ILifecycleTransaction shared, Set<SSTableReader> originals)
    {
        Descriptor descriptor = cfs.newSSTableDescriptor(cfs.getDirectories().getDirectoryForNewSSTables());
        return descriptor.getFormat().getWriterFactory().builder(descriptor)
                         .setKeyCount(originals.iterator().next().estimatedKeys())
                         .setRepairedAt(CompactionTask.getMinRepairedAt(originals))
                         .setPendingRepair(CompactionTask.getPendingRepair(originals))
                         .setTransientSSTable(CompactionTask.getIsTransient(originals))
                         .setTableMetadataRef(cfs.metadata)
                         .setMetadataCollector(new MetadataCollector(cfs.metadata().comparator))
                         .setSerializationHeader(SerializationHeader.make(cfs.metadata(), originals))
                         .addDefaultComponents(cfs.indexManager.listIndexGroups())
                         .setSecondaryIndexGroups(cfs.indexManager.listIndexGroups())
                         .setCompressionDictionaryManager(cfs.compressionDictionaryManager())
                         .build(shared, cfs);
    }

    /** Same shape as anticompaction's SharedTxn: lets several rewriters share one transaction whose lifecycle the task drives. */
    private static final class SharedTxn extends WrappedLifecycleTransaction implements AutoCloseable
    {
        SharedTxn(ILifecycleTransaction delegate)
        {
            super(delegate);
        }

        @Override
        public Throwable commit(Throwable accumulate)
        {
            return accumulate;
        }

        @Override
        public void prepareToCommit()
        {
        }

        @Override
        public void checkpoint()
        {
        }

        @Override
        public void obsoleteOriginals()
        {
        }

        @Override
        public void close()
        {
        }
    }
}
