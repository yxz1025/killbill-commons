/*
 * Copyright 2010-2014 Ning, Inc.
 * Copyright 2014-2020 Groupon, Inc
 * Copyright 2020-2020 Equinix, Inc
 * Copyright 2014-2020 The Billing Project, LLC
 *
 * The Billing Project licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.killbill.queue;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.killbill.CreatorName;
import org.killbill.clock.Clock;
import org.killbill.commons.profiling.Profiling;
import org.killbill.commons.profiling.ProfilingFeature;
import org.killbill.queue.api.PersistentQueueConfig;
import org.killbill.queue.api.PersistentQueueConfig.PersistentQueueMode;
import org.killbill.queue.api.PersistentQueueEntryLifecycleState;
import org.killbill.queue.dao.EventEntryModelDao;
import org.killbill.queue.dao.QueueSqlDao;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.Transaction;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * This class abstract the interaction with the database tables which store the persistent entries for the bus events or
 * notification events.
 * <p/>
 * <p>This can be configured to either cache the recordId for the entries that are ready be fetched so that we avoid expansive
 * queries to the database. Alternatively, the inflight queue is not used and the search query is always run when we need to retrieve
 * new entries.
 *
 * @param <T>
 */
public abstract class DBBackedQueue<T extends EventEntryModelDao> {

    protected static final Logger log = LoggerFactory.getLogger(DBBackedQueue.class);

    protected final String DB_QUEUE_LOG_ID;

    protected final IDBI dbi;
    protected final Class<? extends QueueSqlDao<T>> sqlDaoClass;
    protected final QueueSqlDao<T> sqlDao;
    protected final Clock clock;
    protected final PersistentQueueConfig config;

    //
    // All these *raw* time measurement only measure the query time *not* including the transaction and the time to acquire DB connection
    //
    // Time to get the ready entries (polling) or all entries from ids (inflight)
    protected final Timer rawGetEntriesTime;
    // Time to insert one entry in the DB
    protected final Timer rawInsertEntryTime;
    // Time to claim the batch of entries (STICKY_POLLING)
    protected final Timer rawClaimEntriesTime;
    // Time to claim one entry (POLLING mode)
    protected final Timer rawClaimEntryTime;
    // Time to move a batch of entries (delete from table + insert into history)
    protected final Timer rawDeleteEntriesTime;
    // Time to move one entry (delete from table + insert into history)
    protected final Timer rawDeleteEntryTime;

    protected final Profiling<Long, RuntimeException> prof;

    public DBBackedQueue(final Clock clock,
                         final IDBI dbi,
                         final Class<? extends QueueSqlDao<T>> sqlDaoClass,
                         final PersistentQueueConfig config,
                         final String dbBackedQId,
                         final MetricRegistry metricRegistry) {
        this.dbi = dbi;
        this.sqlDaoClass = sqlDaoClass;
        this.sqlDao = dbi.onDemand(sqlDaoClass);
        this.config = config;
        this.clock = clock;
        this.prof = new Profiling<Long, RuntimeException>();

        this.rawGetEntriesTime = metricRegistry.timer(MetricRegistry.name(DBBackedQueue.class, dbBackedQId, "rawGetEntriesTime"));
        this.rawInsertEntryTime = metricRegistry.timer(MetricRegistry.name(DBBackedQueue.class, dbBackedQId, "rawInsertEntryTime"));
        this.rawClaimEntriesTime = metricRegistry.timer(MetricRegistry.name(DBBackedQueue.class, dbBackedQId, "rawClaimEntriesTime"));
        this.rawClaimEntryTime = metricRegistry.timer(MetricRegistry.name(DBBackedQueue.class, dbBackedQId, "rawClaimEntryTime"));
        this.rawDeleteEntriesTime = metricRegistry.timer(MetricRegistry.name(DBBackedQueue.class, dbBackedQId, "rawDeleteEntriesTime"));
        this.rawDeleteEntryTime = metricRegistry.timer(MetricRegistry.name(DBBackedQueue.class, dbBackedQId, "rawDeleteEntryTime"));

        this.DB_QUEUE_LOG_ID = "DBBackedQueue-" + dbBackedQId;
    }

    public static class ReadyEntriesWithMetrics<T extends EventEntryModelDao> {

        private final List<T> entries;
        private final long time;

        public ReadyEntriesWithMetrics(final List<T> entries, final long time) {
            this.entries = entries;
            this.time = time;
        }

        public List<T> getEntries() {
            return entries;
        }

        public long getTime() {
            return time;
        }
    }

    public abstract void initialize();

    public abstract void close();

    public abstract ReadyEntriesWithMetrics<T> getReadyEntries();

    public abstract void insertEntryFromTransaction(final QueueSqlDao<T> transactional, final T entry);

    public abstract void updateOnError(final T entry);

    protected abstract void insertReapedEntriesFromTransaction(final QueueSqlDao<T> transactional, final List<T> entriesLeftBehind, final DateTime now);

    public void insertEntry(final T entry) {
        executeTransaction(new Transaction<Void, QueueSqlDao<T>>() {
            @Override
            public Void inTransaction(final QueueSqlDao<T> transactional, final TransactionStatus status) {
                insertEntryFromTransaction(transactional, entry);
                return null;
            }
        });
    }

    public void moveEntryToHistory(final T entry) {
        executeTransaction(new Transaction<Void, QueueSqlDao<T>>() {
            @Override
            public Void inTransaction(final QueueSqlDao<T> transactional, final TransactionStatus status) throws Exception {
                moveEntryToHistoryFromTransaction(transactional, entry);
                return null;
            }
        });
    }

    public void moveEntryToHistoryFromTransaction(final QueueSqlDao<T> transactional, final T entry) {
        try {
            switch (entry.getProcessingState()) {
                case FAILED:
                case PROCESSED:
                case REMOVED:
                case REAPED:
                    break;
                default:
                    log.warn("{} Unexpected terminal event state={} for record_id={}", DB_QUEUE_LOG_ID, entry.getProcessingState(), entry.getRecordId());
                    break;
            }

            log.debug("{} Moving entry into history: recordId={}, className={}, json={}", DB_QUEUE_LOG_ID, entry.getRecordId(), entry.getClassName(), entry.getEventJson());

            long ini = System.nanoTime();
            transactional.insertEntry(entry, config.getHistoryTableName());
            transactional.removeEntry(entry.getRecordId(), config.getTableName());
            rawDeleteEntryTime.update(System.nanoTime() - ini, TimeUnit.NANOSECONDS);

        } catch (final Exception e) {
            log.warn("{} Failed to move entry into history: {}", DB_QUEUE_LOG_ID, entry, e);
        }
    }

    public void moveEntriesToHistory(final Iterable<T> entries) {
        try {
            executeTransaction(new Transaction<Void, QueueSqlDao<T>>() {
                @Override
                public Void inTransaction(final QueueSqlDao<T> transactional, final TransactionStatus status) throws Exception {
                    moveEntriesToHistoryFromTransaction(transactional, entries);
                    return null;
                }
            });
        } catch (final Exception e) {
            log.warn("{} Failed to move entries into history: {}", DB_QUEUE_LOG_ID, entries, e);
        }
    }

    public void moveEntriesToHistoryFromTransaction(final QueueSqlDao<T> transactional, final Iterable<T> entries) {
        if (!entries.iterator().hasNext()) {
            return;
        }

        for (final T cur : entries) {
            switch (cur.getProcessingState()) {
                case FAILED:
                case PROCESSED:
                case REMOVED:
                case REAPED:
                    break;
                default:
                    log.warn("{} Unexpected terminal event state={} for record_id={}", DB_QUEUE_LOG_ID, cur.getProcessingState(), cur.getRecordId());
                    break;
            }
            log.debug("{} Moving entry into history: recordId={}, className={}, json={}", DB_QUEUE_LOG_ID, cur.getRecordId(), cur.getClassName(), cur.getEventJson());
        }

        final Iterable<Long> toBeRemovedRecordIds = Iterables.<T, Long>transform(entries, new Function<T, Long>() {
            @Override
            public Long apply(final T input) {
                return input == null ? Long.valueOf(-1) : input.getRecordId();
            }
        });
        final long ini = System.nanoTime();
        transactional.insertEntries(entries, config.getHistoryTableName());
        transactional.removeEntries(ImmutableList.<Long>copyOf(toBeRemovedRecordIds), config.getTableName());
        rawDeleteEntriesTime.update(System.nanoTime() - ini, TimeUnit.NANOSECONDS);
    }

    protected long getNbReadyEntries() {
        final Date now = clock.getUTCNow().toDate();
        return getNbReadyEntries(now);
    }

    public long getNbReadyEntries(final Date now) {
        final String owner = config.getPersistentQueueMode() == PersistentQueueMode.POLLING ? null : CreatorName.get();
        return executeQuery(new Query<Long, QueueSqlDao<T>>() {
            @Override
            public Long execute(final QueueSqlDao<T> queueSqlDao) {
                return queueSqlDao.getNbReadyEntries(now, owner, config.getTableName());
            }
        });
    }

    protected Long safeInsertEntry(final QueueSqlDao<T> transactional, final T entry) {
        return prof.executeWithProfiling(ProfilingFeature.ProfilingFeatureType.DAO, "QueueSqlDao:insert", new Profiling.WithProfilingCallback<Long, RuntimeException>() {

            @Override
            public Long execute() throws RuntimeException {
                final long init = System.nanoTime();

                final Long lastInsertId = transactional.insertEntry(entry, config.getTableName());
                if (lastInsertId > 0) {
                    log.debug("{} Inserting entry: lastInsertId={}, entry={}", DB_QUEUE_LOG_ID, lastInsertId, entry);
                } else {
                    log.warn("{} Error inserting entry: lastInsertId={}, entry={}", DB_QUEUE_LOG_ID, lastInsertId, entry);
                }

                rawInsertEntryTime.update(System.nanoTime() - init, TimeUnit.NANOSECONDS);

                return lastInsertId;
            }
        });
    }

    // It is a good idea to monitor reapEntries in logs as these entries should rarely happen
    public void reapEntries(final Date reapingDate) {
        executeTransaction(new Transaction<Void, QueueSqlDao<T>>() {
            @Override
            public Void inTransaction(final QueueSqlDao<T> transactional, final TransactionStatus status) throws Exception {
                final DateTime now = clock.getUTCNow();
                final String owner = CreatorName.get();
                final List<T> entriesLeftBehind = transactional.getEntriesLeftBehind(config.getMaxReDispatchCount(), now.toDate(), reapingDate, config.getTableName());

                if (entriesLeftBehind.isEmpty()) {
                    return null;
                }

                final Collection<T> entriesToMove = new ArrayList<T>(entriesLeftBehind.size());
                final List<T> entriesToReInsert = new ArrayList<T>(entriesLeftBehind.size());
                final List<T> stuckEntries = new LinkedList<T>();
                final List<T> lateEntries = new LinkedList<T>();
                for (final T entryLeftBehind : entriesLeftBehind) {
                    // entryIsBeingProcessedByThisNode is a sign of a stuck entry on this node
                    final boolean entryIsBeingProcessedByThisNode = owner.equals(entryLeftBehind.getProcessingOwner());
                    // entryCreatedByThisNodeAndNeverProcessed is likely a sign of the queue being late
                    final boolean entryCreatedByThisNodeAndNeverProcessed = owner.equals(entryLeftBehind.getCreatingOwner()) && entryLeftBehind.getProcessingOwner() == null;
                    if (entryIsBeingProcessedByThisNode) {
                        // See https://github.com/killbill/killbill-commons/issues/47
                        stuckEntries.add(entryLeftBehind);
                    } else if (entryCreatedByThisNodeAndNeverProcessed) {
                        lateEntries.addAll(entriesLeftBehind);
                    } else {
                        // Fields will be reset appropriately in insertReapedEntriesFromTransaction
                        entriesToReInsert.add(entryLeftBehind);

                        // Set the status to REAPED in the history table
                        entryLeftBehind.setProcessingState(PersistentQueueEntryLifecycleState.REAPED);
                        entriesToMove.add(entryLeftBehind);
                    }
                }

                if (!stuckEntries.isEmpty()) {
                    log.warn("{} reapEntries: stuck queue entries {}", DB_QUEUE_LOG_ID, stuckEntries);
                }
                if (!lateEntries.isEmpty()) {
                    log.warn("{} reapEntries: late queue entries {}", DB_QUEUE_LOG_ID, lateEntries);
                }

                if (!entriesToReInsert.isEmpty()) {
                    moveEntriesToHistoryFromTransaction(transactional, entriesToMove);
                    insertReapedEntriesFromTransaction(transactional, entriesToReInsert, now);
                    log.warn("{} reapEntries: {} entries were reaped by {} {}",
                             DB_QUEUE_LOG_ID, entriesToReInsert.size(), owner, Iterables.<T, UUID>transform(entriesToReInsert,
                                                                                                            new Function<T, UUID>() {
                                                                                                                @Override
                                                                                                                public UUID apply(final T input) {
                                                                                                                    return input == null ? null : input.getUserToken();
                                                                                                                }
                                                                                                            }));
                }

                return null;
            }
        });
    }

    protected <U> U executeQuery(final Query<U, QueueSqlDao<T>> query) {
        return dbi.withHandle(new HandleCallback<U>() {
            @Override
            public U withHandle(final Handle handle) throws Exception {
                final U result = query.execute(handle.attach(sqlDaoClass));
                printSQLWarnings(handle);
                return result;
            }
        });
    }

    protected <U> U executeTransaction(final Transaction<U, QueueSqlDao<T>> transaction) {
        return dbi.inTransaction(new TransactionCallback<U>() {
            @Override
            public U inTransaction(final Handle handle, final TransactionStatus status) throws Exception {
                final U result = transaction.inTransaction(handle.attach(sqlDaoClass), status);
                printSQLWarnings(handle);
                return result;
            }
        });
    }

    protected void printSQLWarnings(final Handle handle) {
        try {
            SQLWarning warning = handle.getConnection().getWarnings();
            while (warning != null) {
                log.debug("[SQL WARNING] {}", warning);
                warning = warning.getNextWarning();
            }
            handle.getConnection().clearWarnings();
        } catch (final SQLException e) {
            log.debug("Error whilst retrieving SQL warnings", e);
        }
    }

    protected interface Query<U, QueueSqlDao> {

        U execute(QueueSqlDao sqlDao);
    }

    public QueueSqlDao<T> getSqlDao() {
        return sqlDao;
    }

}
