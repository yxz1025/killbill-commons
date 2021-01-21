/*
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

package org.killbill;

import java.sql.SQLException;
import java.util.Properties;
import java.util.UUID;

import javax.sql.DataSource;

import org.killbill.bus.DefaultPersistentBus;
import org.killbill.bus.api.BusEvent;
import org.killbill.bus.api.PersistentBus.EventBusException;

import com.alibaba.druid.pool.DruidDataSource;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;

/**
 * @author xiaozhong
 * @version 1.0
 * @date 2020-09-08 14:06
 */
public class App {

    private DefaultPersistentBus bus;
    private DataSource dataSource;


    public void init() throws SQLException {
        DruidDataSource druidDataSource =  druidDataSource();
        druidDataSource.init();
        dataSource = druidDataSource;
        final Properties properties = new Properties();
        properties.setProperty("org.killbill.persistent.bus.main.inMemory", "false");
        properties.setProperty("org.killbill.persistent.bus.main.queue.mode", "STICKY_POLLING");
        properties.setProperty("org.killbill.persistent.bus.main.max.failure.retry", "3");
        properties.setProperty("org.killbill.persistent.bus.main.claimed", "1000");
        properties.setProperty("org.killbill.persistent.bus.main.claim.time", "5m");
        properties.setProperty("org.killbill.persistent.bus.main.sleep", "1000");
        properties.setProperty("org.killbill.persistent.bus.main.off", "false");
        properties.setProperty("org.killbill.persistent.bus.main.nbThreads", "1");
        properties.setProperty("org.killbill.persistent.bus.main.queue.capacity", "3000");
        properties.setProperty("org.killbill.persistent.bus.main.tableName", "bus_events");
        properties.setProperty("org.killbill.persistent.bus.main.historyTableName", "bus_events_history");
        bus = new DefaultPersistentBus(dataSource, properties);
        this.start();
    }

    public void execute() throws EventBusException, SQLException {

        // Create a Handler (with @Subscribe method)
        final DummyHandler handler = new DummyHandler();
        bus.register(handler);

        // Extract connection from dataSource
//        final Connection connection = dataSource.getConnection();
//        final DummyEvent event = new DummyEvent("foo", 1L, 2L, UUID.randomUUID());
//
//        PreparedStatement stmt = null;
//        try {
//            // In one transaction we both insert a dummy value in some table, and post the event (using same connection/transaction)
//            connection.setAutoCommit(false);
//            stmt = connection.prepareStatement("insert into dummy (dkey, dvalue) values (?, ?)");
//            stmt.setString(1, "Great!");
//            stmt.setLong(2, 47L);
//            stmt.executeUpdate();
//            bus.postFromTransaction(event, connection);
//            connection.commit();
//        } finally {
//            if (stmt != null) {
//                stmt.close();
//            }
//            if (connection != null) {
//                connection.close();
//            }
//        }
    }

    public void start(){
        this.bus.startQueue();
    }

    public void stop(){
        this.bus.stopQueue();
    }

    private DruidDataSource druidDataSource() throws SQLException {
        DruidDataSource druidDataSource = new DruidDataSource();
        druidDataSource.setUrl("jdbc:mysql://172.16.150.99:3306/55170");
        druidDataSource.setUsername("root");
        druidDataSource.setPassword("lefit@sit");
        druidDataSource.setInitialSize(5);
        druidDataSource.setMaxActive(5);
        druidDataSource.setMinIdle(5);
        druidDataSource.setDriverClassName("com.mysql.jdbc.Driver");
        return druidDataSource;
    }

    public static class DummyEvent implements BusEvent {

        private final String name;
        private final Long searchKey1;
        private final Long searchKey2;
        private final UUID userToken;

        @JsonCreator
        public DummyEvent(@JsonProperty("name") final String name,
                          @JsonProperty("searchKey1") final Long searchKey1,
                          @JsonProperty("searchKey2") final Long searchKey2,
                          @JsonProperty("userToken") final UUID userToken) {
            this.name = name;
            this.searchKey2 = searchKey2;
            this.searchKey1 = searchKey1;
            this.userToken = userToken;
        }

        public String getName() {
            return name;
        }

        @Override
        public Long getSearchKey1() {
            return searchKey1;
        }

        @Override
        public Long getSearchKey2() {
            return searchKey2;
        }

        @Override
        public UUID getUserToken() {
            return userToken;
        }
    }

    public static class MyEventHandlerException extends RuntimeException {

        private static final long serialVersionUID = 156337823L;

        public MyEventHandlerException(final String msg) {
            super(msg);
        }
    }

    public static class DummyHandler {

        private int nbEvents;

        public DummyHandler() {
            nbEvents = 0;
        }

        @AllowConcurrentEvents
        @Subscribe
        public void processEvent(final DummyEvent event) {
            System.out.println("YEAH!!!!! event = " + event);
            nbEvents++;
//            throw new MyEventHandlerException("FAIL");
        }

        public synchronized boolean waitForCompletion(final int expectedEvents, final long timeoutMs) {

            final long ini = System.currentTimeMillis();
            long remaining = timeoutMs;
            while (nbEvents < expectedEvents && remaining > 0) {
                try {
                    wait(1000);
                    if (nbEvents == expectedEvents) {
                        break;
                    }
                    remaining = timeoutMs - (System.currentTimeMillis() - ini);
                } catch (final InterruptedException ignore) {
                }
            }
            return (nbEvents == expectedEvents);
        }

    }

    public static void main(String[] args) {
        App app = new App();
        try {
            app.init();
            try {
                app.execute();
            } catch (EventBusException e) {
                System.out.println("执行异常 e = " + e.getMessage());
            }
        } catch (SQLException e) {
            System.out.println("初始化链接异常 e = " + e.getMessage());
        }

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                app.stop();
            }
        }));
    }
}
