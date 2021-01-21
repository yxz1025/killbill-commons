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

/*! SET default_storage_engine=INNODB */;

DROP TABLE IF EXISTS notifications;
CREATE TABLE notifications (
    record_id serial unique,
    class_name varchar(256) NOT NULL,
    event_json varchar(2048) NOT NULL,
    user_token varchar(36),
    created_date datetime NOT NULL,
    creating_owner varchar(50) NOT NULL,
    processing_owner varchar(50) DEFAULT NULL,
    processing_available_date datetime DEFAULT NULL,
    processing_state varchar(14) DEFAULT 'AVAILABLE',
    error_count int /*! unsigned */ DEFAULT 0,
    search_key1 bigint /*! unsigned */ default null,
    search_key2 bigint /*! unsigned */ default null,
    queue_name varchar(64) NOT NULL,
    effective_date datetime NOT NULL,
    future_user_token varchar(36),
    PRIMARY KEY(record_id)
) /*! CHARACTER SET utf8 COLLATE utf8_bin */;
CREATE INDEX idx_comp_where ON notifications(effective_date, processing_state, processing_owner, processing_available_date);
CREATE INDEX idx_update ON notifications(processing_state, processing_owner, processing_available_date);
CREATE INDEX idx_get_ready ON notifications(effective_date, created_date);
CREATE INDEX notifications_search_keys ON notifications(search_key2, search_key1);

DROP TABLE IF EXISTS notifications_history;
CREATE TABLE notifications_history (
    record_id serial unique,
    class_name varchar(256) NOT NULL,
    event_json varchar(2048) NOT NULL,
    user_token varchar(36),
    created_date datetime NOT NULL,
    creating_owner varchar(50) NOT NULL,
    processing_owner varchar(50) DEFAULT NULL,
    processing_available_date datetime DEFAULT NULL,
    processing_state varchar(14) DEFAULT 'AVAILABLE',
    error_count int /*! unsigned */ DEFAULT 0,
    search_key1 bigint /*! unsigned */ default null,
    search_key2 bigint /*! unsigned */ default null,
    queue_name varchar(64) NOT NULL,
    effective_date datetime NOT NULL,
    future_user_token varchar(36),
    PRIMARY KEY(record_id)
) /*! CHARACTER SET utf8 COLLATE utf8_bin */;
CREATE INDEX notifications_history_search_keys ON notifications_history(search_key2, search_key1);

-- 递增序列
Create Sequence BUS_EVENTS_Sequence
  Increment by 1     -- 每次加几个
  start with 1       -- 从1开始计数
  nomaxvalue         -- 不设置最大值,设置最大值：maxvalue 9999
  nocycle            -- 一直累加，不循环
  cache 10;

-- 创建触发器绑定
Create trigger BUS_EVENTS_Sequence before
  insert on BUS_EVENTS for each row /*对每一行都检测是否触发*/
begin
  select BUS_EVENTS_Sequence.nextval into:New.RECORD_ID from dual;
end;


create table BUS_EVENTS
(
	RECORD_ID NUMBER(20) default 0 not null,
	CLASS_NAME VARCHAR2(128) not null,
	EVENT_JSON VARCHAR2(2048) not null,
	USER_TOKEN VARCHAR2(36),
	CREATED_DATE DATE not null,
	CREATING_OWNER VARCHAR2(50) not null,
	PROCESSING_OWNER VARCHAR2(50),
	PROCESSING_AVAILABLE_DATE DATE,
	ERROR_COUNT NUMBER(10) default 0,
	PROCESSING_STATE VARCHAR2(14) default 'AVAILABLE',
	SEARCH_KEY1 NUMBER(10),
	SEARCH_KEY2 NUMBER(20)
)
/

comment on table BUS_EVENTS is '事件表'
/

comment on column BUS_EVENTS.RECORD_ID is '记录ID'
/

comment on column BUS_EVENTS.CLASS_NAME is '类名'
/

comment on column BUS_EVENTS.EVENT_JSON is '事件信息json'
/

comment on column BUS_EVENTS.USER_TOKEN is '识别态'
/

comment on column BUS_EVENTS.CREATED_DATE is '创建时间'
/

comment on column BUS_EVENTS.CREATING_OWNER is '创建者'
/

create unique index BUS_EVENTS_RECORD_ID_UINDEX
	on BUS_EVENTS (RECORD_ID)
/

create index IDX_BUS_WHERE
	on BUS_EVENTS (PROCESSING_STATE, PROCESSING_OWNER, PROCESSING_AVAILABLE_DATE)
/

create index IDX_BUS_RECORD
	on BUS_EVENTS (SEARCH_KEY2, SEARCH_KEY1)
/

alter table BUS_EVENTS
	add constraint BUS_EVENTS_PK
		primary key (RECORD_ID)
/

-- 递增序列
Create Sequence BUS_EVENTS_HISTORY_Sequence
	Increment by 1     -- 每次加几个
	start with 1       -- 从1开始计数
	nomaxvalue         -- 不设置最大值,设置最大值：maxvalue 9999
	nocycle            -- 一直累加，不循环
	cache 10;

-- 创建触发器绑定
Create trigger BUS_EVENTS_HISTORY_Sequence before
	insert on BUS_EVENTS_HISTORY for each row /*对每一行都检测是否触发*/
begin
	select BUS_EVENTS_HISTORY_Sequence.nextval into:New.RECORD_ID from dual;
end;

create table BUS_EVENTS_HISTORY
(
	RECORD_ID NUMBER(20) default '0' not null,
	CLASS_NAME VARCHAR2(128) not null,
	EVENT_JSON VARCHAR2(2048) not null,
	USER_TOKEN VARCHAR2(36),
	CREATED_DATE DATE not null,
	CREATING_OWNER VARCHAR2(50) not null,
	PROCESSING_OWNER VARCHAR2(50) default NULL,
	PROCESSING_AVAILABLE_DATE DATE default NULL,
	PROCESSING_STATE VARCHAR2(14) default 'AVAILABLE',
	ERROR_COUNT NUMBER default 0,
	SEARCH_KEY1 NUMBER default null,
	SEARCH_KEY2 NUMBER default null
)
/

create unique index BUS_EVENTS_HISTORY_UINDEX
	on BUS_EVENTS_HISTORY (RECORD_ID)
/

create index IDX_BUS_HISTORY_RECORD
	on BUS_EVENTS_HISTORY (SEARCH_KEY2, SEARCH_KEY1)
/

alter table BUS_EVENTS_HISTORY
	add constraint BUS_EVENTS_HISTORY_PK
		primary key (RECORD_ID)
/


