create table c_int (
    id int auto_increment,
    c_tinyint tinyint null,
    c_smallint smallint null,
    c_mediumint mediumint null,
    c_int int null,
    c_bigint bigint null,
    constraint pk primary key (id)
);

create table c_unsigned_int (
    id int auto_increment,
    c_unsigned_tinyint tinyint unsigned null,
    c_unsigned_smallint smallint unsigned null,
    c_unsigned_mediumint mediumint unsigned null,
    c_unsigned_int int unsigned null,
    c_unsigned_bigint bigint unsigned null,
    constraint pk primary key (id)
);

create table c_text (
    id int auto_increment,
    c_tinytext tinytext null,
    c_text text null,
    c_mediumtext mediumtext null,
    c_longtext longtext null,
    constraint pk primary key (id)
);

create table c_char_binary (
    id int auto_increment,
    c_char char(16) null,
    c_varchar varchar(16) null,
    c_binary binary(16) null,
    c_varbinary varbinary(16) null,
    constraint pk primary key (id)
);

create table c_blob (
    id int auto_increment,
    c_tinyblob tinyblob null,
    c_blob blob null,
    c_mediumblob mediumblob null,
    c_longblob longblob null,
    constraint pk primary key (id)
);

create table c_time (
    id int auto_increment,
    c_date date null,
    c_datetime datetime null,
    c_timestamp timestamp null,
    c_time time null,
    c_year year null,
    constraint pk primary key (id)
);

create table c_real (
    id int auto_increment,
    c_float float null,
    c_double double null,
    c_decimal decimal null,
    c_decimal_2 decimal(10,4) null,
    constraint pk primary key (id)
);

create table c_unsigned_real (
    id int auto_increment,
    c_unsigned_float float unsigned null,
    c_unsigned_double double unsigned null,
    c_unsigned_decimal decimal unsigned null,
    c_unsigned_decimal_2 decimal(10,4) unsigned null,
    constraint pk primary key (id)
);

create table c_other_datatype (
    id int auto_increment,
    c_enum enum('a','b','c') null,
    c_set  set('a','b','c') null,
    c_bit bit(64) null,
    c_json json null,
    constraint pk primary key (id)
);

CREATE TABLE c_update00 (
	id int primary key,
	val varchar(16)
);

CREATE TABLE c_update01 (
	a INT PRIMARY KEY,
	b INT
);

CREATE TABLE c_update02 (
	a INT PRIMARY KEY,
	b INT
);

create table c_partition_hash (
	a int,
	primary key (a)
) partition by hash(a) partitions 5;

create table c_partition_range (
	a int primary key
) PARTITION BY RANGE (a) (
    PARTITION p0 VALUES LESS THAN (6),
    PARTITION p1 VALUES LESS THAN (11),
    PARTITION p2 VALUES LESS THAN (21)
);

create table c_clustered_t0 (
	a int primary key,
	b int
);

create table c_clustered_t1 (
	a int primary key,
	b int unique key
);

create table c_clustered_t2(
	a char(10) primary key,
	b int
);

create table c_clustered_t3 (
	a int unique key not null
);

create table c_nonclustered_t0 (
    a int primary key /*T![clustered_index] NONCLUSTERED */,
    b int
);

create table c_nonclustered_t1 (
    a int primary key /*T![clustered_index] NONCLUSTERED */,
    b int unique key
);

CREATE TABLE c_nonclustered_t2 (
	a VARCHAR(255) PRIMARY KEY NONCLUSTERED
);

create table c_nonclustered_t3 (
	a int unique key not null
);

CREATE TABLE c_store_gen_col (
    col1 INT NOT NULL,
    col2 VARCHAR(255) NOT NULL,
    col3 INT GENERATED ALWAYS AS (col1 * 2) STORED NOT NULL,
    CONSTRAINT UK UNIQUE (col1,col2,col3)
);

-- ticdc 不支持同步虚拟生成列
CREATE TABLE c_vitual_gen_col (
    col1 INT NOT NULL,
    col2 VARCHAR(255) NOT NULL,
    col3 INT GENERATED ALWAYS AS (col1 * 2) virtual NOT NULL,
    CONSTRAINT UK UNIQUE (col1,col2,col3)
);

create table c_compression_t (
    id int primary key auto_increment,
    c_tinyint tinyint null,
    c_smallint smallint null,
    c_mediumint mediumint null,
    c_int int null,
    c_bigint bigint null,
    c_unsigned_tinyint tinyint unsigned null,
    c_unsigned_smallint smallint unsigned null,
    c_unsigned_mediumint mediumint unsigned null,
    c_unsigned_int int unsigned null,
    c_unsigned_bigint bigint unsigned null,
    c_float float null,
    c_double double null,
    c_decimal decimal null,
    c_decimal_2 decimal(10,4) null,
    c_unsigned_float float unsigned null,
    c_unsigned_double double unsigned null,
    c_unsigned_decimal decimal unsigned null,
    c_unsigned_decimal_2 decimal(10,4) unsigned null,
    c_date date null,
    c_datetime datetime null,
    c_timestamp timestamp null,
    c_time time null,
    c_year year null,
    c_tinytext tinytext null,
    c_text text null,
    c_mediumtext mediumtext null,
    c_longtext longtext null,
    c_tinyblob tinyblob null,
    c_blob blob null,
    c_mediumblob mediumblob null,
    c_longblob longblob null,
    c_char char(16) null,
    c_varchar varchar(16) null,
    c_binary binary(16) null,
    c_varbinary varbinary(16) null,
    c_bit bit(64) null,
    c_json json null
);

create table c_gen_store_t (
	a int,
	b int as (a + 1) stored primary key
);

-- ticdc 不支持同步虚拟生成列
create table c_gen_virtual_t (
    a int,
    b int as (a + 1) virtual not null,
    c int not null,
    unique index idx1(b),
    unique index idx2(c)
);

CREATE TABLE c_random (
    id BIGINT AUTO_RANDOM,
    data int,
    PRIMARY KEY(id) clustered
);

create table c_savepoint_t(
	id int PRIMARY KEY,
	a int,
	index idx(a)
);

CREATE TABLE c_multi_data_type (
    id INT AUTO_INCREMENT,
    t_boolean BOOLEAN,
    t_bigint BIGINT,
    t_double DOUBLE,
    t_decimal DECIMAL(38,19),
    t_bit BIT(64),
    t_date DATE,
    t_datetime DATETIME,
    t_timestamp TIMESTAMP NULL,
    t_time TIME,
    t_year YEAR,
    t_char CHAR,
    t_varchar VARCHAR(10),
    t_blob BLOB,
    t_text TEXT,
    t_json JSON,
    PRIMARY KEY (id)
);