insert into c_int()
values ();

insert into c_int(c_tinyint, c_smallint, c_mediumint, c_int, c_bigint)
values (1, 2, 3, 4, 5);

-- insert max value
insert into c_int(c_tinyint, c_smallint, c_mediumint, c_int, c_bigint)
values (127, 32767, 8388607, 2147483647, 9223372036854775807);

-- insert min value
insert into c_int(c_tinyint, c_smallint, c_mediumint, c_int, c_bigint)
values (-128, -32768, -8388608, -2147483648, -9223372036854775808);

update c_int set c_int = 0, c_tinyint = 0 where c_smallint = 2;
delete from c_int where c_int = 0;


insert into c_unsigned_int()
values ();

insert into c_unsigned_int(c_unsigned_tinyint, c_unsigned_smallint, c_unsigned_mediumint,
                            c_unsigned_int, c_unsigned_bigint)
values (1, 2, 3, 4, 5);

-- insert max value
insert into c_unsigned_int(c_unsigned_tinyint, c_unsigned_smallint, c_unsigned_mediumint,
                            c_unsigned_int, c_unsigned_bigint)
values (255, 65535, 16777215, 4294967295, 18446744073709551615);

-- insert signed max value
insert into c_unsigned_int(c_unsigned_tinyint, c_unsigned_smallint, c_unsigned_mediumint,
                            c_unsigned_int, c_unsigned_bigint)
values (127, 32767, 8388607, 2147483647, 9223372036854775807);

insert into c_unsigned_int(c_unsigned_tinyint, c_unsigned_smallint, c_unsigned_mediumint,
                            c_unsigned_int, c_unsigned_bigint)
values (128, 32768, 8388608, 2147483648, 9223372036854775808);

update c_unsigned_int set c_unsigned_int = 0, c_unsigned_tinyint = 0 where c_unsigned_smallint = 65535;
delete from c_unsigned_int where c_unsigned_int = 0;


insert into c_text()
values ();

insert into c_text(c_tinytext, c_text, c_mediumtext, c_longtext)
values ('89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A'),
       ('', '', '', '');


insert into c_char_binary()
values ();

insert into c_char_binary(c_char, c_varchar, c_binary, c_varbinary)
values ('89504E470D0A1A0A', '89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A'),
       ('', '', x'89504E470D0A1A0A', x'89504E470D0A1A0A'),
       ('89504E470D0A1A0A', '89504E470D0A1A0A', x'', x'');


insert into c_blob()
values ();

insert into c_blob(c_tinyblob, c_blob, c_mediumblob, c_longblob)
values (x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A'),
       (x'', x'', x'', x'');


insert into c_time()
values ();

insert into c_time(c_date, c_datetime, c_timestamp, c_time, c_year)
values ('2020-02-20', '2020-02-20 02:20:20', '2020-02-20 02:20:20', '02:20:20', '2020');



insert into c_real()
values ();

insert into c_real(c_float, c_double, c_decimal, c_decimal_2)
values (2020.0202, 2020.0303, 2020.0404, 2021.1208);

insert into c_real(c_float, c_double, c_decimal, c_decimal_2)
values (-2.7182818284, -3.1415926, -8000, -179394.233);



insert into c_unsigned_real()
values ();

insert into c_unsigned_real(c_unsigned_float, c_unsigned_double, c_unsigned_decimal, c_unsigned_decimal_2)
values (2020.0202, 2020.0303, 2020.0404, 2021.1208);



insert into c_other_datatype()
values ();

insert into c_other_datatype(c_enum, c_set, c_bit, c_json)
values ('a', 'a,b', b'1000001', '{
  "key1": "value1",
  "key2": "value2"
}');



BEGIN;
INSERT INTO c_update00(id,val) VALUES(1,'aa');
INSERT INTO c_update00(id,val) VALUES(2,'aa');
UPDATE c_update00 SET val = 'bb' WHERE id=2;
INSERT INTO c_update00(id,val) VALUES(3,'cc');
COMMIT;

BEGIN;
DELETE FROM c_update00 WHERE id = 1;
UPDATE c_update00 SET val = 'dd' WHERE id = 3;
UPDATE c_update00 SET id = 4, val = 'ee' WHERE id = 2;
COMMIT;


INSERT INTO c_update01 VALUES (1, 1);
UPDATE c_update01 SET a = 2 WHERE a = 1;


INSERT INTO c_update02 VALUES (1, 1);
INSERT INTO c_update02 VALUES (2, 2);

BEGIN;
UPDATE c_update02 SET a = 3 WHERE a = 1;
UPDATE c_update02 SET a = 1 WHERE a = 2;
UPDATE c_update02 SET a = 2 WHERE a = 3;
COMMIT;


insert into c_partition_hash values (1),(2),(3),(4),(5),(6);
update c_partition_hash set a=a+10 where a=2;


insert into c_partition_range values (1),(2),(3),(4),(5),(6);
insert into c_partition_range values (7),(8),(9);
update c_partition_range set a=a+10 where a=9;


insert into c_clustered_t0 values (3, 5);
update c_clustered_t0 set a = 1 where a = 3;


insert into c_clustered_t1 values(1, 2);
update c_clustered_t1 set b = 5 where b = 2;


insert into c_clustered_t2 values('test', 1);
update c_clustered_t2 set a = 'test-2' where a = 'test';


insert into c_clustered_t3 values (7);
update c_clustered_t3 set a = 11 where a = 7;


insert into c_nonclustered_t0 values(2, 4);
update c_nonclustered_t0 set a = 5 where a = 2;

insert into c_nonclustered_t1 values (3, 4);
update c_nonclustered_t1 set b = 7 where b = 4;

insert into c_nonclustered_t2 values ('3');
update c_nonclustered_t2 set a='5' where a='3';

insert into c_nonclustered_t3 values(3);
update c_nonclustered_t3 set a = 5 where a = 3;


insert into c_store_gen_col (col1,col2) values (1,'test');
update c_store_gen_col set col1 = 2 where col2 = 'test';

insert into c_vitual_gen_col (col1,col2) values (1,'test');
update c_vitual_gen_col set col1 = 2 where col2 = 'test';


insert into c_compression_t values (
    1,
    1, 2, 3, 4, 5,
    1, 2, 3, 4, 5,
    2020.0202, 2020.0303, 2020.0404, 2021.1208,
    3.1415, 2.7182, 8000, 179394.233,
    '2020-02-20', '2020-02-20 02:20:20', '2020-02-20 02:20:20', '02:20:20', '2020',
    '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A',
    x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A',
    '89504E470D0A1A0A', '89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A',
    b'1000001', '{
        "key1": "value1",
        "key2": "value2",
        "key3": "123"
    }'
);

update c_compression_t set c_float = 3.1415, c_double = 2.7182, c_decimal = 8000, c_decimal_2 = 179394.233 where id = 1;

delete from c_compression_t where id = 1;

insert into c_compression_t values (
     2,
     1, 2, 3, 4, 5,
     1, 2, 3, 4, 5,
     2020.0202, 2020.0303, 2020.0404, 2021.1208,
     3.1415, 2.7182, 8000, 179394.233,
     '2020-02-20', '2020-02-20 02:20:20', '2020-02-20 02:20:20', '02:20:20', '2020',
     '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A',
     x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A',
     '89504E470D0A1A0A', '89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A',
     b'1000001', '{
        "key1": "value1",
        "key2": "value2",
        "key3": "123"
    }'
);

update c_compression_t set c_float = 3.1415, c_double = 2.7182, c_decimal = 8000, c_decimal_2 = 179394.233 where id = 2;

BEGIN;
insert into c_compression_t values (
     3,
     1, 2, 3, 4, 5,
     1, 2, 3, 4, 5,
     2020.0202, 2020.0303, 2020.0404, 2021.1208,
     3.1415, 2.7182, 8000, 179394.233,
     '2020-02-20', '2020-02-20 02:20:20', '2020-02-20 02:20:20', '02:20:20', '2020',
     '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A',
     x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A',
     '89504E470D0A1A0A', '89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A',
     b'1000001', '{
        "key1": "value1",
        "key2": "value2",
        "key3": "123"
    }'
 ),(
    4,
    1, 2, 3, 4, 5,
    1, 2, 3, 4, 5,
    2020.0202, 2020.0303, 2020.0404, 2021.1208,
    3.1415, 2.7182, 8000, 179394.233,
    '2020-02-20', '2020-02-20 02:20:20', '2020-02-20 02:20:20', '02:20:20', '2020',
    '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A',
    x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A',
    '89504E470D0A1A0A', '89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A',
    b'1000001', '{
    "key1": "value1",
    "key2": "value2",
    "key3": "123"
  }'
),(
     5,
     1, 2, 3, 4, 5,
     1, 2, 3, 4, 5,
     2020.0202, 2020.0303, 2020.0404, 2021.1208,
     3.1415, 2.7182, 8000, 179394.233,
     '2020-02-20', '2020-02-20 02:20:20', '2020-02-20 02:20:20', '02:20:20', '2020',
     '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A',
     x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A',
     '89504E470D0A1A0A', '89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A',
     b'1000001', '{
        "key1": "value1",
        "key2": "value2",
        "key3": "123"
    }'
 );
COMMIT;



insert into c_gen_store_t(a) values (1),(2), (3),(4),(5),(6),(7);
update c_gen_store_t set a = 10 where a = 1;
update c_gen_store_t set a = 11 where b = 3;
delete from c_gen_store_t where b=4;
delete from c_gen_store_t where a=4;



insert into c_gen_virtual_t (a, c) values (1, 2),(2, 3), (3, 4),(4, 5),(5, 6),(6, 7),(7, 8);
update c_gen_virtual_t set a = 10 where a = 1;
update c_gen_virtual_t set a = 11 where b = 3;
delete from c_gen_virtual_t where b=4;
delete from c_gen_virtual_t where a=4;



INSERT INTO c_random (data) value (1);
INSERT INTO c_random (data) value (2);
INSERT INTO c_random (data) value (3);
INSERT INTO c_random (data) value (4);
INSERT INTO c_random (data) value (5);


-- savepoint 乐观
-- Check savepoint with same name will overwrite the old
begin optimistic;
savepoint s1;
insert into c_savepoint_t values (1, 1);
savepoint s1;
insert into c_savepoint_t values (2, 2);
rollback to s1;
commit;
select * from c_savepoint_t order by id;

--  Check rollback to savepoint will delete the later savepoint.
delete from c_savepoint_t;
begin optimistic;
insert into c_savepoint_t values (1, 1);
savepoint s1;
insert into c_savepoint_t values (2, 2);
savepoint s2;
insert into c_savepoint_t values (3, 3);
savepoint s3;
rollback to s2;
commit;
select * from c_savepoint_t order by id;

--  Check rollback to savepoint will rollback insert.
delete from c_savepoint_t;
begin optimistic;
insert into c_savepoint_t values (1, 1), (2, 2);
savepoint s1;
insert into c_savepoint_t values (3, 3);
rollback to s1;
insert into c_savepoint_t values (3, 5);
commit;
select * from c_savepoint_t order by id;

--  Check rollback to savepoint will rollback insert onduplicate update.
delete from c_savepoint_t;
insert into c_savepoint_t values (1, 1);
begin optimistic;
insert into c_savepoint_t values (2, 2);
savepoint s1;
insert into c_savepoint_t values (1, 1), (2, 2), (3, 3) on duplicate key update a=a+1;
rollback to s1;
commit;
insert into c_savepoint_t values (3, 3);
select * from c_savepoint_t order by id;

--  Check rollback to savepoint will rollback update.
delete from c_savepoint_t;
begin optimistic;
insert into c_savepoint_t values (1, 1), (2, 2);
savepoint s1;
update c_savepoint_t set a=a+1 where id = 1;
rollback to s1;
update c_savepoint_t set a=a+1 where id = 2;
commit;
update c_savepoint_t set a=a+1 where id in (1, 2);
select * from c_savepoint_t order by id;

--  Check rollback to savepoint will rollback update.
delete from c_savepoint_t;
insert into c_savepoint_t values (1, 1), (2, 2);
begin optimistic;
insert into c_savepoint_t values (3, 3);
update c_savepoint_t set a=a+1 where id in (1, 3);
savepoint s1;
update c_savepoint_t set a=a+1 where id in (2, 3);
rollback to s1;
commit;
select * from c_savepoint_t order by id;

--  Check rollback to savepoint will rollback delete.
delete from c_savepoint_t;
insert into c_savepoint_t values (1, 1), (2, 2);
begin optimistic;
insert into c_savepoint_t values (3, 3);
savepoint s1;
delete from c_savepoint_t where id in (1, 3);
rollback to s1;
commit;
select * from c_savepoint_t order by id;


-- savepoint 悲观
delete from c_savepoint_t;
begin pessimistic;
savepoint s1;
insert into c_savepoint_t values (1, 1);
savepoint s1;
insert into c_savepoint_t values (2, 2);
rollback to s1;
commit;
select * from c_savepoint_t order by id;


--  Check rollback to savepoint will delete the later savepoint.
delete from c_savepoint_t;
begin pessimistic;
insert into c_savepoint_t values (1, 1);
savepoint s1;
insert into c_savepoint_t values (2, 2);
savepoint s2;
insert into c_savepoint_t values (3, 3);
savepoint s3;
rollback to s2;
commit;
select * from c_savepoint_t order by id;

--  Check rollback to savepoint will rollback insert.
delete from c_savepoint_t;
begin pessimistic;
insert into c_savepoint_t values (1, 1), (2, 2);
savepoint s1;
insert into c_savepoint_t values (3, 3);
rollback to s1;
insert into c_savepoint_t values (3, 5);
commit;
select * from c_savepoint_t order by id;

--  Check rollback to savepoint will rollback insert onduplicate update and release lock.
delete from c_savepoint_t;
insert into c_savepoint_t values (1, 1);
begin pessimistic;
insert into c_savepoint_t values (2, 2);
savepoint s1;
insert into c_savepoint_t values (1, 1), (2, 2), (3, 3) on duplicate key update a=a+1;
rollback to s1;
commit;
insert into c_savepoint_t values (3, 3);
select * from c_savepoint_t order by id;

--  Check rollback to savepoint will rollback update.
delete from c_savepoint_t;
begin pessimistic;
insert into c_savepoint_t values (1, 1), (2, 2);
savepoint s1;
update c_savepoint_t set a=a+1 where id = 1;
rollback to s1;
update c_savepoint_t set a=a+1 where id = 2;
commit;
update c_savepoint_t set a=a+1 where id in (1, 2);
select * from c_savepoint_t order by id;

--  Check rollback to savepoint will rollback update.
delete from c_savepoint_t;
insert into c_savepoint_t values (1, 1), (2, 2);
begin pessimistic;
insert into c_savepoint_t values (3, 3);
update c_savepoint_t set a=a+1 where id in (1, 3);
savepoint s1;
update c_savepoint_t set a=a+1 where id in (2, 3);
rollback to s1;
commit;
select * from c_savepoint_t order by id;

--  Check rollback to savepoint will rollback delete.
delete from c_savepoint_t;
insert into c_savepoint_t values (1, 1), (2, 2);
begin pessimistic;
insert into c_savepoint_t values (3, 3);
savepoint s1;
delete from c_savepoint_t where id in (1, 3);
rollback to s1;
commit;
select * from c_savepoint_t order by id;


SET GLOBAL tidb_row_format_version = 1;
-- new session
-- make sure `nullable` can be handled by the mounter and mq encoding protocol
INSERT INTO c_multi_data_type() VALUES ();

INSERT INTO c_multi_data_type( t_boolean, t_bigint, t_double, t_decimal, t_bit
                           , t_date, t_datetime, t_timestamp, t_time, t_year
                           , t_char, t_varchar, t_blob, t_text, t_json)
VALUES ( true, 9223372036854775807, 123.123, 123456789012.123456789012, b'1000001'
       , '1000-01-01', '9999-12-31 23:59:59', '19731230153000', '23:59:59', 1970
       , '测', '测试', 'blob', '测试text', NULL);
select * from c_multi_data_type;


SET GLOBAL tidb_row_format_version = 2;
-- new session
INSERT INTO c_multi_data_type( t_boolean, t_bigint, t_double, t_decimal, t_bit
                           , t_date, t_datetime, t_timestamp, t_time, t_year
                           , t_char, t_varchar, t_blob, t_text, t_json)
VALUES ( false, 666, 123.777, 123456789012.123456789012, b'1000001'
       , '1000-01-01', '9999-12-31 23:59:59', '19731230153000', '23:59:59', 1970
       , '测', '测试', 'blob', '测试text11', NULL);

UPDATE c_multi_data_type
SET t_bigint = 555
WHERE id = 1;

select * from c_multi_data_type;
