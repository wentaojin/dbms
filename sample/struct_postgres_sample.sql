CREATE TABLE IF NOT EXISTS marvin00(
    i00 int not null default 1,
    i01 smallint,
    i02 bigint,
    s00 serial,
    s01 smallserial,
    b00 bit,
    b01 bit(10),
    b02 boolean,
    r00 real,
    dp00 double precision  not null default 9.078,
    n00 numeric,
    n01 numeric(10,0),
    n02 numeric(10,2)  not null default 1.01,
    d00 decimal,
    d01 decimal(8,0),
    d02 decimal(8,2),
    m00 money,
    c00 char,
    c01 char(10)  not null default 'kp',
    c02 char(255),
    c03 char(355),
    nc00 national character(20) not null default 'pl',
    nc01 national character(10),
    nc02 char(255),
    nc03 char(355),
    v00 varchar,
    v01 varchar(10),
    v02 varchar(65535),
    v03 varchar(131070),
    nv00 national character varying,
    nv01 national character varying(10),
    nv02 national character varying(65535),
    nv03 national character varying(131070),
    de00 date,
    t00 time,
    t01 timestamp,
    t02 timestamp(3),
    it00 interval,
    in01 interval(2),
    by00 bytea,
    tx00 text,
    cr00 cidr,
    in00 inet,
    mc00 macaddr,
    ud00 uuid,
    x00 xml,
    js  json,
    vec00 tsvector,
    tq00 tsquery,
    ar00 text[][],
    po00 point,
    li00 line,
    ls00 lseg,
    bx00 box,
    ph path,
    py polygon,
    cl00 circle,
    txd00 txid_snapshot
);

INSERT INTO marvin00 (
    i00, i01, i02, b00, b01, b02, r00, n00, n01, n02, 
    d00, d01, d02, m00, c00, c01, c02, c03, nc00, nc01, 
    nc02, nc03, v00, v01, v02, v03, nv00, nv01, nv02, 
    nv03, de00, t00, t01, t02, it00, in01, by00, tx00, 
    cr00, in00, mc00, ud00, x00, js, vec00, tq00, ar00, 
    po00, li00, ls00, bx00, ph, py, cl00, txd00
) VALUES (
    2, 32767, 9223372036854775807, B'1', B'1010101010', TRUE, 3.14, 123.456, 1234567890, 123.45, 
    123.456, 12345678, 1234.56, 1234.56, 'A', 'example', 'long string here', 'very long string here', 
    'example', 'example', 'long string here', 'very long string here', 'example', 'ex', 'very long string here', 
    'very very long string here', 'example', 'ex', 'very long string here', 'very very long string here', 
    '2024-01-01', '12:00:00', '2024-01-01 12:00:00', '2024-01-01 12:00:00.123', '1 day', '1 hour', 
    E'\\xdeadbeef', 'this is a text field', '192.168.1.0/24', '192.168.1.1', '08:00:27:00:00:01', 
    '123e4567-e89b-12d3-a456-426614174000', '<root><child>value</child></root>', '{"key": "value"}', 
    'the quick brown fox jumps over the lazy dog', 'the & quick', ARRAY[['text1','text2'],['text3','text4']], 
    '(1.23,4.56)', '(1,2,3,4)', '((1,2),(3,4))', '(1.23,4.56,7.89,10.11)', 
    '((1.23,4.56),(7.89,10.11))', '((1,2),(3,4),(5,6))', '<(1.23,4.56),7.89>', 
    '1000:2000:'
);

COMMENT ON COLUMN marvin00.nc00 IS '您好';
ALTER TABLE marvin00 ADD CONSTRAINT c02_consnull CHECK (c02 is not null);
CREATE UNIQUE INDEX UNIQ_COMPLEX ON marvin.marvin00(c00,c01);
CREATE INDEX idx_nv00 on marvin.marvin00 using hash(nv00);


CREATE TABLE IF NOT EXISTS marvin01(
    i00 int not null default 1,
    i01 smallint,
    i02 bigint,
    b00 bit,
    b01 bit(10),
    b02 boolean,
    r00 real,
    dp00 double precision  not null default 9.078,
    n00 numeric,
    n01 numeric(10,0),
    n02 numeric(10,2)  not null default 1.01,
    d00 decimal,
    d01 decimal(8,0),
    d02 decimal(8,2),
    m00 money,
    c00 char,
    c01 char(10)  not null default 'kp',
    c02 char(255),
    c03 char(355),
    nc00 national character(20) not null default 'pl',
    nc01 national character(10),
    nc02 char(255),
    nc03 char(355),
    v00 varchar,
    v01 varchar(10),
    v02 varchar(65535),
    v03 varchar(131070),
    nv00 national character varying,
    nv01 national character varying(10),
    nv02 national character varying(65535),
    nv03 national character varying(131070),
    de00 date,
    t00 time,
    t01 timestamp,
    t02 timestamp(3),
    it00 interval,
    in01 interval(2),
    by00 bytea,
    tx00 text,
    cr00 cidr,
    in00 inet,
    mc00 macaddr,
    ud00 uuid,
    x00 xml,
    js  json,
    vec00 tsvector,
    tq00 tsquery,
    ar00 text[][],
    po00 point,
    li00 line,
    ls00 lseg,
    bx00 box,
    ph path,
    py polygon,
    cl00 circle,
    txd00 txid_snapshot
);

INSERT INTO marvin01 (
    i00, i01, i02, b00, b01, b02, r00, n00, n01, n02, 
    d00, d01, d02, m00, c00, c01, c02, c03, nc00, nc01, 
    nc02, nc03, v00, v01, v02, v03, nv00, nv01, nv02, 
    nv03, de00, t00, t01, t02, it00, in01, by00, tx00, 
    cr00, in00, mc00, ud00, x00, js, vec00, tq00, ar00, 
    po00, li00, ls00, bx00, ph, py, cl00, txd00
) VALUES (
    2, 32767, 9223372036854775807, B'1', B'1010101010', TRUE, 3.14, 123.456, 1234567890, 123.45, 
    123.456, 12345678, 1234.56, 1234.56, 'B', 'example', 'long string here', 'very long string here', 
    'example', 'example', 'long string here', 'very long string here', 'example', 'ex', 'very long string here', 
    'very very long string here', 'example', 'ex', 'very long string here', 'very very long string here', 
    '2024-01-01', '12:00:00', '2024-01-01 12:00:00', '2024-01-01 12:00:00.123', '1 day', '1 hour', 
    E'\\xdeadbeef', 'this is a text field', '192.168.1.0/24', '192.168.1.1', '08:00:27:00:00:01', 
    '123e4567-e89b-12d3-a456-426614174000', '<root><child>value</child></root>', '{"key": "value"}', 
    'the quick brown fox jumps over the lazy dog', 'the & quick', ARRAY[['text1','text2'],['text3','text4']], 
    '(1.23,4.56)', '(1,2,3,4)', '((1,2),(3,4))', '(1.23,4.56,7.89,10.11)', 
    '((1.23,4.56),(7.89,10.11))', '((1,2),(3,4),(5,6))', '<(1.23,4.56),7.89>', 
    '1000:2000:'
);

COMMENT ON COLUMN marvin01.nc00 IS '您好';
ALTER TABLE marvin01 ADD CONSTRAINT c0201_consnull CHECK (c02 is not null);
CREATE UNIQUE INDEX UNIQ_COMPLEX01 ON marvin.marvin01(c00,c01);
CREATE INDEX idx_nv0001 on marvin.marvin01 using hash(nv00);


CREATE TABLE marvin02 (
    id1 BIGINT NOT NULL,
    vd2 VARCHAR(50) NOT NULL,
    unique_key1 VARCHAR(50) NOT NULL,
    unique_key2 VARCHAR(50) NOT NULL,
    normal_index_column VARCHAR(100),
    data_column TEXT,
    PRIMARY KEY (id1, vd2),
    UNIQUE (unique_key1, unique_key2)
);
CREATE INDEX idx_normal ON marvin02 (normal_index_column);

-- 插入 500 万条数据
INSERT INTO marvin02 (id1, vd2, unique_key1, unique_key2, normal_index_column, data_column)
SELECT
    (series % 1000000) AS id1, -- 生成 0 到 999999 之间的整数
    'vd_' || lpad((series % 100000000000000)::text, 20, '0') AS vd2,  -- 生成唯一的字符串
    'unique1_' || lpad((series % 100000000000000)::text, 20, '0') AS unique_key1, -- 生成唯一的字符串
    'unique2_' || lpad(((series / 100000000000000) % 100000000000000)::text, 20, '0') AS unique_key2, -- 生成唯一的字符串
    'index_' || lpad((random() * 100000000000000)::bigint::text, 20, '0') AS normal_index_column, -- 生成索引用的字符串
    md5(random()::text) AS data_column -- 生成随机的 MD5 字符串
FROM
    generate_series(1, 5000000) AS series; -- 生成 500 万个行