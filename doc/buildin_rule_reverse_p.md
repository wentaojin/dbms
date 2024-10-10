POSTGRES COMPATIBLE DATABASE DATA TYPE MAPPING MYSQL COMPATIBLE DATABASE RULE
------

**Note: mysql compatible database unified character set utf8mb4**

| POSTGRESQL               | MYSQL/TiDB       | COMMENT                                     |
|--------------------------|------------------|---------------------------------------------|
| int                      | int              |                                             |
| smallint                 | smallint         |                                             |
| bigint                   | bigint           |                                             |
| serial                   | int              | 1, suggest set auto_increment in its tables definition（mysql）<br> 2, suggest set auto_random in its tables definition (tidb)|
| smallserial              | smallint         | 1, suggest set auto_increment in its tables definition（mysql）<br> 2, suggest set auto_random in its tables definition (tidb)|
| bigserial                | bigint           | 1, suggest set auto_increment in its tables definition（mysql）<br> 2, suggest set auto_random in its tables definition (tidb)|
| bit                      | bit              |                                             |
| boolean                  | tinyint(1)       |                                             |
| real                     | float            |                                             |
| double precision         | double           |                                             |
| numeric(M,D)             | decimal(M,D)     |                                             |
| decimal(M,D)             | decimal(M,D)     |                                             |
| money                    | decimal(19,2)    |                                             |
| char(P)                  | char(P)          | P <= 255（P represents the length of characters）                                  |
| char(P)                  | varchar(P)       | 255 < P <= 16382 (P represents the length of characters） 65535/4 ～= 16382        |
| char(P)                  | longtext         | P > 16382 (P represents the length of characters）                                 |
| national char(P)         | char(P)          | P <= 255（P represents the length of characters）                                  |
| national char(P)         | varchar(P)       | 255 < P <= 16382 (P represents the length of characters） 65535/4 ～= 16382        |
| national char(P)         | longtext         | P > 16382 (P represents the length of characters）                                 |
| varchar(P)               | varchar(P)       | P <= 16382 (P represents the length of characters） 65535/4 ～= 16382              |
| varchar(P)               | mediumtext       | 16382 < P <= 4194303 (P represents the length of characters） 16,777,215/4 ～= 4194303 |
| varchar(P)               | longtext         | P > 4194303 (P represents the length of characters）                                   |
| national character varying(P) | varchar(P)  | P <= 16382 (P represents the length of characters） 65535/4 ～= 16382                  |
| national character varying(P) | mediumtext  | 16382 < P <= 4194303 (P represents the length of characters） 16,777,215/4 ～= 4194303 |
| national character varying(P) | longtext    | P > 4194303 (P represents the length of characters）                                   |
| date                     | date             |                                              |
| time                     | time             |                                              |
| timestamp                | datetime         |                                              |
| interval                 | time             |                                              |
| bytea                    | longblob         |                                              |
| text                     | longtext         |                                              |
| cidr                     | varchar(43)      |                                              |
| inet                     | varchar(43)      |                                              |
| macaddr                  | varchar(17)      |                                              |
| uuid                     | varchar(36)      |                                              |
| xml                      | longtext         |                                              |
| json                     | longtext         |                                              |
| json                     | longtext         |                                              |
| tsvector                 | longtext         |                                              |
| tsquery                  | longtext         |                                              |
| array                    | longtext         |                                              |
| point                    | point            |                                              |
| line                     | linestring       | although LINE length is infinite, and LINESTRING is finite in MySQL, it is approximated. single row max  65,535 bytes |
| lseg                     | linestring       | a LSEG is like a LINESTRING with only two points. |
| box                      | polygon          | a BOX is a POLYGON with five points and right angles. |
| path                     | linestring       |                                              |
| polygon                  | polygon          |                                              |
| circle                   | polygon          | a POLYGON is used to approximate a CIRCLE.   |
| txid_snapshot            | varchar(256)     |                                              |