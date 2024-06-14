<b>Oracle Data Type Mapping MYSQL/TiDB Rule</b>

| ORACLE                            | MYSQL/TiDB                |
|-----------------------------------|---------------------------|
| bfile                             | varchar(255)              |
| char(length)                      | varchar(length)           |
| character(length)                 | varchar(length)           |
| clob                              | longtext                  |
| blob                              | blob                      |
| date                              | datetime                  |
| decimal(p,s)                      | decimal(p,s)              |
| dec(p,s)                          | decimal(p,s)              |
| double precision                  | double precision          |
| float(p)                          | double                    |
| integer                           | int                       |
| int                               | int                       |
| long                              | longtext                  |
| long raw                          | longblob                  |
| binary_float                      | double                    |
| binary_double                     | double                    |
| nchar(length)                     | nchar(length)             |
| nchar varying(length)             | nchar varying(length)     |
| nclob                             | longtext                  |
| numeric(p,s)                      | numeric(p,s)              |
| nvarchar2(p)                      | varchar(p)                |
| raw(length)                       | varbinary(length)         |
| real                              | double                    |
| rowid                             | varchar(64)               |
| smallint                          | smallint                  |
| urowid(length)                    | varchar(length)           |
| varchar2(length)                  | varchar(length)           |
| varchar(length)                   | varchar(length)           |
| xmltype                           | longtext                  |
| interval year(p) to month         | varchar(30)               |
| interval day(p) to second(s)      | varchar(30)               |
| timestamp(p)                      | timestamp(p)【精度支持 6位】     |
| timestamp(p) with time zone       | datetime(p)【精度支持 6位】      |
| timestamp(p) with local time zone | datetime(p)【精度支持 6位】      |
| other data type                   | text                      |



**Note: MYSQL/TiDB regarding the difference between NUMBER data types**

| ORACLE                         | MYSQL          | TiDB           |
|--------------------------------|----------------|----------------|
| number                         | decimal(65,30) | decimal(65,30) |
| number(*)                      | decimal(65,30) | decimal(65,30) |
| number(*,s)<br />0<s<=30       | decimal(65,s)  | decimal(65,s)  |
| number(*,s)<br />s>30          | decimal(65,30) | decimal(65,30) |
| number(p,s)<br />p>0,p>s       | decimal(p,s)   | decimal(p,s)   |
| number(p,0)<br />1<=p<3        | tinyint        | decimal(p,0)   |
| number(p,0)<br />3<=p<5        | smallint       | decimal(p,0)   |
| number(p,0)<br />5<=p<9        | int            | decimal(p,0)   |
| number(p,0)<br />9<=p<19       | bigint         | decimal(p,0)   |
| number(p,0)<br />19<=p<=38     | decimal(p,0)   | decimal(p,0)   |
| number(p,0)<br />p>38          | decimal(65,0)  | decimal(65,0)  |
| number(p,s)<br />p>0,p<s,s>=30 | decimal(65,30) | decimal(65,30) |
| number(p,s)<br />p>0,p<s,s<30  | decimal(65,s)  | decimal(65,s)  |
