-- Range partitioning with intervals (monthly intervals)

CREATE TABLE salestable
  (s_productid  NUMBER,
   s_saledate   DATE,
   s_custid     NUMBER,
   s_totalprice NUMBER)
PARTITION BY RANGE(s_saledate)
INTERVAL(NUMTOYMINTERVAL(1,'MONTH')) STORE IN (tbs1,tbs2,tbs3,tbs4)
 (PARTITION sal05q1 VALUES LESS THAN (TO_DATE('01-APR-2005', 'DD-MON-YYYY')) TABLESPACE tbs1,
  PARTITION sal05q2 VALUES LESS THAN (TO_DATE('01-JUL-2005', 'DD-MON-YYYY')) TABLESPACE tbs2,
  PARTITION sal05q3 VALUES LESS THAN (TO_DATE('01-OCT-2005', 'DD-MON-YYYY')) TABLESPACE tbs3,
  PARTITION sal05q4 VALUES LESS THAN (TO_DATE('01-JAN-2006', 'DD-MON-YYYY')) TABLESPACE tbs4,
  PARTITION sal06q1 VALUES LESS THAN (TO_DATE('01-APR-2006', 'DD-MON-YYYY')) TABLESPACE tbs1,
  PARTITION sal06q2 VALUES LESS THAN (TO_DATE('01-JUL-2006', 'DD-MON-YYYY')) TABLESPACE tbs2,
  PARTITION sal06q3 VALUES LESS THAN (TO_DATE('01-OCT-2006', 'DD-MON-YYYY')) TABLESPACE tbs3,
  PARTITION sal06q4 VALUES LESS THAN (TO_DATE('01-JAN-2007', 'DD-MON-YYYY')) TABLESPACE tbs4);


  -- interval partitioning on numbers
  create table vid.zz_HIST
(
  archive_date       DATE,
  iteration_id       NUMBER,
  valdate            DATE,
  co                 VARCHAR2(50),
  cocode             VARCHAR2(50),
  productdescription VARCHAR2(500),
  pc_cont            VARCHAR2(15),
  mva                VARCHAR2(3),
  productcode        VARCHAR2(10),
  resstate           VARCHAR2(2),
  fundcode           VARCHAR2(3),
  fundtype           VARCHAR2(3),
  funddesc           VARCHAR2(50),
  guarperiod         NUMBER,  
  pc_pln_code        VARCHAR2(11),
  valuation_date     DATE,
  load_date          DATE
)
partition by range (ITERATION_ID) interval (1)
( partition P1 values less than (1) tablespace ACT_DATA );

---- Example2-----------
CREATE TABLE vid.COMPANY_DIM
(
  ITERATION_ID  NUMBER                          NOT NULL,
  COMPANY_KEY   NUMBER GENERATED ALWAYS AS IDENTITY NOT NULL,
  COMPANY_CD    VARCHAR2(25 BYTE)               NOT NULL,
  COMPANY_DESC  VARCHAR2(100 BYTE)
)
NOCOMPRESS TABLESPACE MISC_DATA LOGGING
PARTITION BY RANGE (ITERATION_ID) INTERVAL( 1)
( 
  PARTITION P1 VALUES LESS THAN (1) TABLESPACE MISC_DATA,  
  PARTITION VALUES LESS THAN (30)   TABLESPACE MISC_DATA,  
  PARTITION VALUES LESS THAN (31)   TABLESPACE MISC_DATA
)
NOCACHE MONITORING;