# weka-hive
# 首先尝试mysql的连接，掌握mysql的流程
# 进行hive zookeeper的登录认证，这个是连接成功的关键,对原有的hivedriver重写
# LoginUtil.setJaasConf("Client", USER_NAME, USER_KEYTAB_FILE);
# LoginUtil.setZookeeperServerPrincipal("zookeeper.server.principal", "zookeeper/hadoop");    
# LoginUtil.login(USER_NAME, USER_KEYTAB_FILE, KRB5_FILE, CONF);
# 4,5,6步必不可少，如果缺少就会提示zookeeper2 from ....信息缺失
# 本地跑完通过，把myHiveDriver文件编译后的.class文件打包到hive-jdbc.jar中，并把配置文件考到相应的文件下
# 把安全认证文件copy到jar包即完成操作
# 最后进入weka\weka.jarweka.jar\weka\experiment下的DataBaseUtils.props文件夹，修改如下所示：如果不添加数据库数据类型转化不到本地

# Database settings for Hive
#
# author:  MinHaoSun

# JDBC driver (comma-separated list)
jdbcDriver=org.apache.hive.jdbc.MyHiveDriver

# database URL
jdbcURL=jdbc:hive2://172.24.2.48:24002,172.24.2.49:24002,172.24.2.50:24002/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;sasl.qop=auth-conf;auth=KERBEROS;principal=hive/hadoop.hadoop.com@HADOOP.COM;

# specific data types，具体的数据类型修改如下，主要是去掉注释
string, getString() = 0; --> nominal
boolean, getBoolean() = 1; --> nominal
double, getDouble() = 2; --> numeric
byte, getByte() = 3; --> numeric
short, getByte()= 4; --> numeric
int, getInteger() = 5; --> numeric
long, getLong() = 6; --> numeric
float, getFloat() = 7; --> numeric
date, getDate() = 8; --> date
text, getString() = 9; --> string
time, getTime() = 10; --> date
BigDecimal,getBigDecimal()=11; -->nominal

# hive-conversion --类型转换，做如下补充
STRING=0
TINYINT=3
SMALLINT=4
SHORT=5
INTEGER=5
INT=5
INT_UNSIGNED=6
BIGINT=6
LONG=6
REAL=7
NUMERIC=2
DECIMAL=2
FLOAT=2
DOUBLE=2
CHAR=0
TEXT=0
VARCHAR=0
LONGVARCHAR=9
BINARY=0
VARBINARY=0
LONGVARBINARY=9
BIT=1
BLOB=8
DATE=8
TIME=8
DATETIME=8
TIMESTAMP=8
string=0
tinyint=3
smallint=4
short=5
integer=5
int=5
int_unsigned=6
bigint=6
long=6
real=7
numeric=2
decimal=2
float=2
double=2
char=0
text=0
varchar=0
longvarchar=9
binary=0
varbinary=0
longvarbinary=9
bit=1
blob=8
date=8
time=8
datetime=8
timestamp=8

# other options
CREATE_DOUBLE=DOUBLE
CREATE_STRING=TEXT
CREATE_INT=INT
CREATE_DATE=DATETIME
DateFormat=yyyy-MM-dd HH:mm:ss
checkUpperCaseNames=false
checkLowerCaseNames=false
checkForTable=true

# All the reserved keywords for this database
# Based on the keywords listed at the following URL (2009-04-13):
# http://dev.mysql.com/doc/mysqld-version-reference/en/mysqld-version-reference-reservedwords-5-0.html
Keywords=\
  ADD,\
  ALL,\
  ALTER,\
  ANALYZE,\
  AND,\
  AS,\
  ASC,\
  ASENSITIVE,\
  BEFORE,\
  BETWEEN,\
  BIGINT,\
  BINARY,\
  BLOB,\
  BOTH,\
  BY,\
  CALL,\
  CASCADE,\
  CASE,\
  CHANGE,\
  CHAR,\
  CHARACTER,\
  CHECK,\
  COLLATE,\
  COLUMN,\
  COLUMNS,\
  CONDITION,\
  CONNECTION,\
  CONSTRAINT,\
  CONTINUE,\
  CONVERT,\
  CREATE,\
  CROSS,\
  CURRENT_DATE,\
  CURRENT_TIME,\
  CURRENT_TIMESTAMP,\
  CURRENT_USER,\
  CURSOR,\
  DATABASE,\
  DATABASES,\
  DAY_HOUR,\
  DAY_MICROSECOND,\
  DAY_MINUTE,\
  DAY_SECOND,\
  DEC,\
  DECIMAL,\
  DECLARE,\
  DEFAULT,\
  DELAYED,\
  DELETE,\
  DESC,\
  DESCRIBE,\
  DETERMINISTIC,\
  DISTINCT,\
  DISTINCTROW,\
  DIV,\
  DOUBLE,\
  DROP,\
  DUAL,\
  EACH,\
  ELSE,\
  ELSEIF,\
  ENCLOSED,\
  ESCAPED,\
  EXISTS,\
  EXIT,\
  EXPLAIN,\
  FALSE,\
  FETCH,\
  FIELDS,\
  FLOAT,\
  FLOAT4,\
  FLOAT8,\
  FOR,\
  FORCE,\
  FOREIGN,\
  FROM,\
  FULLTEXT,\
  GOTO,\
  GRANT,\
  GROUP,\
  HAVING,\
  HIGH_PRIORITY,\
  HOUR_MICROSECOND,\
  HOUR_MINUTE,\
  HOUR_SECOND,\
  IF,\
  IGNORE,\
  IN,\
  INDEX,\
  INFILE,\
  INNER,\
  INOUT,\
  INSENSITIVE,\
  INSERT,\
  INT,\
  INT1,\
  INT2,\
  INT3,\
  INT4,\
  INT8,\
  INTEGER,\
  INTERVAL,\
  INTO,\
  IS,\
  ITERATE,\
  JOIN,\
  KEY,\
  KEYS,\
  KILL,\
  LABEL,\
  LEADING,\
  LEAVE,\
  LEFT,\
  LIKE,\
  LIMIT,\
  LINES,\
  LOAD,\
  LOCALTIME,\
  LOCALTIMESTAMP,\
  LOCK,\
  LONG,\
  LONGBLOB,\
  LONGTEXT,\
  LOOP,\
  LOW_PRIORITY,\
  MATCH,\
  MEDIUMBLOB,\
  MEDIUMINT,\
  MEDIUMTEXT,\
  MIDDLEINT,\
  MINUTE_MICROSECOND,\
  MINUTE_SECOND,\
  MOD,\
  MODIFIES,\
  NATURAL,\
  NOT,\
  NO_WRITE_TO_BINLOG,\
  NULL,\
  NUMERIC,\
  ON,\
  OPTIMIZE,\
  OPTION,\
  OPTIONALLY,\
  OR,\
  ORDER,\
  OUT,\
  OUTER,\
  OUTFILE,\
  PRECISION,\
  PRIMARY,\
  PRIVILEGES,\
  PROCEDURE,\
  PURGE,\
  READ,\
  READS,\
  REAL,\
  REFERENCES,\
  REGEXP,\
  RELEASE,\
  RENAME,\
  REPEAT,\
  REPLACE,\
  REQUIRE,\
  RESTRICT,\
  RETURN,\
  REVOKE,\
  RIGHT,\
  RLIKE,\
  SCHEMA,\
  SCHEMAS,\
  SECOND_MICROSECOND,\
  SELECT,\
  SENSITIVE,\
  SEPARATOR,\
  SET,\
  SHOW,\
  SMALLINT,\
  SONAME,\
  SPATIAL,\
  SPECIFIC,\
  SQL,\
  SQLEXCEPTION,\
  SQLSTATE,\
  SQLWARNING,\
  SQL_BIG_RESULT,\
  SQL_CALC_FOUND_ROWS,\
  SQL_SMALL_RESULT,\
  SSL,\
  STARTING,\
  STRAIGHT_JOIN,\
  TABLE,\
  TABLES,\
  TERMINATED,\
  THEN,\
  TINYBLOB,\
  TINYINT,\
  TINYTEXT,\
  TO,\
  TRAILING,\
  TRIGGER,\
  TRUE,\
  UNDO,\
  UNION,\
  UNIQUE,\
  UNLOCK,\
  UNSIGNED,\
  UPDATE,\
  UPGRADE,\
  USAGE,\
  USE,\
  USING,\
  UTC_DATE,\
  UTC_TIME,\
  UTC_TIMESTAMP,\
  VALUES,\
  VARBINARY,\
  VARCHAR,\
  VARCHARACTER,\
  VARYING,\
  WHEN,\
  WHERE,\
  WHILE,\
  WITH,\
  WRITE,\
  XOR,\
  YEAR_MONTH,\
  ZEROFILL

# The character to append to attribute names to avoid exceptions due to
# clashes between keywords and attribute names
KeywordsMaskChar=_

#flags for loading and saving instances using DatabaseLoader/Saver
nominalToStringLimit=50
idColumn=auto_generated_id

