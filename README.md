
# 第一代\-基于Hadoop体系的离线数据同步


## 一、背景


随着业务的发展，系统进行了微服务的差分，导致数据越来越分散，很难进行一个完整的生命周期的数据查询，对于某些业务的需求支持变得越来越难，越来越复杂，也越来越难以进行职责划分。对着业务的发展，数据量越来越大之后，为了良好的业务支持，进行了分库分表，分库分表规则五花八门，一旦脱离了业务逻辑，很难确定某一条数据在哪个库哪个表。


基于这样的问题和情况，为了满足业务需求，很自然的就想到了使用大数据服务，将业务数据归集到一起，建立完整的数据仓库，便于数据的查询。


## 二、数据同步架构


为了追求简单和通用，由于自身的认识现在，选择了最标准的大数据架构，即基于Hadoop的大数据体现。整个集群采用三节点，通过CDH进行集群的部署和维护。


![](https://img2024.cnblogs.com/blog/779703/202412/779703-20241205100849598-1576079178.png)


整个数据链路为：


通过Azkaban调用Spark应用，将数据从RDS同步到Hive，运营平台和报表系统采用Presto加速访问Hive的数据。


## 三、数据同步详细过程


数据同步采用Spark任务来进行，将任务打包之后，上传到Azkaban调度平台，使用Azkaban进行定时调度，完成T\+1级别的数据同步工作。


数据同步代码示例:



```
object MarketMysqlToHiveEtl extends SparkHivePartitionOverwriteApplication{


  /**
   * 删除已存在的分区
   *
   * @param spark SparkSessions实例
   * @param date 日期
   * @param properties 数据库配置
   */
  def delete_partition(spark: SparkSession, properties:Properties, date: String):Unit={
    val odsDatabaseName = properties.getProperty("hive.datasource.ods")
    DropPartitionTools
     .dropPartitionIfExists(spark,odsDatabaseName,"ods_t_money_record","ds",date)
    DropPartitionTools
     .dropPartitionIfExists(spark,odsDatabaseName,"ods_t_account","ds",date)
  }



  /**
   * 抽取数据
   * @param spark SparkSession实例
   * @param properties 数据库配置
   * @param date 日期
   */
  def loadData(spark: SparkSession, properties:Properties, date: String): Unit ={
    // 删除历史数据，解决重复同步问题
    delete_partition(spark,properties,date)

    // 获取数据源配置
    val odsDatabaseName = properties.get("hive.datasource.ods")
    val dataSource = DataSourceUtils.getDataSourceProperties(FinalCode.MARKET_MYSQL_FILENAME,properties)

    var sql = s"select id,account_id,type,original_id,original_code,money,reason,user_type,user_id,organization_id," +
    s"create_time,update_time,detail,deleted,parent_id,counts,'${date}' AS ds from TABLENAME where date(update_time) ='${date}'"

    // 同步数据
    MysqlToHiveTools.readFromMysqlIncrement(spark,dataSource,sql.replace("TABLENAME","t_money_record"),
                                            s"${odsDatabaseName}.ods_t_money_record",SaveMode.Append,"ds")


    sql = s"select id,code,customer_code,name,mobile,type,organization_id,organization_name,create_time,update_time,deleted,status,customer_name," +
    s"customer_id,channel_type,nike_name,version,register_Time,'${date}' AS ds from TABLENAME where date(update_time) ='${date}'"
    MysqlToHiveTools.readFromMysqlIncrement(spark,dataSource,sql.replace("TABLENAME","t_account"),
                                            s"${odsDatabaseName}.ods_t_account",SaveMode.Append,"ds")
  }



  /**
   * 数据etl
   * @param spark SparkSession实例
   * @param SparkSession 数据库配置
   */
  def etl(spark: SparkSession, properties:Properties): Unit = {
    val sparkConf = spark.sparkContext.getConf
    // 获取同步的日期
    var lastDate = sparkConf.get("spark.etl.last.day", DateUtils.getLastDayString)
    val dateList = new  ListBuffer[String]()
    if(lastDate.isEmpty){
      // 未配置，设置为前一天
      lastDate = DateUtils.getLastDayString
    }
    if(lastDate.contains("~")){
      // 如果是时间段，获取时间段中的每一天，解析为时间list
      val dateArray = lastDate.split("~")
      DateUtils.findBetweenDates(dateArray(0), dateArray(1)).foreach(it => dateList.append(it))
    }else if(lastDate.contains(",")){
      // 如果是使用,分隔的多个日期，解析为时间list
      lastDate.split(",").foreach(it => dateList.append(it))
    }else{
      // 添加进时间列表
      dateList.append(lastDate)
    }
    // 循环同步每天的数据
    dateList.foreach(it =>  loadData(spark, properties, it))
  }


  def main(args: Array[String]): Unit = {
    job() {
      val sparkAndProperties = SparkUtils.get()
      val spark = sparkAndProperties.spark
      val properties = sparkAndProperties.properties
      // 调度任务
      etl(spark,properties)
    }
  }
}


```

删除Partition的代码示例：



```
object DropPartitionTools {


  /**
   * 删除指定的Partition
   * @param SparkSession实例
   * @param database数据库名称
   * @param table表名称
   * @param partitionKey 分区字段的名称
   * @param partitionValue 具体的分区值
   */
  def dropPartitionIfExists(spark: SparkSession, database: String, table: String, partitionKey: String, partitionValue:String): Unit ={

     val df = spark.sql(
       s"""
         | show tables in ${database} like '${table}'
         |""".stripMargin)

    if(df.count() > 0 ){
      // 表存在，删除分区
      spark.sql(
        s"""
           |ALTER TABLE  ${database}.${table} DROP  IF EXISTS  PARTITION (${partitionKey}='${partitionValue}')
           |""".stripMargin)
    }
  }


  /**
   * 删除Partition
   * @param SparkSession实例
   * @param database数据库名称
   * @param table表名称
   * @param partitionKey 分区字段的名称
   */
  def dropHistoryPartitionIfExists(spark: SparkSession, database: String, table: String, partitionKey: String): Unit ={

    val df = spark.sql(
      s"""
         | show tables in ${database} like '${table}'
         |""".stripMargin)

    if(df.count() > 0 ){
      // 表存在，删除历史分区，获取8天前的日期
      val sevenDay = DateUtils.getSomeLastDayString(8);
      spark.sql(
        s"""
           |ALTER TABLE  ${database}.${table} DROP  IF EXISTS  PARTITION (${partitionKey} ='${sevenDay}')
           |""".stripMargin)
    }
  }

}


```

从RDS同步数据到HIVE的代码示例：



```
object MysqlToHiveTools {


  /**
   * 从mysql抽取数据到hive -- 全量
   * @param spark spark实例
   * @param dataSource 数据库配置信息
   * @param tableName 抽取的数据库表名
   * @param destTableName 目标表名
   * @param mode 抽取的模式
   */
  def mysqlToHiveTotal(spark: SparkSession, dataSource: JSONObject,tableName: String, destTableName:String,mode: SaveMode, partition: String): Unit = {
     val sql = "(select * from " + tableName + ") as t"
     mysqlToHive(spark, dataSource, sql, destTableName, mode, partition)
  }


  /**
   * 从mysql抽取数据到hive -- 增量量
   * @param spark spark实例
   * @param dataSource 数据库配置信息
   * @param sql 抽取数据的SQL
   * @param destTableName 目标表名
   * @param mode 抽取的模式
   */
  def readFromMysqlIncrement(spark: SparkSession, dataSource: JSONObject,sql: String, destTableName:String,mode: SaveMode, partition: String): Unit = {
    mysqlToHive(spark, dataSource, sql, destTableName, mode, partition)
  }


  /**
   * 真正的抽取数据
   * @param spark spark实例
   * @param properties 数据库配置信息
   * @param sql 抽取数据的SQL
   * @param destTableName 目标表名
   * @param mode 抽取的模式
   */
  def mysqlToHive(spark: SparkSession, dataSource: JSONObject,sql: String, destTableName:String, mode: SaveMode, partition: String):Unit={
    val df = spark.read.format("jdbc")
      .option("url",dataSource.getString("url"))
      .option("driver",dataSource.getString("driver"))
      .option("fetchSize", 10000)
      .option("numPartitions",2)
      .option("dbtable",s"(${sql}) AS t")
      .option("user",dataSource.getString("user"))
      .option("password",dataSource.getString("password"))
      .load()
    if(partition == null || partition.isEmpty){
      df.write.format("parquet").mode(mode).saveAsTable(destTableName)
    }else{
      df.write.format("parquet").mode(mode).partitionBy("ds").saveAsTable(destTableName)
    }
  }
}


```

Spark Application代码示例



```
trait SparkHivePartitionOverwriteApplication extends Logging{


  def getProperties(): Properties ={
    val prop:Properties = new Properties()
    val inputStream = this.getClass.getClassLoader.getResourceAsStream("config.properties")
    prop.load(inputStream);
    prop
  }

  def job(appName: String = null,
          master: String = null)(biz: => Unit): Unit = {
    var spark: SparkSession = null
    System.setProperty("HADOOP_USER_NAME", "mapred")
    val prop:Properties = getProperties()
    if (null == appName) {
      spark = SparkSession.builder
        .config("spark.sql.parquet.writeLegacyFormat", true)
        .config("spark.sql.sources.partitionOverwriteMode","dynamic")
        .config("hive.exec.dynamic.partition.mode","nonstrict")
        .config("spark.sql.hive.convertMetastoreParquet",false)
        .enableHiveSupport
        .getOrCreate
      var sparkAndProperties = SparkAndProperties(spark, prop)
      SparkUtils.set(sparkAndProperties)
    } else {
      spark = SparkSession.builder.master(master).appName(appName)
        .config("spark.sql.parquet.writeLegacyFormat", true)
        .config("spark.sql.sources.partitionOverwriteMode","dynamic")
        .config("hive.exec.dynamic.partition.mode","nonstrict")
        .config("spark.sql.hive.convertMetastoreParquet",false)
        .config("spark.testing.memory","2147480000")
        .config("spark.driver.memory","2147480000")
        .enableHiveSupport.getOrCreate
      var sparkAndProperties = SparkAndProperties(spark, prop)
      SparkUtils.set(sparkAndProperties)
      SparkUtils.set(sparkAndProperties)
    }
    biz
    spark.stop()
    SparkUtils.remove()
  }

}

case class SparkAndProperties(spark: SparkSession,
                              properties: Properties)

```

## 四、配套生态


1. 自定义UDF函数


在使用的过程中，需要将表中的IP地址，解析为所在地的名称，这需要调用第三方的一个服务接口来完成，为了完成这个任务，定义了一个自定义UDF函数，进行解析。


a. 自定义UDF函数



```
object ParseIp  {
    def evaluate(ip: String):String= {
      // 具体的IP解析服务
      SplitAddress.getPlaceFromIp(ip)
   }
}


```

b. 使用自定义UDF函数



```
object TraceTmpEtl extends SparkHivePartitionOverwriteApplication{

  /**
   * 数据同步任务
   * @param spark sparkSession实例
   * @param properties 数据库配置
   * @param date 日期
   */
  def tmp_t_trace_user_visit_real_time_statistic(spark: SparkSession,properties:Properties,date: String):Unit ={
    // 获取数据库配置的数据库名称
    val odsDatabaseName = properties.get("hive.datasource.ods")
    val tmpDatabaseName = properties.get("hive.datasource.tmp")

    // 注册自定义的UDF函数
    spark.udf.register("parseIP", (ip: String) => SplitAddress.getPlaceFromIp(ip))
    // 在Spark SQL中使用UDF函数
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE ${tmpDatabaseName}.tmp_t_statistic partition(ds='${date}')
         |select
         |	  `id` ,
         |	  `create_time` ,
         |	  `update_time` ,
         |	  `ip` ,
         |      replace( replace( replace(replace( case when parseIP(ip) rlike '^中国' then replace(parseIP(ip),'中国','')
         |          when parseIP(ip) rlike '^内蒙古' then replace(parseIP(ip),'内蒙古','内蒙古自治区')
         |          when parseIP(ip) rlike '^广西' then replace(parseIP(ip),'广西','广西壮族自治区')
         |          when parseIP(ip) rlike '^西藏' then replace(parseIP(ip),'西藏','西藏自治区')
         |          when parseIP(ip) rlike '^宁夏' then replace(parseIP(ip),'宁夏','宁夏回族自治区')
         |          when parseIP(ip) rlike '^新疆' then replace(parseIP(ip),'新疆','新疆维吾尔自治区')
         |          when parseIP(ip) rlike '^香港' then replace(parseIP(ip),'香港','香港特别行政区')
         |          when parseIP(ip) rlike '^澳门' then replace(parseIP(ip),'澳门','澳门特别行政区')
         |     else parseIP(ip) end, "省", "省."),"市", "市."),"县", "县."),"区", "区.") as ip_place,
         |	  `page_view` 
         |from ${odsDatabaseName}.ods_t_statistic where ds ='${date}'
         |""".stripMargin)
  }

  /**
   * 数据etl
   * @param spark SparkSession实例
   * @param properties 数据库配置
   */
  def etl(spark: SparkSession, properties:Properties): Unit = {
    val lastDate = DateUtils.getLastDayString
    tmp_t_trace_user_visit_real_time_statistic(spark,properties, lastDate)
  }


  
  def main(args: Array[String]): Unit = {
    job() {
      val sparkAndProperties = SparkUtils.get()
      val spark = sparkAndProperties.spark
      val properties = sparkAndProperties.properties
      etl(spark,properties)
    }
  }
}


```

2. 数据库的配置安全性问题


刚开始数据库配置同步配置文件直接写死，但是后续发现这样存在一些安全性的问题，后来采用将数据库相关的配置组合为一个JSON字符串，将其加密之后保存到MongoDB中，在使用时进行查询解密。



```
public class DataSourceUtils {

    private  static Logger logger = LoggerFactory.getLogger(DataSourceUtils.class);

    public static JSONObject getDataSourceProperties(String dataSourceKey,Properties properties){
        List<ServerAddress> adds = new ArrayList<>();
        try {
            String filePath = properties.getProperty("spark.mongo.properties.file.url");
            properties = new Properties();
            File file = new File(filePath);
            FileInputStream inputStream = null;
             inputStream = new FileInputStream(file);
            properties.load(inputStream);
        }catch (Exception e){
            logger.info("not load file, reason：" + e.getMessage());
            e.printStackTrace();
        }
        String mongoUrl = properties.getProperty("mongo_url");
        String mongoPort = properties.getProperty("mongo_port");
        String mongoDbName = properties.getProperty("mongo_dbName");
        String mongoCollect = properties.getProperty("mongo_collect");
        String mongoUser = properties.getProperty("mongo_user");
        String mongoPassword = properties.getProperty("mongo_password");
        String desKey = properties.getProperty("data_des_key");
        ServerAddress serverAddress = new ServerAddress(mongoUrl, Integer.parseInt(mongoPort));
        adds.add(serverAddress);
        List<MongoCredential> credentials = new ArrayList<>();
        MongoCredential mongoCredential = MongoCredential.createScramSha1Credential(mongoUser, mongoDbName, mongoPassword.toCharArray());
        credentials.add(mongoCredential);
        MongoClient mongoClient = new MongoClient(adds, credentials);
        MongoDatabase mongoDatabase = mongoClient.getDatabase(mongoDbName);
        MongoCollection<Document> collection = mongoDatabase.getCollection(mongoCollect);
        //指定查询过滤器
        Bson filter = Filters.eq("key", dataSourceKey);
        //指定查询过滤器查询
        FindIterable findIterable = collection.find(filter);
        //取出查询到的第一个文档
        Document document = (Document) findIterable.first();
        //打印输出
        String content = DESUtil.decrypt(desKey, document.getString("content"));
        return JSON.parseObject(content);
    }


    public static  Properties json2Properties(JSONObject jsonObject){
        String tmpKey = "";
        String tmpKeyPre = "";
        Properties properties = new Properties();
        j2p(jsonObject, tmpKey, tmpKeyPre, properties);
        return properties;
    }



    private static void j2p(JSONObject jsonObject, String tmpKey, String tmpKeyPre, Properties properties){
        for (String key : jsonObject.keySet()) {
            // 获得key
            String value = jsonObject.getString(key);
            try {
                JSONObject jsonStr = JSONObject.parseObject(value);
                tmpKeyPre = tmpKey;
                tmpKey += key + ".";
                j2p(jsonStr, tmpKey, tmpKeyPre, properties);
                tmpKey = tmpKeyPre;
            } catch (Exception e) {
                properties.put(tmpKey + key, value);
                System.out.println(tmpKey + key + "=" + value);
            }
        }
    }
    public static void main(String[] args) {

    }
}


```

3. Spark任务脚本示例



```
#!/bin/sh

##### env ###########
export JAVA_HOME=/usr/java/jdk1.8.0_151
export SPARK_HOME=/opt/cloudera/parcels/CDH/lib/spark
export PATH=${JAVA_HOME}/bin:${SPARK_HOME}/bin:${PATH}
export SPARK_USER=hadoop
export HADOOP_USER_NAME=hadoop
LAST_DAY="$1"
echo LAST_DAY

spark-submit \
--class net.app315.bigdata.operatereport.ods.MarketMysqlToHiveEtl \
--conf spark.sql.hive.metastore.version=2.1.1 \
--conf spark.sql.hive.metastore.jars=/opt/cloudera/parcels/CDH/lib/hive/lib/* \
--jars /opt/cloudera/parcels/CDH/lib/spark/jars/mysql-connector-java-5.1.48.jar,/opt/cloudera/parcels/CDH/lib/spark/jars/druid-1.1.10.jar \
--master yarn \
--deploy-mode cluster \
--executor-memory 4G \
--driver-memory 2G \
--num-executors 4 \
--executor-cores 2 \
--conf spark.dynamicAllocation.minExecutors=1 \
--conf spark.dynamicAllocation.maxExecutors=8 \
--conf spark.yarn.am.attemptFailuresValidityInterval=1h \
--conf spark.yarn.max.executor.failures=128 \
--conf spark.yarn.executor.failuresValidityInterval=1h \
--conf spark.task.maxFailures=4 \
--conf spark.yarn.maxAppAttempts=2 \
--conf spark.scheduler.mode=FIFO \
--conf spark.network.timeout=420000 \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.executor.heartbeatInterval=360000 \
--conf spark.sql.crossJoin.enabled=true \
--conf spark.mongo.properties.file.url=/opt/conf/mongo.properties \
--conf spark.etl.last.day="${LAST_DAY}" \
./target/spark-operate-report-project-1.0.jar

```

4. Job任务脚本实例



```
nodes:

  - name: bigdata_market_ods_etl
    type: command
    config:
      command: sh -x ./script/bigdata_market_ods_etl.sh "${spark.etl.last.day}"
      failure.emails: mxx@xxx.com

  - name: bigdata_market_dim_etl
    type: command
    config:
      command: sh -x ./script/bigdata_market_dim_etl.sh "${spark.etl.last.day}"
      failure.emails: mxx@xxx.com
    dependsOn:
          - bigdata_market_ods_etl
          
  - name: bigdata_market_dw_etl
    type: command
    config:
      command: sh -x ./script/bigdata_market_dw_etl.sh "${spark.etl.last.day}"
      failure.emails: mxx@xxx.com
    dependsOn:
          - bigdata_market_dim_etl
          - bigdata_user_dw_etl

```

## 五、备注


1. [Davinci报表](https://github.com) 一个开源的报表平台


# 第二代\-基于DolphinScheduler的离线数据同步


## 一、背景


自从上次开始使用基于Hadoop的大数据体现方案之后，业务平稳发展，但是随着时间的推移，新的问题开始出现，主要出现的问题为两个：


1. 数据的变更越来越频繁，基于之前SparkSQL任务的方式，只要需要对表结构进行变更，就需要重新修改Scala代码，然后重新进行任务的打包，这对于一些不熟悉代码的人来说，不太友好，而且成本也很高。
2. 虽然使用了Presto对HIVE的数据查询进行了加速，但是所在数据量越来越大，分析要求越来越复杂，即席查询越来越多，由于集群本身资源有限，查询能力出现了显著瓶颈。


## 二、数据同步架构


随着技术的发展已经对大数据的认识，接触到了更多的大数据相关的知识与组件，基于此，通过认真分析与思考之后，对数据的同步方案进行了如下的重新设计。


1. 数据存储与查询放弃了HDFS\+HIVE\+Presto的组合，转而采用现代化的MPP数据库StarRocks，StarRocks在数据查询的效率层面非常优秀，在相同资源的情况下，可以解决目前遇到的数据查询瓶颈。
2. 数据同步放弃了SparkSQL，转而采用更加轻量级的DATAX来进行，其只需要通过简单的配置，即可完成数据的同步，同时其也支持StarRocks Writer，开发人员只需要具备简单的SQL知识，就可以完成整个数据同步任务的配置，难度大大降低，效率大大提升，友好度大大提升。
3. 定时任务调度放弃Azkaban，采用现代化的任务调度工作Apache DolphinScheduler，通过可视化的页面进行调度任务工作流的配置，更加友好。


![](https://img2024.cnblogs.com/blog/779703/202412/779703-20241205100909138-797225882.png)


## 三、数据同步的详细流程


数据同步在这种方式下变动非常简单，只需要可视化的配置DataX任务，即可自动调度。下面的一个任务的配置示例



```
{
  "job": {
    "setting": {
      "speed": {
        "channel":1
      }
    },
    "content": [
      {
        "reader": {
          "name": "mysqlreader",
          "parameter": {
            "username": "",
            "password": "",
            "connection": [
              {
                "querySql": [
                  "SELECT CustomerId AS customer_id FROM base_info.base_customer where date(UpdateTime) > '${sdt}' and date(UpdateTime) < '${edt}'"
                ],
                "jdbcUrl": [
                  "jdbc:mysql://IP:3306/base_info?characterEncoding=utf-8&useSSL=false&tinyInt1isBit=false"
                ]
              }
            ]
          }
        },
        "writer": {
          "name": "starrockswriter",
          "parameter": {
            "username": "xxx",
            "password": "xxx",
            "database": "ods_cjm_test",
            "table": "ods_base_customer",
            "column": ["id"],
            "preSql": [],
            "postSql": [], 
            "jdbcUrl": "jdbc:mysql://IP:9030/",
            "loadUrl": ["IP:8050", "IP:8050", "IP:8050"],
            "loadProps": {
              "format": "json",
              "strip_outer_array": true
            }
          }
        }
      }
        ]
    }
}

```

数据同步过程中，遇到了另外一个问题，即业务存在大量的分库分表的，这些分库分表的逻辑五花八门，60张左右的逻辑板，经过分库分表之后达到了惊人的5000多张，为每张表配置任务很显然不太正常，这就需要能够在进行数据同步的时候动态生成需要的表列表，把表列表配置到DataX的配置文件中去。


经过技术的调用，Apache DolphinScheduler的Python任务类型很适合做这个事情，由于公司本身使用了Apache DolphinScheduler3\.0的版本，其Python任务还不支持返回数据到下游节点，但是社区最新版本已经支持该能力，因为按照已实现版本对其进行改造。


改造之后，Python节点能够将数据传递给他的下游节点，因此使用Python脚本查询获取需要进行同步的表列表，将其传递给DataX节点，完成动态表的数据同步



```
import pymysql
import datetime


def select_all_table(date: str):
    result_list = []
    sql = """
    SELECT concat('"', table_name, '"') 
    FROM information_schema.`TABLES` 
    WHERE table_schema='hydra_production_flow' 
        and table_name like 't_package_flow_log_%'
        and table_name like '%_{}'
    """.format(date)
    conn = pymysql.connect(host='', port=3306, user='', passwd='',
                           db='information_schema')
    cur = conn.cursor()
    cur.execute(query=sql)
    while 1:
        res = cur.fetchone()
        if res is None:
            break
        result_list.append(res[0])
    cur.close()
    conn.close()
    return result_list


if __name__ == '__main__':
    # 获取当前年月
    # 获取当前日期
    today = datetime.date.today()
    # 计算前一天的日期
    yesterday = today - datetime.timedelta(days=1)
    current_date = yesterday.strftime("%Y_%m")
    table_list = select_all_table(current_date)
    table_str = ",".join(table_list)
    # 设置变量,传递给下游节点
    print('${setValue(table_list=%s)}' % table_str)

```


```
{
  "job": {
    "setting": {
      "speed": {
        "channel":1
      }
    },
    "content": [
      {
        "reader": {
          "name": "mysqlreader",
          "parameter": {
            "username": "xxx",
            "password": "xxxx",
            "column": [
              "id",
              "concat('t_package_flow_log_',DATE_FORMAT(create_time,'%Y_%m'))",
              "operation_type"
            ],
            "where": "date(create_time) ${operator_symbol} '${dt}'",
            "connection": [
              {
                "table": [
                  ${table_list}
                ],
                "jdbcUrl": [
                  "jdbc:mysql://xx:3306/hydra_production_flow?characterEncoding=utf-8&useSSL=false&tinyInt1isBit=false"
                ]
              }
            ]
          }
        },
        "writer": {
                    "name": "starrockswriter",
                    "parameter": {
                        "username": "xxxxxx",
                        "password": "xxxxxxx",
                        "database": "ods_cjm",
                        "table": "ods_t_package_flow_log",
                        "column": ["id", "table_name","operation_type"],
                        "preSql": [],
                        "postSql": [], 
                        "jdbcUrl": "jdbc:mysql://IP:9030/",
                        "loadUrl": ["IP:8050", "IP:8050", "IP:8050"],
                        "loadProps": {
                            "format": "json",
                            "strip_outer_array": true
                        }
                    }
                }
            }
        ]
    }
}

```

## 四、踩坑记录


1. DATAX只支持python2\.x


下载支持python3\.x的相关文件，替换DataX中的相同文件，即可支持python3\.x使用


## 五、备注


1. [StarRocks](https://github.com) 高性能的MPP数据库
2. [DataX](https://github.com) 离线数据同步
3. [Apache DolphinScheduler](https://github.com) 任务调度工具


# 第三代\-基于Python自定义的离线数据同步


## 一、背景


自从采用Apache DolphinScheduler \+ StarRocks数据方案以来，一切都很平稳发展；但是随着时间的推移，总会出现新的问题。


随着数据量的增多，使用方需求的增长，已经一些其他因素的影响，对目前的数据同步架构带来了一些不小的挑战，这些问题导致任务的维护和更新越来越麻烦，需要耗费大量的时间来进行，急需一种新的方式来处理。


1. 由于等保的要求，线上RDS数据库不再支持通过公网访问，又因为StarRocks也在内网，这就导致了之前的数据同步链路彻底断裂，需要新的方案。
2. 由于数据结构的频繁变更、服务器资源导致的任务调度异常等等原因，需要重跑数据的需求越来越多，这就导致需要不断的修改任务的调度参数(如日期)，目前已经上线了10个业务的调度任务，也就是重新同步一次，就需要依次修改调度这10个任务，这期间还需要专人进行状态的跟踪，即使修改调度，压力很大。


## 二、数据同步架构


鉴于数据链路变更，导致原本数据链路断裂的问题，通过调研之后，决定采用KAFKA进行数据的中转，在内网部署KAFKA集群，同时该集群提供公网访问地址；在RDS所在的内网机器上使用DataX将RDS数据通过公网地址写入KAFKA，在内网中通过KafkaConnector消费数据写入StarRocks。


鉴于新的资源有限，原本内网提供了4台8C32G的服务器，但是新的RDS所在内网只能提供一台最大4C8G的服务器。因此放弃了使用Apache DolphinScheduler来进行调度，直接使用crontab调用对应的Python脚本进行DataX任务调度。


![](https://img2024.cnblogs.com/blog/779703/202412/779703-20241205100921285-1407264702.png)


## 三、具体的数据同步


新的方案，主要解决的问题有两个，一是DataX如何将数据写入KAFKA，二是Python脚本怎么解决前面遇到的修改复杂的问题。


1. DataX写KAFKA


DataX本身并没有kafkawriter实现，这就需要我们自己实现一个KafkaWriter来支持我们的需求，同时为了数据安全，希望能够对数据进行加密。


DataX的KafkaWriter实现



```
public class KafkaWriter extends Writer {

    public static class Job extends Writer.Job {

        private static final Logger logger = LoggerFactory.getLogger(Job.class);
        private Configuration conf = null;

        @Override
        public List split(int mandatoryNumber) {
            List configurations = new ArrayList(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                configurations.add(conf);
            }
            return configurations;
        }

        private void validateParameter() {
            this.conf.getNecessaryValue(Key.BOOTSTRAP_SERVERS, KafkaWriterErrorCode.REQUIRED_VALUE);
            this.conf.getNecessaryValue(Key.TOPIC, KafkaWriterErrorCode.REQUIRED_VALUE);
        }

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();
            logger.info("kafka writer params:{}", conf.toJSON());
            this.validateParameter();
        }


        @Override
        public void destroy() {

        }
    }

    public static class Task extends Writer.Task {
        private static final Logger logger = LoggerFactory.getLogger(Task.class);
        private static final String NEWLINE_FLAG = System.getProperty("line.separator", "\n");

        private Producer producer;
        private String fieldDelimiter;
        private Configuration conf;
        private Properties props;
        private AesEncryption aesEncryption;
        private List columns;

        @Override
        public void init() {
            this.conf = super.getPluginJobConf();
            fieldDelimiter = conf.getUnnecessaryValue(Key.FIELD_DELIMITER, "\t", null);
            columns = conf.getList(Key.COLUMN_LIST, new ArrayList<>(), String.class);

            props = new Properties();
            props.put("bootstrap.servers", conf.getString(Key.BOOTSTRAP_SERVERS));
            props.put("acks", conf.getUnnecessaryValue(Key.ACK, "0", null));//这意味着leader需要等待所有备份都成功写入日志，这种策略会保证只要有一个备份存活就不会丢失数据。这是最强的保证。
            props.put("retries", conf.getUnnecessaryValue(Key.RETRIES, "5", null));
            props.put("retry.backoff.ms", "1000");
            props.put("batch.size", conf.getUnnecessaryValue(Key.BATCH_SIZE, "16384", null));
            props.put("linger.ms", 100);
            props.put("connections.max.idle.ms", 300000);
            props.put("max.in.flight.requests.per.connection", 5);
            props.put("socket.keepalive.enable", true);
            props.put("key.serializer", conf.getUnnecessaryValue(Key.KEYSERIALIZER, "org.apache.kafka.common.serialization.StringSerializer", null));
            props.put("value.serializer", conf.getUnnecessaryValue(Key.VALUESERIALIZER, "org.apache.kafka.common.serialization.StringSerializer", null));
            producer = new KafkaProducer(props);
            String encryptKey = conf.getUnnecessaryValue(Key.ENCRYPT_KEY, null, null);
            if(encryptKey != null){
                aesEncryption = new AesEncryption(encryptKey);
            }
        }

        @Override
        public void prepare() {
            AdminClient adminClient = AdminClient.create(props);
            ListTopicsResult topicsResult = adminClient.listTopics();
            String topic = conf.getNecessaryValue(Key.TOPIC, KafkaWriterErrorCode.REQUIRED_VALUE);
            try {
                if (!topicsResult.names().get().contains(topic)) {
                    new NewTopic(
                            topic,
                            Integer.parseInt(conf.getUnnecessaryValue(Key.TOPIC_NUM_PARTITION, "1", null)),
                            Short.parseShort(conf.getUnnecessaryValue(Key.TOPIC_REPLICATION_FACTOR, "1", null))
                    );
                    List newTopics = new ArrayList();
                    adminClient.createTopics(newTopics);
                }
                adminClient.close();
            } catch (Exception e) {
                throw new DataXException(KafkaWriterErrorCode.CREATE_TOPIC, KafkaWriterErrorCode.REQUIRED_VALUE.getDescription());
            }
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            logger.info("start to writer kafka");
            Record record = null;
            while ((record = lineReceiver.getFromReader()) != null) {
                if (conf.getUnnecessaryValue(Key.WRITE_TYPE, WriteType.TEXT.name(), null)
                        .equalsIgnoreCase(WriteType.TEXT.name())) {
                    producer.send(new ProducerRecord(this.conf.getString(Key.TOPIC),
                            Md5Encrypt.md5Hexdigest(recordToString(record)),
                            aesEncryption ==null ? recordToString(record): JSONObject.toJSONString(aesEncryption.encrypt(recordToString(record))))
                    );
                } else if (conf.getUnnecessaryValue(Key.WRITE_TYPE, WriteType.TEXT.name(), null)
                        .equalsIgnoreCase(WriteType.JSON.name())) {
                    producer.send(new ProducerRecord(this.conf.getString(Key.TOPIC),
                            Md5Encrypt.md5Hexdigest(recordToString(record)),
                            aesEncryption ==null ? recordToJsonString(record) : JSONObject.toJSONString(aesEncryption.encrypt(recordToJsonString(record))))
                    );
                }
                producer.flush();
            }
        }

        @Override
        public void destroy() {
            if (producer != null) {
                producer.close();
            }
        }

        /**
         * 数据格式化
         *
         * @param record
         * @return
         */
        private String recordToString(Record record) {
            int recordLength = record.getColumnNumber();
            if (0 == recordLength) {
                return NEWLINE_FLAG;
            }
            Column column;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < recordLength; i++) {
                column = record.getColumn(i);
                sb.append(column.asString()).append(fieldDelimiter);
            }

            sb.setLength(sb.length() - 1);
            sb.append(NEWLINE_FLAG);
            return sb.toString();
        }

        /**
         * 数据格式化
         *
         * @param record 数据
         *
         */
        private String recordToJsonString(Record record) {
            int recordLength = record.getColumnNumber();
            if (0 == recordLength) {
                return "{}";
            }
            Map map = new HashMap<>();
            for (int i = 0; i < recordLength; i++) {
                String key = columns.get(i);
                Column column = record.getColumn(i);
                map.put(key, column.getRawData());
            }
            return JSONObject.toJSONString(map);
        }
    }
}

```

进行数据加密的实现：



```
public class AesEncryption {

    private SecretKey secretKey;

    public AesEncryption(String secretKey) {
        byte[] keyBytes = Base64.getDecoder().decode(secretKey);
        this.secretKey = new SecretKeySpec(keyBytes, 0, keyBytes.length, "AES");
    }


    public String encrypt(String data) {
        try {
            Cipher cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.ENCRYPT_MODE, secretKey);
            byte[] encryptedBytes = cipher.doFinal(data.getBytes());
            return Base64.getEncoder().encodeToString(encryptedBytes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String decrypt(String encryptedData) throws Exception {
        Cipher cipher = Cipher.getInstance("AES");
        cipher.init(Cipher.DECRYPT_MODE, secretKey);
        byte[] decodedBytes = Base64.getDecoder().decode(encryptedData);
        byte[] decryptedBytes = cipher.doFinal(decodedBytes);
        return new String(decryptedBytes);
    }
}



```

Kafka的公网配置


Kafka的内外网配置，只需要修改kafka/config下面的server.properties文件中的如下配置即可。



```
# 配置kafka的监听端口,同时监听9093和9092
listeners=INTERNAL://kafka节点3内网IP:9093,EXTERNAL://kafka节点3内网IP:9092

# 配置kafka的对外广播地址， 同时配置内网的9093和外网的19092
advertised.listeners=INTERNAL://kafka节点3内网IP:9093,EXTERNAL://公网IP:19092

# 配置地址协议
listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT

# 指定broker内部通信的地址
inter.broker.listener.name=INTERNAL

```

2. 自定义的配置文件


Python脚本需要能够自动生成对应的DataX调度的配置文件和shell脚本，自动调度DataX进行任务的执行。因此经过调研，采用自定义配置文件，通过读取配置文件，动态生成对应的DataX任务脚本和调度脚本，调度任务执行。


自定义的配置文件示例1：



```
{
  "datasource": {
    "host": "xxxxxx",
    "port": "3306",
    "username": "xxxxx",
    "password": "xxxxxxx",
    "properties": {
      "characterEncoding": "utf-8",
      "useSSL": "false",
      "tinyInt1isBit": "false"
    }
  },
  "table": {
    "database": "app",
    "table": "device",
    "column": [
      "Id AS id",
      "CompanyName AS company_name",
      "CompanyId AS company_id",
      "SecretKey AS secret_key",
      "Brand AS brand",
      "ModelType AS model_type",
      "Enable AS enable",
      "CAST(CreateTime as CHAR) AS create_time",
      "CAST(UpdateTime as CHAR) AS update_time"
    ],
    "where": "date(UpdateTime) >= '$[yyyy-MM-dd-8]'",
    "searchTableSql": []
  },
  "kafka": {
    "topic": "mzt_ods_cjm.ods_device"
  }
}

```

支持分库分表的配置文件示例2



```
{
  "datasource": {
    "host": "xxxxxxx",
    "port": "3306",
    "username": "xxxxxxx",
    "password": "xxxxxxxx",
    "properties": {
      "characterEncoding": "utf-8",
      "useSSL": "false",
      "tinyInt1isBit": "false"
    }
  },
  "table": {
    "database": "hydra_logistics_flow",
    "table": "",
    "column": [
      "id",
      "concat('t_logistics_sweep_out_code_flow_',DATE_FORMAT(create_time,'%Y')) AS table_name",
      "cus_org_id",
      "CAST(create_time as CHAR) AS create_time",
      "replace_product_id",
      "replace_product_name",
      "replace_product_code"
    ],
    "where": "date(create_time) >= '$[yyyy-MM-dd-8]'",
    "searchTableSql": [
      "SELECT concat('t_logistics_sweep_out_code_flow_',YEAR(SUBDATE(CURDATE(), 1))) AS TABLE_NAME",
      "SELECT concat('t_logistics_sweep_out_code_flow_',YEAR(DATE_SUB(DATE_SUB(CURDATE(), INTERVAL 1 DAY), INTERVAL 1 YEAR))) AS TABLE_NAME"
    ]
  },
  "kafka": {
    "topic": "mzt_ods_cjm.ods_t_logistics_sweep_out_code_flow"
  }
}

```

如上的配置文件，解释如下：




| KEY | 说明 |
| --- | --- |
| datasource | RDS数据源 |
| datasource.host | RDS数据库的host |
| datasource.port\> | RDS数据库的端口 |
| datasource.username | RDS数据库的用户名 |
| datasource.password | RDS数据库的密码 |
| datasource.properties | jdbc连接的参数，连接时拼接为?key\=value\&key\=value |
| table | 要同步的表信息 |
| table.database | RDS数据库名称 |
| table.table | RDS中表的名称，分库分表的可以为空 |
| table.column | RDS表中要同步的字段列表，支持取别名和使用函数 |
| table.where | 同步数据的过滤条件 |
| table.searchTableSql | 查询表名称的SQL语句，用于动态分库分表 |
| kafka | kafka相关的配置 |
| kafka.topic | 数据要写入的kafka topic的名称 |


3. Python调度脚本



```
import json
import os
import pymysql
import re
from datetime import datetime
from dateutil.relativedelta import relativedelta
import uuid
import subprocess
import logging
import hmac
import hashlib
import base64
import urllib.parse
import urllib
import requests
import time
from typing import List, Mapping


def list_files_in_directory(directory_path: str) -> List[str]:
    """
    获取目录下的所有以.json结尾的文件
    :param directory_path: 目录
    :return: 文件列表
    """
    entries = os.listdir(directory_path)
    # 过滤出所有文件
    files = [entry for entry in entries if
             os.path.isfile(os.path.join(directory_path, entry)) and entry.endswith(".json")]
    logging.info(f"读取配置文件数量：{len(files)}")
    return files


def read_file_content(file_path: str) -> str:
    """
    读取文件内容
    :param file_path: 文件路径
    :return: 文件内容
    """
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
    return content


def read_all_files_in_directory(directory_path: str) -> Mapping[str, str]:
    """
    读取文件夹下面的所有文件的内容
    :param directory_path: 文件夹路径
    :return: 内容map
    """
    logging.info(f"开始读取所有的配置文件信息")
    files = list_files_in_directory(directory_path)
    file_contents = {}
    for file in files:
        file_path = os.path.join(directory_path, file)
        content = read_file_content(file_path)
        file_contents[file] = content
    sorted_items = sorted(file_contents.items())
    sorted_dict = dict(sorted_items)
    return file_contents


def search_table_list(datasource: json, search_table_sql_list: List[str]) -> List[str]:
    """
    执行语句获取表信息
    :param datasource: 数据源信息
    :param search_table_sql_list: 查询表的SQL语句
    :return: 表列表
    """
    logging.info(f"开始查询需要同步的表")
    host = datasource['host']
    port = int(datasource['port'])
    username = datasource['username']
    password = datasource['password']
    conn = pymysql.connect(host=host,
                           port=port,
                           user=username,
                           passwd=password,
                           db='',
                           charset='utf8',
                           connect_timeout=200,
                           autocommit=True,
                           read_timeout=2000
                          )
    table_name_list = []
    for search_table_sql in search_table_sql_list:
        search_table_sql = parse_where_sql(search_table_sql)
        with conn.cursor() as cursor:
            cursor.execute(query=search_table_sql)
            while 1:
                res = cursor.fetchone()
                if res is None:
                    break
                table_name_list.append(res[0])
    return table_name_list


def general_default_job_config() -> json:
    """
    生成默认的datax配置
    :return: 默认的配置
    """
    default_job_json = """
    {
    "job": {
        "setting": {
            "speed": {
                 "channel":1
            }
        },
        "content": [
            {
                "reader": {
                    "name": "mysqlreader",
                    "parameter": {
                        "username": "test",
                        "password": "test1234",
                        "connection": [
                            {
                                "querySql": [
                                    "SELECT id, code from test.t_open_api_classify"
                                ],
                                "jdbcUrl": [
                                    "jdbc:mysql://IP:3306/test?characterEncoding=utf-8&useSSL=false&tinyInt1isBit=false"
                                ]
                            }
                        ]
                    }
                },
                 "writer": {
                    "name": "kafkawriter",
                    "parameter": {
                        "bootstrapServers": "IP:9092,IP:9092,IP:9092",
                        "topic": "test-m-t-k",
                        "ack": "all",
                        "batchSize": 1000,
                        "retries": 0,
                        "keySerializer": "org.apache.kafka.common.serialization.StringSerializer",
                        "valueSerializer": "org.apache.kafka.common.serialization.StringSerializer",
                        "fieldDelimiter": ",",
                        "writeType": "json",
                        "topicNumPartition": 1,
                        "topicReplicationFactor": 1,
                        "encryptionKey": "5s8FGjerddfWkG/b64CGHHZYvQ=="
                    }
                }
            }
        ]
    }
}
    """
    return json.loads(default_job_json, encoding='utf-8')


def general_jdbc_url(json_config: json) -> str:
    """
    根据数据源信息生成jdbc url
    :param json_config: 配置
    :return: jdbc url
    """
    logging.info(f"开始解析jdbc url")
    host = json_config['datasource']['host']
    port = int(json_config['datasource']['port'])
    database = json_config['table']['database']
    url = "jdbc:mysql://{}:{}/{}".format(host, port, database)
    # 解下properties
    properties = json_config['datasource']['properties']
    properties_list = []
    if properties is not None and len(properties) > 0:
        for key, value in properties.items():
            properties_list.append(key + "=" + str(value))
        url = url + "?" + "&".join(properties_list)
    logging.info(f"jdbc url： {url}")
    return url


def parse_where_sql(where_sql: str) -> str:
    """
    解析where语句
    :param where_sql: 原始where语句
    :return: 转换之后的where语句
    """
    # 定义支持的类型 $[yyyyMMdd+N_Y]  $[yyyyMMdd-N_Y]
    # 正则表达式模式
    logging.info(f"还是解析where语句：where_sql: {where_sql}")
    pattern = r"\$\[.*?\]"
    return re.sub(pattern, replacement_function, where_sql)


def replacement_function(match):
    """
    替换函数
    :param match: 匹配结果
    :return: 替换之后的结果
    """
    matched_text = match.group(0)
    return calc_datetime(matched_text)


def calc_datetime(expression: str) -> str:
    """
    计算时间表达式
    :param expression: 表达式
    :return: 计算之后的值
    """
    logging.info(f"开始计算时间参数：expression: {expression}")
    # 设置映射
    format_units = {
        "yyyy": "%Y",
        "MM": "%m",
        "dd": "%d",
        "HH": "%H",
        "mm": "%M",
        "ss": "%S"
    }

    unit_map = {
        "Y": "yyyy",
        "M": "MM",
        "d": "dd",
        "H": "HH",
        "m": "mm",
        "s": "ss"
    }
    # 解析参数
    expression = expression[2:-1]
    # 判断其开头，截取尾部
    min_unit = None
    for key, value in format_units.items():
        if key in expression:
            min_unit = key
            expression = expression.replace(key, value)

    # 替换完毕，确定是否有数字
    logging.info(f"转换为Python格式的表达式：expression: {expression}")
    # 定义正则表达式模式
    pattern = r'([^0-9]+)([-+]\d+(\*\d+)?)(?:_([YMdHms]))?'
    matches = re.match(pattern, expression)
    # 输出拆分结果
    if matches:
        date_part = matches.group(1)
        remainder = matches.group(2)
        unit = matches.group(4)
        if unit is not None and unit in unit_map.keys():
            min_unit = unit_map[unit]
        return calculate_expression(min_unit, date_part, remainder)
    else:
        return expression


def calculate_expression(min_unit: str, date_part: str, remainder: str) -> str:
    """
    计算表达式
    :param min_unit: 最小单位
    :param date_part: 日期表达式部分
    :param remainder: 偏移量部分
    :return: 计算之后的结果
    """
    logging.info(f"开始计算表达式：min_unit: {min_unit}, date_part: {date_part}, remainder:{remainder}")
    # 获取当前日期和时间
    now = datetime.now()
    # 计算时间的偏移量
    if remainder is None:
        # 格式化的日期
        formatted_datetime = now.strftime(date_part)
        logging.info(f"日期偏移量为空，返回值:{formatted_datetime}")
        return formatted_datetime
    else:
        # 计算偏移量
        plus_or_sub = remainder[0:1]
        offset = eval(remainder[1:])
        logging.info(f"计算偏移量，plus_or_sub：{plus_or_sub}， offset：{offset}")
        if min_unit == 'yyyy':
            if plus_or_sub == '-':
                now = now - relativedelta(years=offset)
            else:
                now = now + relativedelta(years=offset)
        elif min_unit == 'MM':
            if plus_or_sub == '-':
                now = now - relativedelta(months=offset)
            else:
                now = now + relativedelta(months=offset)
        elif min_unit == 'dd':
            if plus_or_sub == '-':
                now = now - relativedelta(days=offset)
            else:
                now = now + relativedelta(days=offset)
        elif min_unit == 'HH':
            if plus_or_sub == '-':
                now = now - relativedelta(hours=offset)
            else:
                now = now + relativedelta(hours=offset)
        elif min_unit == 'mm':
            if plus_or_sub == '-':
                now = now - relativedelta(minutes=offset)
            else:
                now = now + relativedelta(minutes=offset)
        elif min_unit == 'ss':
            if plus_or_sub == '-':
                now = now - relativedelta(seconds=offset)
            else:
                now = now + relativedelta(seconds=offset)
        formatted_datetime = now.strftime(date_part)
        logging.info(f"日期偏移量为空，返回值:{formatted_datetime}")
        return formatted_datetime


def general_reader(json_config: json) -> json:
    """
    生成配置的reader部分
    :param json_config: 配置
    :return: JSON结果
    """
    logging.info(f"开始生成DataX的配置JSON文件的reader内容")
    reader_json = json.loads("{}", encoding='utf-8')
    reader_json['name'] = "mysqlreader"
    reader_json['parameter'] = {}
    reader_json['parameter']['username'] = json_config['datasource']['username']
    reader_json['parameter']['password'] = json_config['datasource']['password']
    reader_json['parameter']['column'] = json_config['table']['column']
    reader_json['parameter']['connection'] = [{}]
    reader_json['parameter']['connection'][0]['table'] = json_config['table']['table']
    reader_json['parameter']['connection'][0]['jdbcUrl'] = [general_jdbc_url(json_config)]
    where_sql = json_config['table']['where']
    if where_sql is not None and where_sql != '':
        reader_json['parameter']['where'] = parse_where_sql(where_sql)
    return reader_json


def general_writer(json_config: json) -> json:
    """
    生成配置的Writer部分
    :param json_config: 配置
    :return: JSON结果
    """
    columns = json_config['table']['column']
    new_columns = []
    for column in columns:
        column = str(column).replace("`", "")
        if " AS " in str(column).upper():
            new_columns.append(str(column).split(" AS ")[1].strip())
        else:
            new_columns.append(str(column).strip())
    logging.info(f"开始生成DataX的配置JSON文件的Writer内容")
    writer_json = json.loads("{}", encoding='utf-8')
    writer_json['name'] = "kafkawriter"
    writer_json['parameter'] = {}
    writer_json['parameter']['bootstrapServers'] = "IP:19092,IP:19093,IP:19094"
    writer_json['parameter']['topic'] = json_config['kafka']['topic']
    writer_json['parameter']['ack'] = "all"
    writer_json['parameter']['batchSize'] = 1000
    writer_json['parameter']['retries'] = 3
    writer_json['parameter']['keySerializer'] = "org.apache.kafka.common.serialization.StringSerializer"
    writer_json['parameter']['valueSerializer'] = "org.apache.kafka.common.serialization.StringSerializer"
    writer_json['parameter']['fieldDelimiter'] = ","
    writer_json['parameter']['writeType'] = "json"
    writer_json['parameter']['topicNumPartition'] = 1
    writer_json['parameter']['topicReplicationFactor'] = 1
    writer_json['parameter']['encryptionKey'] = "5s8FGjerddfWkG/b64CGHHZYvQ=="
    writer_json['parameter']['column'] = new_columns
    return writer_json


def general_datax_job_config(datax_config: str):
    """
    生成job的配置内容
    :param datax_config: 配置
    :return: 完整的JSON内容
    """
    logging.info(f"开始生成DataX的配置JSON文件内容, {datax_config}")
    json_config = json.loads(datax_config, encoding='utf-8')
    # 判定是否需要查询表
    datasource = json_config['datasource']
    table = json_config['table']['table']
    search_table_sql_list = json_config['table']['searchTableSql']
    if search_table_sql_list is not None and len(search_table_sql_list) > 0:
        # 查询表列表，覆盖原来的配置信息
        table_list = search_table_list(datasource, search_table_sql_list)
    else:
        table_list = [table]
    json_config['table']['table'] = table_list

    # 开始生成配置文件
    job_json = general_default_job_config()
    job_json['job']['content'][0]['reader'] = general_reader(json_config)
    job_json['job']['content'][0]['writer'] = general_writer(json_config)
    return job_json


def write_job_file(base_path: str, job_config: json) -> str:
    """
    生成job的JSON配置文件
    :param base_path: 根路径
    :param job_config: 配置信息
    :return: 完整的JSON文件路径
    """
    # 生成一个脚本
    logging.info(f"开始创建DataX的配置JSON文件")
    date_day = datetime.now().strftime('%Y-%m-%d')
    timestamp_milliseconds = int(datetime.now().timestamp() * 1000)
    # 生成UUID
    file_name = str(uuid.uuid4()).replace("-", "") + "_" + str(timestamp_milliseconds) + ".json"
    # 完整文件路径
    # 创建文件夹
    mkdir_if_not_exist(base_path + "/task/datax/json/" + date_day)
    complex_file_path = base_path + "/task/datax/json/" + date_day + "/" + file_name
    logging.info(f"完整的DataX的配置JSON文件路径:{complex_file_path}")
    with open(complex_file_path, 'w+', encoding='utf-8') as f:
        f.write(json.dumps(job_config, ensure_ascii=False))
    return complex_file_path


def mkdir_if_not_exist(path):
    """
    创建目录
    :param path: 目录路径
    :return: None
    """
    os.makedirs(path, exist_ok=True)


def write_task_file(base_path: str, python_path: str, datax_path: str, job_file_path: str) -> str:
    """
    写shell脚本文件
    :param base_path: 跟路径
    :param python_path: python执行文件路径
    :param datax_path: datax执行文件路径
    :param job_file_path: JSON配置文件路径
    :return: shell脚本的完整路径
    """
    # 组合内容
    logging.info(f"开始创建Shell脚本文件")
    task_content = python_path + " " + datax_path + " " + job_file_path
    # 生成一个脚本
    date_day = datetime.now().strftime('%Y-%m-%d')
    timestamp_milliseconds = int(datetime.now().timestamp() * 1000)
    # 生成UUID
    task_file_name = str(uuid.uuid4()).replace("-", "") + "_" + str(timestamp_milliseconds) + ".sh"
    # 完整文件路径
    # 创建文件夹
    mkdir_if_not_exist(base_path + "/task/datax/shell/" + date_day)
    complex_file_path = base_path + "/task/datax/shell/" + date_day + "/" + task_file_name
    logging.info(f"完整的shell脚本路径: {complex_file_path}")
    with open(complex_file_path, 'w+', encoding='utf-8') as f:
        f.write(task_content)
    # 添加执行权限
    current_permissions = os.stat(complex_file_path).st_mode
    # 添加执行权限 (权限值 0o111 表示用户、组和其他人的执行权限)
    new_permissions = current_permissions | 0o111
    # 使用 os.chmod 设置新的权限
    os.chmod(complex_file_path, new_permissions)
    return complex_file_path


def signs(dd_secret: str, timestamp: str) -> str:
    """
    钉钉机器人签名
    :param dd_secret: 秘钥
    :param timestamp: 时间戳
    :return: 签名
    """
    secret_enc = dd_secret.encode('utf-8')
    string_to_sign = '{}\n{}'.format(timestamp, dd_secret)
    string_to_sign_enc = string_to_sign.encode('utf-8')
    hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
    sign = urllib.parse.quote(base64.b64encode(hmac_code))
    return sign


def real_send_msg(dd_secret: str, dd_access_token: str, text: json):
    """
    发送钉钉机器人消息
    :param dd_secret: 秘钥
    :param dd_access_token: token
    :param text: 内容
    :return: None
    """
    timestamp = str(round(time.time() * 1000))
    sign = signs(dd_secret, timestamp)
    headers = {'Content-Type': 'application/json'}
    web_hook = f'https://oapi.dingtalk.com/robot/send?access_token={dd_access_token}&timestamp={timestamp}&sign={sign}'
    # 定义要发送的数据
    requests.post(web_hook, data=json.dumps(text), headers=headers)


def send_msg(dd_secret: str, dd_access_token: str, job_start_time: str, total_count: int, success_count: int, fail_task_list: List[str]):
    """
    组合钉钉消息
    :param dd_secret: 秘钥
    :param dd_access_token: token
    :param job_start_time: 任务开始时间
    :param total_count: 总任务数
    :param success_count: 成功任务数
    :return: NONE
    """
    title = '### 数据同步结果'
    if success_count == total_count:
        title = '### 数据同步结果'
    elif success_count == 0:
        title = '### 数据同步结果'

    end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    result = {
        "msgtype": "markdown",
        "markdown": {
            "title": "数据同步结果",
            "text": title + ' \n\n\n\n- '
                    + "总同步任务数:" + str(total_count) + "\n\n- "
                    + "成功任务数:" + str(success_count) + "\n\n- "
                    + "失败任务数" + str(total_count - success_count) + "\n\n- "
                    + "开始时间:" + str(job_start_time) + "\n\n- "
                    + "结束时间:" + str(end_time) + "\n\n- "
                    + "失败列表:" + str(fail_task_list) + "\n\n "
        }
    }
    if success_count < total_count:
        result['markdown']['at'] = json.loads("{\"atMobiles\": [\"12345678997\"]}")
    real_send_msg(dd_secret, dd_access_token, result)


def run_job(dd_secret, dd_access_token, job_start_time, base_path: str, python_script_path: str, datax_json_path: str):
    """
    运行任务
    :param dd_secret: 秘钥
    :param dd_access_token: token
    :param job_start_time: 任务开始时间
    :param base_path: 根路径
    :param python_script_path: Python执行路径
    :param datax_json_path: datax执行路径
    :return: NONE
    """
    task_content_list = read_all_files_in_directory(base_path + "/task/config/")
    success_count = 0
    total_count = len(task_content_list)
    fail_task_list = []
    for task_content in task_content_list:
        try:
            logging.info(f"开始生成，配置文件名称：{task_content}")
            job_config = general_datax_job_config(task_content_list[task_content])
            job_file_path = write_job_file(base_path, job_config)
            shell_path = write_task_file(base_path, python_script_path, datax_json_path, job_file_path)
            logging.info(f"shell脚本创建成功，路径为：{base_path}")
            # 调用脚本
            call_shell(shell_path)
            success_count += 1
        except Exception as e:
            fail_task_list.append(task_content)
            logging.error(f"配置文件：{task_content} 执行失败", e)
    # 发送消息
    send_msg(dd_secret, dd_access_token, job_start_time, total_count, success_count, fail_task_list)


def call_shell(shell_path: str):
    """
    执行shell脚本
    :param shell_path: shell脚本路径
    :return: NONE
    """
    logging.info(f"调用shell脚本，路径为：{shell_path}")
    result = subprocess.run(shell_path,
                            check=True,
                            shell=True,
                            universal_newlines=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)

    # 输出标准输出
    logging.info(f"shell脚本{shell_path}标准输出：%s", result.stdout)
    # # 输出标准错误输出
    logging.info(f"shell脚本{shell_path}标准错误输出：%s", result.stderr)
    # # 输出返回码
    logging.info(f"shell脚本{shell_path}的返回码：%s", result.returncode)


if __name__ == '__main__':
    """
    码中台数据同步任务脚本
    使用前请修改如下配置信息：
      - secret  钉钉机器人的秘钥
      - access_token  钉钉机器人的token
      - python_path   Python的安装路径
      - datax_path   datax的执行文件路径
    """
    # 钉钉配置
    start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    secret = ''
    access_token = ''
    python_path = "/usr/bin/python3"
    datax_path = "/opt/datax-k/bin/datax.py"
    # 当前脚本文件的目录路径
    script_dir = '/opt/data-job'
    curr_date_day = datetime.now().strftime('%Y-%m-%d')
    # 创建文件夹
    mkdir_if_not_exist(script_dir + "/logs/" + curr_date_day)
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(lineno)d - %(message)s',
                        filename='logs/' + curr_date_day + '/app.log',
                        filemode='w')
    run_job(secret, access_token, start_time, script_dir, python_path, datax_path)
    logging.shutdown()


```

4. 同步日期的控制


我们在之前的任务同步中，遇到的问题便是日期的修改很麻烦，因此我们需要一个更加简单的方式来进行日期的批量更新。在我们上面的调度脚本中，包含了对日期表达式的解析，我们自定义了一种时间的表达式$\[yyyyMMddHHmmss\+/\-N\_Y] 通过解析该表达式，我们可以生成需要的任意时间，该时间表达式的含义为：


* yyyy 表示年份
* MM 表示月份
* dd 表示日期
* HH 表示24进制小时
* mm 表示分钟
* ss 表示秒
* + 表示当前时间加上N
* + 表示当前时间减去N
* \_Y 表示加减的单位，可以是YMdHms(年、月、日、时、分、秒)


通过对该表达式的解析，我们可以生成相对于当前之前或之后的任何格式的时间字符串，将其用于同步的where条件中，既可以完成针对时间的解析。


5. 如何更新日期


日期目前可以计算，但是我们需要能够批量修改配置文件中的WHERE条件中的时间表达式，如我们想同步8天前的数据，我们就需要将脚本中的表达式修改为$\[yyyyMMdd\-8\_d] ,即代表当前时间减去8天，这样我们就可以同步八天前那一天的数据，但是我们可能想同步从8天气到现在的所有数据，那么我们希望我们也能批量修改where表达式中的条件，如将\=改为\>\=。


鉴于以上的需求，我们开发了一个新的Python脚本，通过简单的配置，即可一次修改所有脚本中的where条件中的表达式，这样，我们只需要执行两个脚本，就完成了一切，再也不需要依次修改执行10个工作流了。



```
import json
import os
import logging
from typing import List, Mapping
import re
from datetime import datetime, date


def list_files_in_directory(directory_path: str) -> List[str]:
    """
    获取目录下的所有以.json结尾的文件
    :param directory_path: 目录
    :return: 文件列表
    """
    entries = os.listdir(directory_path)
    # 过滤出所有文件
    files = [entry for entry in entries if
             os.path.isfile(os.path.join(directory_path, entry)) 
             and entry.endswith(".json")]
    logging.info(f"读取配置文件数量：{len(files)}")
    return files


def read_file_content(file_path: str) -> str:
    """
    读取文件内容
    :param file_path: 文件路径
    :return: 文件内容
    """
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
    return content


def read_all_files_in_directory(directory_path: str) -> Mapping[str, str]:
    """
    读取文件夹下面的所有文件的内容
    :param directory_path: 文件夹路径
    :return: 内容map
    """
    logging.info(f"开始读取所有的配置文件信息")
    files = list_files_in_directory(directory_path)
    file_contents = {}
    for file in files:
        file_path = os.path.join(directory_path, file)
        content = read_file_content(file_path)
        file_contents[file] = content
    sorted_items = sorted(file_contents.items())
    sorted_dict = dict(sorted_items)
    return file_contents


def parse_where_sql(where_sql: str, sub_day: int, comparator: str = None) -> str:
    """
    解析where语句
    :param where_sql: 原始where语句
    :param sub_day: 天数
    :param comparator: 比较符  包括 = != > < >=   <=
    :return: 转换之后的where语句
    """
    # 定义支持的类型 $[yyyyMMdd+N_Y]  $[yyyyMMdd-N_Y]
    # 正则表达式模式
    pattern = r'\$(\[.*?\])'
    matches = re.finditer(pattern, where_sql)
    for match in matches:
        matched_text = match.group(1)
        new_search = calc_datetime(matched_text, sub_day)
        where_sql = where_sql.replace(matched_text, new_search)

    legal_comparator_list = ['>==','<>', '!=', '>=', '<=', '=', '>','<']
    legal_default = '@'
    if comparator is not None:
        for legal_comparator in legal_comparator_list:
            if legal_comparator in where_sql:
                where_sql = where_sql.replace(legal_comparator, legal_default)
        where_sql = where_sql.replace(legal_default, comparator)
    return where_sql


def calc_datetime(expression: str, sub_day: int) -> str:
    """
    计算时间表达式
    :param expression: 表达式
    :param sub_day: 天数
    :return: 计算之后的值
    """
    # 替换完毕，确定是否有数字
    # 定义正则表达式模式
    pattern = r'([^0-9]+)([-+]\d+(\*\d+)?)(?:_([YMdHms]))?'
    matches = re.match(pattern, expression)
    # 输出拆分结果
    if matches:
        date_part = matches.group(1)
        remainder = matches.group(2)
        unit = matches.group(4)
        plus_or_sub = remainder[0:1]
        if unit is not None:
            return date_part + plus_or_sub + str(sub_day) + '_' + unit + "]"
        else:
            return date_part + plus_or_sub + str(sub_day) + "]"
    else:
        return expression


def check_parma(formatted_date: str, sub_day: int, comparator: str = None):
    """
    校验参数是否合法
    :param formatted_date: 格式化日期
    :param sub_day: 天数
    :param comparator: 操作符
    :return: NONE
    """
    legal_comparator = ['=', '<>', '!=', '>', '>=', '<', '<=']
    if formatted_date is None and sub_day is None:
        raise "formatted_date 和 sub_day不能同时为空"

    if formatted_date is not None:
        try:
            datetime.strptime(formatted_date, "%Y-%m-%d")
        except Exception as _:
            raise "formatted_date 必须是一个完整的yyyy-MM-dd日期格式, 当前sub_day={}".format(sub_day)

    if formatted_date is None and not isinstance(sub_day, int):
        raise "sub_day 必须是一个整数, 当前sub_day={}".format(sub_day)

    if comparator is not None and comparator not in legal_comparator:
        raise "comparator 不合法,合法操作列表为：{} 当前comparator={}".format(legal_comparator, comparator)


def update_file(base_path: str, sub_day: int, comparator: str = None):
    """
    更新配置文件
    :param base_path 配置文件根目录
    :param sub_day  要减去的天数
    :param comparator 比较符
    """
    file_dict = read_all_files_in_directory(base_path)
    for key, value in file_dict.items():
        json_data = json.loads(value, encoding='utf-8')
        where_sql = json_data['table']['where']
        if where_sql is not None:
            new_where_sql = parse_where_sql(where_sql, sub_day, comparator)
            json_data['table']['where'] = new_where_sql

        search_tal_sql_list = json_data['table']['searchTableSql']
        if search_tal_sql_list is not None:
            new_search_table_sql_list = []
            for search_tal_sql in search_tal_sql_list:
                new_search_table_sql = parse_where_sql(search_tal_sql, sub_day)
                new_search_table_sql_list.append(new_search_table_sql)
            json_data['table']['searchTableSql'] = new_search_table_sql_list

        with open(base_path + "/" + key, "w+", encoding='utf-8') as f:
            f.write(json.dumps(json_data, ensure_ascii=False, indent=2))
        print("{} 更新完成".format(key))


if __name__ == '__main__':
    """
    更新数据同步配置文件的日期
    """
    dir_path = r'/opt/data-job/task/config'
    # 多少天前
    day = 6
    # 要指定的日期
    date_format = '2024-11-19'
    # where表达式的条件
    comparator_symbol = '>='
    check_parma(date_format, day, comparator_symbol)
    if date_format is not None:
        # 使用date_format的值覆盖day
        single_date = datetime.strptime(date_format, "%Y-%m-%d").date()
        current_date = date.today()
        day = (current_date - single_date).days
    update_file(dir_path, day, comparator_symbol)


```

6. 通过KafkaConnector同步数据到StarRocks
	1. starrocks\-connector\-for\-kafka的实现


StarRocks官方提供了starrocks\-connector\-for\-kafka的实现，我们只需要在其中加入我们的数据解密逻辑即可直接使用。



```
package com.starrocks.connector.kafka.transforms;

public class DecryptJsonTransformation extends ConnectRecord> implements Transformation {
    private static final Logger LOG = LoggerFactory.getLogger(DecryptJsonTransformation.class);
    private AesEncryption aesEncryption;

    private interface ConfigName {
        String SECRET_KEY = "secret.key";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
    .define(ConfigName.SECRET_KEY, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "secret key");


    @Override
    public R apply(R record) {
        if (record.value() == null) {
            return record;
        }
        String value = (String) record.value();
        try {
            String newValue = aesEncryption.decrypt(value);
            JSONObject jsonObject = JSON.parseObject(newValue, JSONReader.Feature.UseBigDecimalForDoubles);
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), null, jsonObject, record.timestamp());
        } catch (Exception e) {
            return record;
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map map) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, map);
        String secretKey = config.getString(ConfigName.SECRET_KEY);
        aesEncryption = new AesEncryption(secretKey);
    }
}

```

解密的逻辑



```
package com.starrocks.connector.kafka;


import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.util.Base64;

public class AesEncryption {

    private SecretKey secretKey;

    public AesEncryption(String secretKey) {
        byte[] keyBytes = Base64.getDecoder().decode(secretKey);
        this.secretKey = new SecretKeySpec(keyBytes, 0, keyBytes.length, "AES");
    }

    public String encrypt(String data) {
        try {
            Cipher cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.ENCRYPT_MODE, secretKey);
            byte[] encryptedBytes = cipher.doFinal(data.getBytes());
            return Base64.getEncoder().encodeToString(encryptedBytes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String decrypt(String encryptedData) throws Exception {
        Cipher cipher = Cipher.getInstance("AES");
        cipher.init(Cipher.DECRYPT_MODE, secretKey);
        byte[] decodedBytes = Base64.getDecoder().decode(encryptedData);
        byte[] decryptedBytes = cipher.doFinal(decodedBytes);
        return new String(decryptedBytes);
    }
}



```

b. 配置KafkaConnector任务



```
{
  "name": "mzt_ods_cjm.ods_device-connect",
  "config": {
    "connector.class": "com.starrocks.connector.kafka.StarRocksSinkConnector",
    "topics": "mzt_ods_cjm.ods_device",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.storage.StringConverter",
    "key.converter.schemas.enable": "true",
    "value.converter.schemas.enable": "false",
    "starrocks.http.url": "IP:8050,IP:8050,IP:8050",
    "starrocks.topic2table.map": "mzt_ods_cjm.ods_device:ods_device",
    "starrocks.username": "xxxxxxx",
    "starrocks.password": "xxxxxx",
    "starrocks.database.name": "ods_cjm",
    "sink.properties.strip_outer_array": "true",
    "sink.properties.columns": "id,company_name,company_id,secret_key,",
    "sink.properties.jsonpaths": "[\"$.id\",\"$.company_name\",\"$.company_id\",\"$.secret_key\"]",
    "transforms": "decrypt",
    "transforms.decrypt.type": "com.starrocks.connector.kafka.transforms.DecryptJsonTransformation",
    "transforms.decrypt.secret.key": "5s8ekjRWkG/b64CGHHZYvQ=="
  }
}

```

## 四、备注


1. [starrocks\-connector\-for\-kafka](https://github.com):[slower加速器](https://jisuanqi.org) Kafka Connector是StarRocks数据源连接器
2. [DataX](https://github.com) 批量数据同步工具
3. [kafka\-console\-ui](https://github.com) Kakfa可视化控制台
4. [StarRocks\-kafka\-Connector](https://github.com) 通过kafkaConnector导入数据到StarRocks
5. [StreamLoad实现数据增删改](https://github.com)
6. Kafka Connector的API列表




| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | /connectors | 返回活动连接器的列表 |
| POST | /connectors | 创建一个新的连接器; 请求主体应该是包含字符串name字段和config带有连接器配置参数的对象字段的JSON对象 |
| GET | /connectors/ | 获取有关特定连接器的信息 |
| GET | /connectors/{name}/config | 获取特定连接器的配置参数 |
| PUT | /connectors/{name}/config | 更新特定连接器的配置参数 |
| GET | /connectors/{name}/status | 获取连接器的当前状态，包括连接器是否正在运行，失败，已暂停等，分配给哪个工作者，失败时的错误信息以及所有任务的状态 |
| GET | /connectors/{name}/tasks | 获取当前为连接器运行的任务列表 |
| GET | /connectors/{name}/tasks/{taskid}/status | 获取任务的当前状态，包括如果正在运行，失败，暂停等，分配给哪个工作人员，如果失败，则返回错误信息 |
| PUT | /connectors/{name}/pause | 暂停连接器及其任务，停止消息处理，直到连接器恢复 |
| PUT | /connectors/{name}/resume | 恢复暂停的连接器（或者，如果连接器未暂停，则不执行任何操作） |
| POST | /connectors/{name}/restart | 重新启动连接器（通常是因为失败） |
| POST | /connectors/{name}/tasks/{taskId}/restart | 重启个别任务（通常是因为失败） |
| DELETE | /connectors/ | 删除连接器，停止所有任务并删除其配置 |


