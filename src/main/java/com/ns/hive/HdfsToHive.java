package com.ns.hive;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/**
 * 
 * 验证将hdfs 上数据写入hive
 * 
 * @author ryan
 *
 */

public class HdfsToHive {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("HiveTest").set("spark.serializer",
				"org.apache.spark.serializer.KryoSerializer");

		JavaSparkContext jsc = new JavaSparkContext(conf);

		HiveContext hc = new HiveContext(jsc.sc());

		///要指定hdfs路径
		JavaRDD<String> lines = jsc.textFile("hdfs://cztcluster/user/edc_yz_safety/persons.txt");
		
		JavaRDD<Person> persons = lines.map(new Function<String, Person>() {

			public Person call(String line) throws Exception {
				// TODO Auto-generated method stub
				String[] splited = line.split(",");
				Person p = new Person();
				p.setId(Integer.valueOf(splited[0].trim()));
				p.setName(splited[1]);
				p.setAge(Integer.valueOf(splited[2].trim()));
				return p;
			}
		});

		//在底层通过反射的方式获得Person的所有fields，结合RDD本身，就生成了DataFrame
        DataFrame df = hc.createDataFrame(persons, Person.class);
		
        df.registerTempTable("person_tmp");
        
		String sql = "insert into table yz_test.parquet_person select id , name , age from person_tmp";
		
		hc.sql(sql);

		jsc.stop();

		jsc.close();

	}

}