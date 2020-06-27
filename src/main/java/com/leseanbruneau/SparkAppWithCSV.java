package com.leseanbruneau;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class SparkAppWithCSV {

	public static void main(String[] args) {
		// Get rid of extra logging
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		
		SparkSession spark = new SparkSession.Builder()
							.appName("First Spark Java Program")
							.master("local[*]")
							.getOrCreate();
		
		Dataset<Row> horseRacesDF = spark.read()
								.option("header", "true")
								.option("inferSchema", "true")
								.format("csv")
								.load("src/main/resources/20190711-01.csv");
		
		horseRacesDF.printSchema();
		horseRacesDF.show();

	}

}
