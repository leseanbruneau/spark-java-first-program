package com.leseanbruneau;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;


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
		
		// Apache Spark Extra Stuff - use if you like
		//
		// Create Dataframe with less columns
		
		horseRacesDF = horseRacesDF.drop("DATE").drop("SDAY")
								.drop("FIN1").drop("PRGNUM1").drop("PPNUM1").drop("FODDS1").drop("PTFAV1")
								.drop("FIN2").drop("PRGNUM2").drop("PPNUM2").drop("FODDS2").drop("PTFAV2")
								.drop("FIN3").drop("PRGNUM3").drop("PPNUM3").drop("FODDS3").drop("PTFAV3")
								.drop("FIN4").drop("PRGNUM4").drop("PPNUM4").drop("FODDS4").drop("PTFAV4");

		Dataset<Row> turfRacesDF = horseRacesDF.filter("upper(SURFACE) like 'TURF' ");
		
		System.out.println("Show the Turf races");
		turfRacesDF.show();
		
		Dataset<Row> highDollarRacesDF = horseRacesDF.filter("PURSE >= 80000 ");
		
		highDollarRacesDF = highDollarRacesDF.orderBy(col("PURSE").desc());
		
		System.out.println("Show races with purse money greater than or equal to 80,000, sorted by purse money in decending order");
		highDollarRacesDF.show();
		
	}

}
