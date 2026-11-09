package com.manas.lakehouse.iceberg

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

/**
 * `local` here is a Hadoop-catalog Iceberg catalog rooted at a local filesystem path,
 * standing in for the S3 warehouse the resume claim names; see the README's honest
 * framing for why no real S3 bucket is exercised.
 */
object IcebergSupport {

    fun newSparkSession(appName: String, warehousePath: String, cores: Int = 2): SparkSession =
        SparkSession.builder()
            .master("local[$cores]")
            .appName(appName)
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", warehousePath)
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .getOrCreate()

    fun writeTable(spark: SparkSession, df: Dataset<Row>, tableName: String) {
        val fqName = "local.lakehouse.$tableName"
        if (!spark.catalog().tableExists(fqName)) {
            df.writeTo(fqName).create()
        } else {
            df.writeTo(fqName).append()
        }
    }
}
