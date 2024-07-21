import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession, functions}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField}
import OBJ.{bar, experimentFun}
import org.apache.spark.sql.functions.{col, lit, sum}


object FirstSparkApp {
  def main(args: Array[String]): Unit = {

    // Initialize SparkSession
    val spark: SparkSession = SparkSession.builder
      .appName("First Spark App")
      .config("spark.master", "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Create a simple DataFrame
    val data: Seq[(String, String, String, Int, Int)] = Seq(
      ("Alice", "Young", "F", 1, 12),
      ("Bob", "Huchingson", "M", 2, 52),
      ("Catherine", "Simmon", "F", 1, 37),
      ("Tara", "Jackson", "F", 2, 40),
      ("Theo", "Metheus", "M", 2, 37),
      ("Ian", "Nutting", "M", 1, 13),
      ("Trevor", "Haire", "M", 1, 13),
      ("Maya", "Beresford", "F", 1, 9)
    )

    val df: DataFrame = spark.createDataFrame(data).toDF("FirstName", "LastName", "Sex", "cohort", "Age")

    // Show the DataFrame
    df.show()

    val inputFields: Seq[StructField] = Seq(
      StructField("FirstName", StringType),
      StructField("LastName", StringType),
      StructField("Sex", StringType),
      StructField("Cohort", IntegerType),
      StructField("Age", IntegerType)
    )

    val outputField: StructField = StructField("combineCol", StringType)

    val aggregate = Aggregate(df.col("Sex"), inputFields.map(_.name), outputField.name, "Gender")
    val concatenatedColumn: Column = aggregate.concatenateCols(" ")
    val renamedColumn: Column = aggregate.nameCol

    val resultDF: DataFrame = df.withColumn(outputField.name, concatenatedColumn).withColumn("Gender", renamedColumn)

    resultDF.show(truncate = false)

    println("bar = " + bar)
    experimentFun(soemthingReturnsString)  // pass a function tha returns a String as an argument to a
    // function that expects String as the argument

    val aggregate2 = Aggregate2(criteriaName)
    val filtered: Dataset[Row] = aggregate2.transform(df)
    filtered.show(truncate = false)

    val aggregate2Age = Aggregate2(criteriaAge)
    val filteredAge: Dataset[Row] = aggregate2Age.transform(df)
    filteredAge.show()

    val aggregate3 = Aggregate(filteredAge.col("Sex"), inputFields.map(_.name), outputField.name, "Gender")
    val concatenatedColnum2: Column = aggregate3.concatenateCols(" ")
    val renameColumn2: Column = aggregate3.nameCol

    val resultDF2: DataFrame = filteredAge.withColumn(outputField.name, concatenatedColnum2).withColumn("Gender", renameColumn2)
    resultDF2.show(truncate = false)

    val aggregate4= Aggregate(functions.count(lit(2)), Seq(), "field", "count")
    val resultDf3 = resultDF.groupBy(col("Cohort"), col("Sex")).agg(aggregate4.nameCol).orderBy("Sex", "Cohort")
    resultDf3.show(truncate = false)

    val resultDF4 = resultDF.groupBy("Cohort", "Sex").count()
    resultDF4.show()

    val countField: StructField = StructField("count", IntegerType)

    val aggregate5 = Aggregate(sum(col("Age")), Seq(countField.name), countField.name, countField.name)
    val resultDF5 = df.groupBy(col("Cohort")).agg(aggregate5.nameCol)
    resultDF5.show()

    val resultDF6 = df.groupBy(col("Cohort")).agg(sum(col("Age"))).as("Sum of all ages")
    resultDF6.show(truncate = false)

    // Stop the SparkSession
    spark.stop()
  }

  def soemthingReturnsString(): String = {
    "A quick brown fox jumps over a lazy dog!"
  }

  def criteriaName(): Column = {
    col("FirstName").equalTo(lit("Alice"))
  }

  def criteriaAge(): Column = {
    col("Age") > 35
  }
}
