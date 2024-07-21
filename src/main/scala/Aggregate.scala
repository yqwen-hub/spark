import org.apache.spark.sql.functions.{col, concat_ws}
import org.apache.spark.sql.{Column, Dataset, Row}

case class Aggregate(column: Column, inputFields: Seq[String], outputFieldName: String, outputFieldName2: String) {
  def nameCol: Column = column.as(outputFieldName2)

  def concatenateCols(delimiter: String="") : Column = {
    val columns: Seq[Column] = inputFields.map(name => col(name))
    concat_ws(delimiter, columns: _*).as(outputFieldName)
  }
}

case class Aggregate2(condition: Column) {
  def transform(inputData: Dataset[Row]) : Dataset[Row] =
    inputData.where(condition)
}

object OBJ {
  var foo:String = "fooString"
  var bar:String = "barString"
  var foobar:String = "foobarString"

  def experimentFun(arg: String): Unit = {
    println("arg = " + arg)
  }
}
