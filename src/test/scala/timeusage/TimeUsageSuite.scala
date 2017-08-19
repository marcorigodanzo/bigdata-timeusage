package timeusage

import org.apache.spark.sql.{ColumnName, DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import timeusage.TimeUsage._

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {


  def initializeTimeUsage(): Boolean =
    try {
      TimeUsage
      true
    } catch {
      case ex: Throwable =>
        println(ex.getMessage)
        ex.printStackTrace()
        false
    }

  override def afterAll(): Unit = {
    assert(initializeTimeUsage(), " -- did you create a sparkSession?")
    import TimeUsage._
    spark.stop()
  }






  test("'tucaseid' should be the first column") {
    // SETUP
    assert(initializeTimeUsage(), " -- everithing ok?")
    val (columns, initDf) = read("/timeusage/atussum.csv")

    val condition = (columns.head === "tucaseid")
    initDf.printSchema()
    assert(condition, "the first column should be tucaseid")
  }

  test("Schema of the dataframe equal to column list from read function") {
    // SETUP
    assert(initializeTimeUsage(), " -- everithing ok?")
    val (columns, initDf) = read("/timeusage/atussum.csv")
    val cols = initDf.columns

    assert(cols.toSet == columns.toSet, "The given columns are not the same as the dataframe columns")


  }



  test("verify summaryDf") {
    // SETUP
    assert(initializeTimeUsage(), " -- everithing ok?")
    val (columns, initDf) = read("/timeusage/atussum.csv")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)

    summaryDf.printSchema()
    val colsSet = summaryDf.columns.toSet
    val summaryExpectedSetOfColumns = Set("working", "sex", "age", "primaryNeeds", "work", "other" )

    assert(colsSet === summaryExpectedSetOfColumns, "The summaryDf does not contains the expected columns")

  }


  test("timeusage grouped") {
    // SETUP
    assert(initializeTimeUsage(), " -- everithing ok?")
    val (columns, initDf) = read("/timeusage/atussum.csv")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
    val finalDf = timeUsageGrouped(summaryDf)

    // working -> 2, sex -> 2, age -> 3
    // df should be of 2*2*3 = 12 rows
    finalDf.show()
    assert(finalDf.count() === 12, "The dataframe has a wrong number of rows")

  }


  test("timeusage grouped SQL") {
    // SETUP
    assert(initializeTimeUsage(), " -- everithing ok?")
    val (columns, initDf) = read("/timeusage/atussum.csv")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
    val finalDf = timeUsageGroupedSql(summaryDf)

    // working -> 2, sex -> 2, age -> 3
    // df should be of 2*2*3 = 12 rows
    finalDf.show()
    assert(finalDf.count() === 12, "The dataframe has a wrong number of rows")

  }


  test("timeusage grouped typed") {
    // SETUP
    assert(initializeTimeUsage(), " -- everithing ok?")
    val (columns, initDf) = read("/timeusage/atussum.csv")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
    val finalDf = timeUsageGroupedTyped(timeUsageSummaryTyped(summaryDf))

    // working -> 2, sex -> 2, age -> 3
    // df should be of 2*2*3 = 12 rows
    finalDf.show()
    assert(finalDf.count() === 12, "The dataframe has a wrong number of rows")

  }

}
