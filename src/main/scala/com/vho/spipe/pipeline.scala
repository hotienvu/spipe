package com.vho.spipe

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row, SparkSession}
import scalaz.{IndexedReaderWriterState, Semigroup}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

object pipeline {
  trait DataStage[W <: Dataset[_], S1 <: Dataset[_], S2 <: Dataset[_], A] extends Serializable {

    def apply(): DataStageResult[W, S1, S2, A]

    def stageName: String

    def stageDescription: String
  }

  final case class DataQuality(stage: String,
                               message: String,
                               rowKey: String = "",
                               fieldName: String = "",
                               fieldValue: String = "",
                               cause: Throwable)

  object DataQuality{
    def apply(row: Row): DataQuality = new DataQuality(
      row.getAs[String]("stage"),
      row.getAs[String]("message"),
      row.getAs[String]("rowKey"),
      row.getAs[String]("fieldName"),
      row.getAs[String]("fieldValue"),
      row.getAs[Throwable]("cause")
    )
  }

  type DataStageResult[W, S1 <: Dataset[_], S2 <: Dataset[_], A] = IndexedReaderWriterState[SparkSession, W, S1, S2, A]

  type DataErrorDS = Dataset[DataQuality]
  type NoInputDS = Dataset[Nothing]
  type DataQualityDS = Dataset[DataQuality]
  type DataQualityDF = DataFrame

  object DataStageResult {
    def empty[W <: Dataset[_], S1 <: Dataset[_], S2 <: Dataset[_], A]: DataStageResult[W, S1, S2, A] =  ???

    def apply[W <: Dataset[_], S1 <: Dataset[_], S2 <: Dataset[_], A](func: (SparkSession, S1) => (W, A, S2)) : DataStageResult[W, S1, S2, A] =
      IndexedReaderWriterState(func)
  }

  object implicits {
    import scala.reflect.ClassTag

    implicit def eitherEncoder[A <: Either[_, _]](implicit ct: ClassTag[A]): Encoder[A] =
      org.apache.spark.sql.Encoders.kryo[A](ct)

    implicit def dataFrameSemigroup[A]: Semigroup[Dataset[A]] = Semigroup.instance(_ union _)

  }

  object syntax {
    implicit class ExtendedEitherDataset[L, R](ds: Dataset[Either[L, R]]) {
      def getLeft(implicit encoder: Encoder[L]): Dataset[L] = ds.filter(_.isLeft).map(_.left.get)
      def getRight(implicit encoder: Encoder[R]): Dataset[R] = ds.filter(_.isRight).map(_.right.get)
    }

    implicit class ExtendedSparkSession(spark: SparkSession) {
      def emptyErrorDS(implicit encoder: Encoder[DataQuality]): Dataset[DataQuality] =
        spark.emptyDataset[DataQuality]

      def emptyFromDS[T](ds: Dataset[T])(implicit encoder: Encoder[T]): Dataset[T] =
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], ds.schema).as[T]
    }

    implicit class ExtendedDataset[T](ds: Dataset[T]) {
      def toErrorDS(stageName: String)(implicit convertFn: (String, T) => DataQuality, encoder: Encoder[DataQuality]): DataErrorDS =
        ds.map(convertFn(stageName, _))

      def asStringDF: DataFrame =
        ds.columns.foldLeft(ds.toDF())((casted, c) =>
          casted.withColumn(c , col(c).cast(DataTypes.StringType)))
    }

    implicit class ExtendedDataFrame(df: DataFrame) {
      def empty(implicit spark: SparkSession): DataFrame =
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], df.schema)
    }
  }
}