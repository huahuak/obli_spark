package org.apache.spark.examples.sql.kaihua

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.unsafe.types.UTF8String
import org.kaihua.obliop.collection.FbsVector
import org.kaihua.obliop.collection.fbs.{DoubleValue, FieldUnion, IntValue, RowTable, StringValue}

import java.nio.ByteBuffer
import scala.jdk.CollectionConverters.seqAsJavaListConverter

class blockIter(block: BlockInfo) extends Iterator[InternalRow] {
  val rowTable = RowTable.getRootAsRowTable(block.getBytBuf())
  var curRow = 0
  val rowSize = rowTable.rowsLength()

  override def hasNext: Boolean = {
    if (curRow < rowSize) {
      return true
    }
    false
  }

  override def next(): InternalRow = {
    assert(curRow < rowSize)
    val row = rowTable.rows(curRow);
    curRow += 1
    val writer =
      new UnsafeRowWriter(row.fieldsLength(), row.fieldsLength() * 32)
    for (i <- 0 to row.fieldsLength()) {
      val fieldObj = row.fields(i)
      fieldObj.valueType match {
        case FieldUnion.IntValue =>
          val valueObj =
            fieldObj.value(new IntValue).asInstanceOf[IntValue]
          writer.write(i, valueObj.value())
        case FieldUnion.DoubleValue =>
          val valueObj =
            fieldObj.value(new DoubleValue).asInstanceOf[DoubleValue]
          writer.write(i, valueObj.value())
        case FieldUnion.StringValue =>
          val valueObj =
            fieldObj.value(new StringValue).asInstanceOf[StringValue]
          writer.write(i, UTF8String.fromString(valueObj.value()))
        case _ =>
          throw new Exception("[ObliviousSort] no this type")

      }
    }
    writer.getRow
  }
}

class BlockInfo(fbs: FbsVector) {
  private val cap = math.pow(10, 4).toInt
  private var len: Int = 0
  private var bytebuf: ByteBuffer = null
  def write(internalRow: InternalRow, attrs: Seq[Attribute]): Boolean = {
    if (len >= cap) {
      finish()
      return false
    }
    // get row
    val row = attrs.zipWithIndex
      .map(tuple => {
        val (attr, index) = tuple
        var field = internalRow.get(index, attr.dataType)
        field match {
          case string: UTF8String =>
            field = string.toString
          case _ =>
        }
        FbsVector.createCell(field, attr.dataType.toString)
      })
      .toList
    fbs.append(row.asJava)
    len += 1
    true
  }

  def finish(): Unit = {
    this.bytebuf = fbs.finishAndClear()
  }

  def getBytBuf(): ByteBuffer = {
    this.bytebuf
  }

  def setBytBuf(bytBuf: ByteBuffer): Unit = {
    this.bytebuf = bytBuf
  }
}
