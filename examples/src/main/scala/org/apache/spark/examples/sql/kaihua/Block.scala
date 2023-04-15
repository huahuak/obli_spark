package org.apache.spark.examples.sql.kaihua

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.unsafe.types.UTF8String
import org.kaihua.obliop.collection.FbsVector
import org.kaihua.obliop.collection.fbs._

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
    val writer =
      new UnsafeRowWriter(row.fieldsLength(), row.fieldsLength() * 32)
    for (i <- 0 until row.fieldsLength()) {
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
    curRow += 1
    writer.getRow
  }
}

object BlockInfo {
  def merge(lhs: BlockInfo, rhs: BlockInfo): BlockInfo = {
    val mergedBlock = new BlockInfo(lhs.attrs)
    Seq(lhs, rhs).foreach(block => {
      val iter = new blockIter(block)
      iter.foreach(ele => {
        mergedBlock.write(ele)
      })
    })
    mergedBlock
  }

  def divideInto2Blocks(
      source: BlockInfo,
      lhs: BlockInfo,
      rhs: BlockInfo
  ): Unit = {
    lhs.reset()
    rhs.reset()
    new blockIter(source).foreach(ele => {
      // write the smaller to lhs,
      // if lhs has no space more,
      // then write the bigger to rhs
      if (!lhs.write(ele)) {
        rhs.write(ele)
      }
    })
  }
}

class BlockInfo(val attrs: Seq[Attribute]) {
  private val cap = math.pow(10, 4).toInt
  private var len: Int = 0
  private var bytebuf: ByteBuffer = null
  private var fbs = FbsVector.createVec()
  def write(internalRow: InternalRow): Boolean = {
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
    this.bytebuf = fbs.finish()
  }

  def drop(): Unit = {
    fbs.clearBuilder()
    this.bytebuf = null
  }

  /** renew fbs and clean bytebuf
    */
  def reset(): Unit = {
    drop()
    fbs = FbsVector.createVec()
  }

  def getBytBuf(): ByteBuffer = {
    this.bytebuf
  }

  /** first drop old fbs and clean old bytebuf, then renew fbs and set bytebuf
    */
  def setBytBuf(bytBuf: ByteBuffer): Unit = {
    drop()
    this.bytebuf = bytBuf
  }
}
