package org.kaihua.obliop.collection;

import com.google.flatbuffers.FlatBufferBuilder;

import org.kaihua.obliop.Config;
import org.kaihua.obliop.collection.fbs.*;
import org.kaihua.obliop.data.ObliData;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author kahua.li
 * @email moflowerlkh@gmail.com
 * @date 2023/01/12
 **/
public class FbsVector {
  private static int blockSize = Config.blockSize;

  private ByteBuffer directBuffer;
  private FlatBufferBuilder builder;
  private int offset;
  private List<Integer> rowOffset;

  FbsVector() {
    offset = 0;
    rowOffset = new ArrayList<>(0);
    directBuffer = ByteBuffer.allocateDirect(blockSize);
    builder = new FlatBufferBuilder(directBuffer);
  }

  public static ByteBuffer test() {
    ByteBuffer directBuf = ByteBuffer.allocateDirect(1024);
    FlatBufferBuilder builder = new FlatBufferBuilder(directBuf);
    int strOffset = builder.createString("hello world! here is java test");
    int strValueOffset = StringValue.createStringValue(builder, strOffset);
    int fieldOffset = Field.createField(builder, FieldUnion.StringValue, strValueOffset, false);
    int fieldVecOffset = Row.createFieldsVector(builder, new int[]{fieldOffset});
    int rowOffset = Row.createRow(builder, fieldVecOffset);
    int rowVecOffset = RowTable.createRowsVector(builder, new int[]{rowOffset});
    int rowsOffset = RowTable.createRowTable(builder, rowVecOffset);
    builder.finish(rowsOffset);
    ByteBuffer buffer = builder.dataBuffer();
    printFbs(buffer);
    return buffer;
  }

  public static FbsVector createVec() {
    return new FbsVector();
  }

  public static class Cell {
    Object value;
    String clz;

    public boolean isNull() {
      return value == null;
    }
  }

  public static Cell createCell(Object value, String clz) {
    Cell cell = new Cell();
    cell.value = value;
    cell.clz = clz;
    return cell;
  }

  public void append(List<Cell> record) {
    List<Integer> fieldOffset = new ArrayList<>(0);
    record.forEach(r -> {
      switch (r.clz) {
        case "StringType":
          offset = builder.createString((String) r.value);
          offset = StringValue.createStringValue(builder, offset);
          offset = Field.createField(builder, FieldUnion.StringValue, offset, r.isNull());
          fieldOffset.add(offset);
          break;
        case "IntegerType":
          offset = IntValue.createIntValue(builder, (Integer) r.value);
          offset = Field.createField(builder, FieldUnion.IntValue, offset, r.isNull());
          fieldOffset.add(offset);
          break;
        case "LongType":
          offset = IntValue.createIntValue(builder, ((Long) r.value).intValue());
          offset = Field.createField(builder, FieldUnion.IntValue, offset, r.isNull());
          fieldOffset.add(offset);
          break;
        default:
          throw new RuntimeException("[FbsVector.java::append()] the type " + r.clz + " is unsupported!");
      }
    });
    int[] arr = new int[fieldOffset.size()];
    for (int i = 0; i < arr.length; i++) {
      arr[i] = fieldOffset.get(i);
    }
    offset = Row.createFieldsVector(builder, arr);
    offset = Row.createRow(builder, offset);
    rowOffset.add(offset);
  }

  public ByteBuffer finish() {
    int[] arr = new int[rowOffset.size()];
    for (int i = 0; i < arr.length; i++) {
      arr[i] = rowOffset.get(i);
    }
    offset = RowTable.createRowsVector(builder, arr);
    offset = RowTable.createRowTable(builder, offset);
    builder.finish(offset);
    ByteBuffer bytbuf = builder.dataBuffer();
    return bytbuf;
  }

  public void clearBuilder() {
    builder.clear();
  }

  public static ObliData toObliData(ByteBuffer buf) {
    return new ObliData(
        ((DirectBuffer) buf).address() + buf.position(),
        buf.capacity() - buf.position());
  }

  public static void printFbs(ByteBuffer buffer) {
    System.out.println("[FbsVector.java] printFbs()");
    RowTable rowsObj = RowTable.getRootAsRowTable(buffer);
    for (int i = 0; i < rowsObj.rowsLength(); i++) {
      Row rowObj = rowsObj.rows(i);
      for (int j = 0; j < rowObj.fieldsLength(); j++) {
        Field fieldObj = rowObj.fields(j);
        switch (fieldObj.valueType()) {
          case FieldUnion.IntValue: {
            IntValue valueObj = (IntValue) fieldObj.value(new IntValue());
            System.out.print(valueObj.value() + " | ");
          }
          break;
          case FieldUnion.DoubleValue: {
            DoubleValue valueObj = (DoubleValue) fieldObj.value(new DoubleValue());
            System.out.print(valueObj.value() + " | ");
          }
          break;
          case FieldUnion.StringValue: {
            StringValue valueObj = (StringValue) fieldObj.value(new StringValue());
            System.out.print(valueObj.value() + " | ");
          }
          break;
          default:
            break;
        }
      }
      System.out.println();
    }
  }

}
