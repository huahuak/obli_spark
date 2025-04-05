package org.kaihua.obliop.collection;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;

public class ArrowVector {
  public void init() {
    BufferAllocator allocator = new RootAllocator();
    ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
    ArrowArray arrowArray = ArrowArray.allocateNew(allocator);
    long arrayAddr = arrowArray.memoryAddress();
    long schemaAddr = arrowSchema.memoryAddress();
    IntVector intVector = new IntVector("int-to-export", allocator);
    intVector.allocateNew(3);
    intVector.setSafe(0, 1);
    intVector.setSafe(1, 2);
    intVector.setSafe(2, 3);
    intVector.setValueCount(3);
    System.out.println("[Java] FieldVector: \n" + intVector);
    // Data.exportVector(allocator, intVector, null, ArrowArray.wrap(arrayAddr), ArrowSchema.wrap(schemaAddr));
    // ObliOp.ObliDataSend(new ObliData(0, arrayAddr, schemaAddr));
    allocator.close();
    arrowSchema.close();
    arrowArray.close();
  }

}
