package sssj.io;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import sssj.time.Timeline;

public class BinaryVectorStreamReaderTest {
  private static final String vw_example = "1 |f 13:3.9656971e-02 24:3.4781646e-02 69:4.6296168e-02 85:6.1853945e-02 140:3.2349996e-02 156:1.0290844e-01 175:6.8493910e-02 188:2.8366476e-02 229:7.4871540e-02 230:9.1505975e-02 234:5.4200061e-02 236:4.4855952e-02 238:5.3422898e-02 387:1.4059304e-01 394:7.5131744e-02 433:1.1118756e-01 434:1.2540409e-01 438:6.5452829e-02 465:2.2644201e-01 468:8.5926279e-02 518:1.0214076e-01 534:9.4191484e-02 613:7.0990764e-02 646:8.7701865e-02 660:7.2289191e-02 709:9.0660661e-02 752:1.0580081e-01 757:6.7965068e-02 812:2.2685185e-01 932:6.8250686e-02 1028:4.8203137e-02 1122:1.2381379e-01 1160:1.3038123e-01 1189:7.1542501e-02 1530:9.2655659e-02 1664:6.5160148e-02 1865:8.5823394e-02 2524:1.6407280e-01 2525:1.1528353e-01 2526:9.7131468e-02 2536:5.7415009e-01 2543:1.4978983e-01 2848:1.0446861e-01 3370:9.2423186e-02 3960:1.5554591e-01 7052:1.2632671e-01 16893:1.9762035e-01 24036:3.2674628e-01 24303:2.2660980e-01";
  private static final String EXAMPLE_FILENAME = "/example.bin";

  @Test
  public void test() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    out.writeLong(1); // writing one vector
    Vector v = new Vector(111);
    v.put(1, 1.0);
    v.put(2, 2.0);
    v.put(3, 3.0);
    v.write(out);
    BinaryVectorStreamReader reader = new BinaryVectorStreamReader(ByteBuffer.wrap(baos.toByteArray()));
    Iterator<Vector> it = reader.iterator();
    assertTrue(it.hasNext());
    Vector r = it.next();
    assertFalse(it.hasNext()); // only one vector
    assertEquals(v, r); // recovered v
  }

  @Test
  public void testOnData() throws FileNotFoundException, IOException {
    File file = new File(this.getClass().getResource(EXAMPLE_FILENAME).getPath());
    VectorStream reader = new BinaryVectorStreamReader(file);
    assertEquals(804414, reader.numVectors());
    Vector v = reader.iterator().next();
    Vector r = Vector.l2normalize(Format.VW.getRecordParser().apply(vw_example));
    assertEquals(r, v);
  }

  @Test
  public void testSpeed() throws IOException {
    File file;
    VectorStream reader;
    int nnz;
    long start, finish;

    file = new File("data/RCV1-seq.bin");
    reader = new BinaryVectorStreamReader(file);
    nnz = 0;
    start = System.currentTimeMillis();
    for (Vector v : reader) {
      nnz += v.size();
    }
    finish = System.currentTimeMillis();
    System.out.println("nnz=" + nnz);
    System.out.println("BINARY READER - Total time taken: " + TimeUnit.MILLISECONDS.toMillis(finish - start) + " ms.");

    file = new File("data/RCV1.vw");
    reader = new VectorStreamReader(file, Format.VW, new Timeline.Sequential());
    start = System.currentTimeMillis();
    for (Vector v : reader) {
      nnz -= v.size();
    }
    assertEquals(0, nnz);
    finish = System.currentTimeMillis();
    System.out.println("nnz=" + nnz);
    System.out.println("TEXT READER - Total time taken: " + TimeUnit.MILLISECONDS.toMillis(finish - start) + " ms.");
  }

  // @Test
  @SuppressWarnings("resource")
  public void testBB() throws IOException {
    File file = new File(this.getClass().getResource(EXAMPLE_FILENAME).getPath());
    ByteBuffer bb = ByteBuffer.allocate(8);
    FileChannel channel = new FileInputStream(file).getChannel();
    System.out.println("read bytes=" + channel.read(bb));
    bb.flip();
    System.out.println("read value=" + bb.getLong() + " remaining bytes=" + bb.remaining());
    refill(bb, channel);
    bb.mark();
    System.out.println("read value=" + bb.getInt() + " remaining bytes=" + bb.remaining());
    bb.reset();
    System.out.println("read value=" + bb.getInt() + " remaining bytes=" + bb.remaining());
    refill(bb, channel);
    System.out.println("read value=" + bb.getLong() + " remaining bytes=" + bb.remaining());
    refill(bb, channel);
    System.out.println("read value=" + bb.getInt() + " remaining bytes=" + bb.remaining());
  }

  private static void refill(ByteBuffer bb, FileChannel channel) throws IOException {
    bb.compact();
    System.out.println("read bytes=" + channel.read(bb));
    bb.flip();
  }
}
