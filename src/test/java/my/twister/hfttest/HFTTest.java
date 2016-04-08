package my.twister.hfttest;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.Uninterruptibles;
import my.twister.utils.Constants;
import my.twister.utils.Utils;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.VanillaBytes;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.values.Array;
import net.openhft.chronicle.values.Values;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

/**
 * Created by kkulagin on 4/2/2016.
 */
public class HFTTest {

//  @Test
  public void testCreateAndDeleteFiles() {
    File file = Utils.createFile("hfttests" + File.separator + "delete", true);
    try {
      ChronicleMap<LongValue, LongValue> map = ChronicleMap.of(LongValue.class, LongValue.class).putReturnsNull(true).
          entries(System.getProperty("os.name").toLowerCase().contains("win") ? 10_000 : 500_000).
          createPersistedTo(file);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    Uninterruptibles.sleepUninterruptibly(30, TimeUnit.SECONDS);
  }

//  @Test
  public void testQueue() {
    ChronicleQueue queue = null;
    try {

      queue = ChronicleQueueBuilder.single("hfttests" + File.separator + "deletequeue").build();
//      queue = SingleChronicleQueueBuilder.binary(new File("hfttests" + File.separator + "deletequeue")).build();
      final VanillaBytes<Void> writeBytes = Bytes.allocateDirect(8);
      VanillaBytes<Void> readBytes = Bytes.allocateDirect(8);
      final ExcerptAppender appender = queue.createAppender();
      ExcerptTailer tailer = queue.createTailer();

//      Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().build()).execute(() -> {
      Stopwatch started = Stopwatch.createStarted();
      LongStream.range(0, 5_00).forEach((l) -> {
//      LongStream.range(0, 5_000_000).forEach((l) -> {
          appender.writeBytes(writeBytes.append(l));
          writeBytes.clear();
        });
      System.out.println(started.elapsed(TimeUnit.NANOSECONDS));
//      });
      started.reset();

      Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
      int count = 0;
      started.start();
      while(tailer.readBytes(readBytes)) {
        long l = readBytes.parseLong();
        count++;
//        System.out.println(l);
        readBytes.clear();
      }
      System.out.println(started.elapsed(TimeUnit.NANOSECONDS));
      System.out.println("count = " + count);

    } finally {
      Optional.ofNullable(queue).ifPresent((chronicleQueue) -> {
        try {
          chronicleQueue.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    }

  }

//  @org.junit.Test
  public void testArrays() {
    File file = Utils.createFile("hfttests" + File.separator + "delete", true);
    ChronicleMap<LongValue, ITest> map = null;
    try {
      try {
        map = ChronicleMap.of(LongValue.class, ITest.class).putReturnsNull(true).
            entries(10).
            createPersistedTo(file);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      ITest iTest = Values.newHeapInstance(ITest.class);
      LongValue key = Values.newHeapInstance(LongValue.class);

      for (long i = 0; i < 20; i++) {
        iTest.setMentionAt(0, 1);
        iTest.setMentionAt(1, 2);
        iTest.setMentionAt(2, 3);
        iTest.setData(i + i);
        key.setValue(i);
        map.put(key, iTest);
      }

      ITest readTest = Values.newHeapInstance(ITest.class);

      final ChronicleMap<LongValue, ITest> finalMap = map;
      map.keySet().stream().forEach((l) -> {
        ITest read = finalMap.getUsing(l, readTest);
        System.out.println(read);
      });
    } finally {
      Optional.ofNullable(map).ifPresent(ChronicleMap::close);
    }


  }

  public interface ITest {

    long getData();

    void setData(long data);

    @Array(length = Constants.MAX_MENTIONS_SIZE)
    long getMentionAt(int index);

    void setMentionAt(int index, long value);

  }


}
