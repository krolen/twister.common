package my.twister.hfttest;

import com.google.common.util.concurrent.Uninterruptibles;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by kkulagin on 4/2/2016.
 */
public class HFTTest {

//  @Test
  public void testCreateAndDeleteFiles() {
    String location = "hfttests" + File.separator + "delete";
    File file = new File(location);
    if (!file.exists()) {
      try {
        file.getParentFile().mkdirs();
        file.createNewFile();
      } catch (IOException e) {
        // fail fast
        throw new RuntimeException(e);
      }
    }
    try {
      ChronicleMap<LongValue, LongValue> map = ChronicleMap.of(LongValue.class, LongValue.class).putReturnsNull(true).
          entries(System.getProperty("os.name").toLowerCase().contains("win") ? 10_000 : 500_000).
          createPersistedTo(file);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    Uninterruptibles.sleepUninterruptibly(30, TimeUnit.SECONDS);
  }
}
