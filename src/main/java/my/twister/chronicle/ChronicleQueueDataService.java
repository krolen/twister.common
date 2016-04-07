package my.twister.chronicle;

import my.twister.utils.Constants;
import my.twister.utils.LogAware;
import my.twister.utils.Utils;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import org.jetbrains.annotations.NotNull;

import java.io.File;

/**
 * Created by kkulagin on 4/2/2016.
 */
public abstract class ChronicleQueueDataService implements LogAware {

  public abstract ChronicleQueue createQueue(String id);

  public abstract boolean deleteQueue(String id);


  private static volatile ChronicleQueueDataService instance;

  public static ChronicleQueueDataService getInstance() {
    if (instance == null) {
      synchronized (ChronicleQueueDataService.class) {
        if (instance == null) {
          instance = new DefaultChronicleQueueDataService();
        }
      }
    }
    return instance;
  }


  static class DefaultChronicleQueueDataService extends ChronicleQueueDataService {

    @Override
    public ChronicleQueue createQueue(String id) {
      String location = getQueueLocation(id);
      return ChronicleQueueBuilder.single(location).build();
    }

    @Override
    public boolean deleteQueue(String id) {
      String location = getQueueLocation(id);
      File file = new File(location);
      return file.exists() && file.delete();
    }

    @NotNull
    private String getQueueLocation(String id) {
      return Utils.getProperty(Constants.QUEUES_DATA_DIR) + File.separator + id;
    }
  }

}
