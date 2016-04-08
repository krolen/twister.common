package my.twister.chronicle;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import my.twister.entities.IShortProfile;
import my.twister.entities.IShortTweet;
import my.twister.utils.Constants;
import my.twister.utils.LogAware;
import my.twister.utils.Utils;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by kkulagin on 4/2/2016.
 */
public class ChronicleDataService implements LogAware {

  protected ChronicleMap<CharSequence, LongValue> name2IdMap;
  protected ChronicleMap<LongValue, LongValue> id2TimeMap;
  protected ChronicleMap<LongValue, IShortProfile> id2ProfileMap;
  protected NavigableMap<Long, ChronicleMap<LongValue, IShortTweet>> tweetsDataMaps = Collections.synchronizedNavigableMap(Maps.newTreeMap());

  private Integer name2IdMapOpenCount = 0;
  private Integer id2ProfileOpenCount = 0;
  private Integer id2TimeOpenCount = 0;

  private static volatile ChronicleDataService instance;

  public static ChronicleDataService getInstance() {
    if (instance == null) {
      synchronized (ChronicleDataService.class) {
        if (instance == null) {
          instance = new ChronicleDataService();
        }
      }
    }
    return instance;
  }

  private ChronicleDataService() {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        close();
      }
    });
  }

  public synchronized MapReference<CharSequence, LongValue> connectName2IdMap() {
    if(name2IdMap == null) {
      String location = Utils.getProperty(Constants.PROFILE_NAME_2_ID);
      File file = new File(location);
      if(!file.exists()) {
        throw new RuntimeException("File not found: " + file);
      }
      initName2IdMap(false);
    }
    name2IdMapOpenCount++;
    return new MapReference<>(name2IdMap, name2IdMapOpenCount);
  }

  public synchronized MapReference<LongValue, IShortProfile> connectId2ProfileMap() {
    if (id2ProfileMap == null) {
      String location = Utils.getProperty(Constants.PROFILE_ID_2_PROFILE);
      File file = new File(location);
      if(!file.exists()) {
        throw new RuntimeException("File not found: " + file);
      }
      initId2ProfileMap(false);
    }
    id2ProfileOpenCount++;
    return new MapReference<>(id2ProfileMap, id2ProfileOpenCount);
  }

  public synchronized MapReference<LongValue, LongValue> connectId2TimeMap() {
    if (id2TimeMap == null) {
      String location = Utils.getProperty(Constants.PROFILE_ID_2_TIME);
      File file = new File(location);
      if(!file.exists()) {
        throw new RuntimeException("File not found: " + file);
      }
      initId2TimeMap(false);
    }
    id2TimeOpenCount++;
    return new MapReference<>(id2TimeMap, id2TimeOpenCount);
  }

  public ChronicleMap<LongValue, IShortTweet> getTweetsDataMap(long time) {
    Map.Entry<Long, ChronicleMap<LongValue, IShortTweet>> entry = tweetsDataMaps.floorEntry(time);
    return entry == null ? null : entry.getValue();
  }

  public void connectTweetsMaps(int maxTweetDataMapsToConnect) {
    refreshTweetMaps(maxTweetDataMapsToConnect);

    ScheduledExecutorService service = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setDaemon(true).build());
    // this will keep only the latest maxTweetDataMapsToConnect entries in map
    service.scheduleAtFixedRate(() -> {
      refreshTweetMaps(maxTweetDataMapsToConnect);
    }, 3, 3, TimeUnit.MINUTES);
  }

  private void refreshTweetMaps(int maxTweetDataMapsToConnect) {
    File file = new File(Utils.getProperty(Constants.TWEETS_DATA_DIR));
    if (file.exists() && file.isDirectory()) {
      File[] files = file.listFiles();
      if (files == null || files.length == 0) {
        log().warn("No tweet data were found!!!");
      } else {
        Long lastKey = tweetsDataMaps.isEmpty() ? 0L : tweetsDataMaps.lastKey();
        Arrays.stream(files).map((f) -> Long.valueOf(f.getName())).
            sorted(Comparator.reverseOrder()).
            filter((l) -> l > lastKey).
            forEach((l) -> {
              if (maxTweetDataMapsToConnect > 0 && tweetsDataMaps.size() > maxTweetDataMapsToConnect) {
                Optional.ofNullable(tweetsDataMaps.firstEntry().getValue()).ifPresent(ChronicleMap::close);
              }
              createOrConnectTweetsMap(l);
            });
      }
    } else {
      throw new RuntimeException("Cannot find tweet data directory");
    }
  }

  NavigableMap<Long, ChronicleMap<LongValue, IShortTweet>> getTweetsDataMaps() {
    return tweetsDataMaps;
  }

  public void close() {
    closeAndLogException(name2IdMap);
    name2IdMap = null;
    closeAndLogException(id2TimeMap);
    id2TimeMap = null;
    closeAndLogException(id2ProfileMap);
    id2ProfileMap = null;
    tweetsDataMaps.values().stream().forEach(this::closeAndLogException);
    tweetsDataMaps.clear();
  }

  private void closeAndLogException(ChronicleMap map) {
    try {
      Optional.ofNullable(map).ifPresent(ChronicleMap::close);
    } catch (Exception e) {
      log().error("Error closing map", e);
    }
  }

  public synchronized void release(MapReference reference) {
    reference.refCount--;
    if (reference.refCount == 0) {
      closeAndLogException(reference.map);
    }
  }

  boolean removeTweetsMap(long time) {
    String location = Utils.getProperty(Constants.TWEETS_DATA_DIR) + File.separator + time;
    closeAndLogException(tweetsDataMaps.get(time));
    File file = new File(location);
    return file.exists() && file.delete();
  }

  void createOrConnectTweetsMap(long time) {
    String location = Utils.getProperty(Constants.TWEETS_DATA_DIR) + File.separator + time;
    File tweetsDataFile = Utils.getOrCreateFile(location, false);
    try {
      ChronicleMap<LongValue, IShortTweet> map = ChronicleMap.of(LongValue.class, IShortTweet.class).
          putReturnsNull(true).
          entries(System.getProperty("os.name").toLowerCase().contains("win") ? 10_000 : 25_000_000).
          createPersistedTo(tweetsDataFile);
      tweetsDataMaps.put(time, map);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    log().info("Tweet data map created at location {}", location);
  }

  void initName2IdMap(boolean forceRecreate) {
    String location = Utils.getProperty(Constants.PROFILE_NAME_2_ID);
    File name2IdFile = Utils.getOrCreateFile(location, forceRecreate);
    try {
      name2IdMap = ChronicleMap.of(CharSequence.class, LongValue.class).putReturnsNull(true).
          averageKeySize("this_is_18_charctr".length() * 4).
          entries(System.getProperty("os.name").toLowerCase().contains("win") ? 10_000 : 500_000_000).
          createPersistedTo(name2IdFile);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    log().info("ProfileName2Id Map created at location {}", location);
  }

  void initId2ProfileMap(boolean forceRecreate) {
    String location = Utils.getProperty(Constants.PROFILE_ID_2_PROFILE);
    File id2ProfileFile = Utils.getOrCreateFile(location, forceRecreate);
    try {
      id2ProfileMap = ChronicleMap.of(LongValue.class, IShortProfile.class).putReturnsNull(true).
          entries(System.getProperty("os.name").toLowerCase().contains("win") ? 10_000 : 500_000_000).
          createPersistedTo(id2ProfileFile);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    log().info("Id2Profile Map created at location {}", location);
  }

  void initId2TimeMap(boolean forceRecreate) {
    String location = Utils.getProperty(Constants.PROFILE_ID_2_TIME);
    File time2IdFile = Utils.getOrCreateFile(location, forceRecreate);
    try {
      id2TimeMap = ChronicleMap.of(LongValue.class, LongValue.class).putReturnsNull(true).
          entries(System.getProperty("os.name").toLowerCase().contains("win") ? 10_000 : 500_000_000).
          createPersistedTo(time2IdFile);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    log().info("ProfileTime2Id Map created at location {}", location);
  }




  public static class MapReference<K, V> {
    private Integer refCount;
    private ChronicleMap<K, V> map;

    public MapReference(ChronicleMap<K, V> map, Integer refCount) {
      this.map = map;
      this.refCount = refCount;
    }

    public ChronicleMap<K, V> map() {
      return map;
    }
  }

}
