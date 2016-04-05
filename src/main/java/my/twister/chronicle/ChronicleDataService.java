package my.twister.chronicle;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import my.twister.entities.IShortProfile;
import my.twister.entities.IShortTweet;
import my.twister.utils.Constants;
import my.twister.utils.LogAware;
import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.ChronicleMap;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Created by kkulagin on 4/2/2016.
 */
public abstract class ChronicleDataService implements LogAware {

  public abstract ChronicleMap<CharSequence, LongValue> getName2IdMap();

  public abstract ChronicleMap<LongValue, IShortProfile> getId2ProfileMap();

  public abstract ChronicleMap<LongValue, LongValue> getId2TimeMap();

  public abstract ChronicleMap<LongValue, IShortTweet> getTweetsDataMap(long tweetId);

  public abstract void connect(int maxTweetDataMapsToConnect);

  public void close() {
    Optional.ofNullable(getId2ProfileMap()).ifPresent(ChronicleMap::close);
    Optional.ofNullable(getId2TimeMap()).ifPresent(ChronicleMap::close);
    Optional.ofNullable(getName2IdMap()).ifPresent(ChronicleMap::close);
  }

  private static volatile ChronicleDataService instance;

  public static ChronicleDataService getInstance() {
    if (instance == null) {
      synchronized (ChronicleDataService.class) {
        if (instance == null) {
          instance = new DefaultChronicleDataService();
        }
      }
    }
    return instance;
  }


  static class DefaultChronicleDataService extends ChronicleDataService {

    private ChronicleMap<CharSequence, LongValue> name2IdMap;
    private ChronicleMap<LongValue, LongValue> id2TimeMap;
    private ChronicleMap<LongValue, IShortProfile> id2ProfileMap;
    private NavigableMap<Long, ChronicleMap<LongValue, IShortTweet>> tweetsDataMaps =
        Collections.synchronizedNavigableMap(Maps.newTreeMap());

    private DefaultChronicleDataService() {
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          DefaultChronicleDataService.this.close();
        }
      });

//    name2IdFileLocation = (String) stormConf.get("profile.id.to.profile.name2IdFile");
//    name2IdFile = new File(name2IdFileLocation);
//    ChronicleMapBuilder<Long, IShortProfile> builder =
//      ChronicleMapBuilder.of(Long.class, IShortProfile.class).
//        constantValueSizeBySample(new ShortProfile()).
//        entries(400_000_000);
//    try {
//      id2ProfileMap = builder.createPersistedTo(name2IdFile);
//    } catch (IOException e) {
//      // fail fast
//      throw new RuntimeException(e);
//    }

    }

    @Override
    public void connect(int maxTweetDataMapsToConnect) {
      createProfileId2TimeMap(false);
      createProfileName2IdMap(false);

      ScheduledExecutorService service = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setDaemon(true).build());
      // this will keep only the latest maxTweetDataMapsToConnect entries in map
      service.scheduleAtFixedRate(() -> {
        File file = new File(getProperty(Constants.TWEETS_DATA_DIR));
        if (file.exists() && file.isDirectory()) {
          File[] array = file.listFiles();
          if(array == null || array.length == 0) {
            log().warn("No tweet data were found!!!");
          } else {
            Long lastKey = tweetsDataMaps.lastKey();
            Arrays.stream(array).map((f) -> Long.valueOf(f.getName())).
                sorted().filter((l) -> l > lastKey).
                forEach((l) -> {
                  if (maxTweetDataMapsToConnect > 0 && tweetsDataMaps.size() > maxTweetDataMapsToConnect) {
                    Optional.ofNullable(tweetsDataMaps.firstEntry().getValue()).ifPresent(ChronicleMap::close);
                  }
                  createTweetsMap(l, false);
                });
          }
        } else {
          throw new RuntimeException("Cannot find tweet data directory");
        }
      }, 1, 3, TimeUnit.MINUTES);
    }

    @Override
    // this method is a bit of mess
    public ChronicleMap<LongValue, IShortTweet> getTweetsDataMap(long tweetId) {
      Map.Entry<Long, ChronicleMap<LongValue, IShortTweet>> entry = tweetsDataMaps.floorEntry(tweetId);
      return entry == null ? null : entry.getValue();
    }

    @Override
    public void close() {
      super.close();
      tweetsDataMaps.values().stream().forEach(ChronicleMap::close);
    }

    NavigableMap<Long, ChronicleMap<LongValue, IShortTweet>> getTweetsDataMaps() {
      return tweetsDataMaps;
    }

    boolean removeTweetsMap(long id) {
      String location = getProperty(Constants.TWEETS_DATA_DIR) + File.separator + id;
      Optional.ofNullable(tweetsDataMaps.get(id)).ifPresent(ChronicleMap::close);
      File file = new File(location);
      return file.exists() && file.delete();
    }

    void createTweetsMap(long id, boolean forceRecreate) {
      String location = getProperty(Constants.TWEETS_DATA_DIR) + File.separator + id;
      File tweetsDataFile = createFile(location, forceRecreate);
      try {
        ChronicleMap<LongValue, IShortTweet> map = ChronicleMap.of(LongValue.class, IShortTweet.class).putReturnsNull(true).
            constantValueSizeBySample(new IShortTweet() {
              @Override
              public long getId() {
                return System.currentTimeMillis();
              }

              @Override
              public void setId(long id) {
              }

              @Override
              public long getCreateDate() {
                return System.currentTimeMillis();
              }

              @Override
              public void setCreateDate(long createDate) {
              }

              @Override
              public long getAuthorId() {
                return System.currentTimeMillis();
              }

              @Override
              public void setAuthorId(long authorId) {
              }

              @Override
              public long[] getMentions() {
                return new long[10];
              }

              @Override
              public void setMentions(long[] mentions) {
              }
            }).
            entries(System.getProperty("os.name").toLowerCase().contains("win") ? 10_000 : 25_000_000).
            createPersistedTo(tweetsDataFile);
        tweetsDataMaps.put(id, map);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      log().info("Tweet data map created at location {}", location);
    }

    void createProfileName2IdMap(boolean forceRecreate) {
      String location = getProperty(Constants.PROFILE_NAME_2_ID);
      File name2IdFile = createFile(location, forceRecreate);
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

    void createProfileId2TimeMap(boolean forceRecreate) {
      String location = getProperty(Constants.PROFILE_ID_2_TIME);
      File time2IdFile = createFile(location, forceRecreate);
      try {
        id2TimeMap = ChronicleMap.of(LongValue.class, LongValue.class).putReturnsNull(true).
            entries(System.getProperty("os.name").toLowerCase().contains("win") ? 10_000 : 500_000_000).
            createPersistedTo(time2IdFile);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      log().info("ProfileTime2Id Map created at location {}", location);
    }

    private static String getProperty(String name) {
      final Properties properties = new Properties();
      try (InputStream stream = DefaultChronicleDataService.class.getResourceAsStream("/hft.properties")) {
        properties.load(stream);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return Optional.ofNullable(properties.getProperty(name)).
          orElseThrow(() -> new RuntimeException("Property " + name + " was not found"));
    }

    @NotNull
    private static File createFile(String fileLocation, boolean forceRecreate) {
      File file = new File(fileLocation);
      if (file.exists() && forceRecreate) {
        file.delete();
      }
      if (!file.exists()) {
        try {
          file.getParentFile().mkdirs();
          file.createNewFile();
        } catch (IOException e) {
          // fail fast
          throw new RuntimeException(e);
        }
      }
      return file;
    }

    @Override
    public ChronicleMap<LongValue, LongValue> getId2TimeMap() {
      return id2TimeMap;
    }

    @Override
    public ChronicleMap<CharSequence, LongValue> getName2IdMap() {
      return name2IdMap;
    }

    @Override
    public ChronicleMap<LongValue, IShortProfile> getId2ProfileMap() {
      return id2ProfileMap;
    }
  }

}
