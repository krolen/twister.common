package my.twister.utils;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * Created by kkulagin on 5/15/2015.
 */
public class Utils {

  private static final String twitter = "EEE MMM dd HH:mm:ss ZZZZ yyyy";
  private static final SimpleDateFormat sf = new SimpleDateFormat(twitter, Locale.ENGLISH);
  public static final long MILLIS_PER_HOUR = Duration.ofHours(1).toMillis();
  private static final NavigableSet<Long> hoursStartsMillis = new TreeSet<>();

  static {
    long hourStart = LocalDateTime.now().minusHours(4).
        truncatedTo(ChronoUnit.HOURS).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    // TODO: 4/12/2016 make it normal
    for (int j = 0; j < 10000; j++) {
      hoursStartsMillis.add(hourStart + j * MILLIS_PER_HOUR);
    }
  }


  public static Date getTwitterDate(String date) throws ParseException {
    sf.setLenient(true);
    return sf.parse(date);
  }

  public static void main(String[] args) throws ParseException {
    String date = "Mon Dec 03 16:17:46 +0000 2012";
    Date twitterDate = getTwitterDate(date);
    System.out.println(twitterDate);
  }

  public static int getIntervalsNumberSinceDayStart(Instant instant, long intervalDuration) {
    return getIntervalsNumberSinceDayStart(LocalDateTime.ofInstant(instant, ZoneId.systemDefault()), intervalDuration);
  }

  public static int getIntervalsNumberSinceDayStart(LocalDateTime dateTime, long intervalDuration) {
    LocalDateTime dayStart = dateTime.with(LocalTime.MIN);
    long sinceDayStart = Duration.between(dayStart, dateTime).toMillis();
    return (int) (sinceDayStart / intervalDuration);
  }

  public static LocalDateTime getDayStart(Instant instant) {
    LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
    LocalDateTime dayStart = dateTime.with(LocalTime.MIN);
    return dayStart;
  }

  public static long toMillis(LocalDateTime dateTime) {
    return dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
  }

  public static LocalDateTime toHourMinute(Instant instant, int hourMinute) {
    LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
    if (dateTime.getMinute() < hourMinute) {
      dateTime = dateTime.minus(1, ChronoUnit.HOURS);
    }
    dateTime = dateTime.truncatedTo(ChronoUnit.MINUTES).plusMinutes(hourMinute);
    return dateTime;
  }

  public static long getClosestHourStart(long time) {
    return hoursStartsMillis.floor(time);
  }

  @NotNull
  public static File getOrCreateFile(String fileLocation, boolean forceRecreate) {
    File file = new File(fileLocation);
    if (forceRecreate && file.exists()) {
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


  public static String getProperty(String name) {
    final Properties properties = new Properties();
    try (InputStream stream = Utils.class.getResourceAsStream("/hft.properties")) {
      properties.load(stream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return Optional.ofNullable(properties.getProperty(name)).
        orElseThrow(() -> new RuntimeException("Property " + name + " was not found"));
  }
}
