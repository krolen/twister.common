package my.twister.entities;

import net.openhft.chronicle.values.Range;

/**
 * @author kkulagin
 * @since 26.10.2015
 */
public interface IShortProfile {

  int getAuthority();

  void setAuthority(@Range(min = 0, max = 10) int authority);

  boolean isVerified();

  void setVerified(boolean verified);

  int getFollowersCount();

  void setFollowersCount(@Range(min = 0, max = 100_000_000) int followersCount);

  int getFriendsCount();

  void setFriendsCount(@Range(min = 0, max = 100_000_000) int friendsCount);

  int getPostCount();

  void setPostCount(int postCount);

  long getModifiedTime();

  void setModifiedTime(long time);

}
