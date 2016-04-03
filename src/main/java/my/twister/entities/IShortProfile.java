package my.twister.entities;

/**
 * @author kkulagin
 * @since 26.10.2015
 */
public interface IShortProfile {
  byte getAuthority();

  boolean isVerified();

  int getFollowersCount();

  int getFriendsCount();

  int getPostCount();

  long getModifiedTime();

  void setVerified(boolean verified);

  void setAuthority(byte authority);

  void setFollowersCount(int followersCount);

  void setFriendsCount(int friendsCount);

  void setPostCount(int postCount);

  void setModifiedTime(long time);

}
