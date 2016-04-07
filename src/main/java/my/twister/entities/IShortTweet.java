package my.twister.entities;

/**
 * Created by kkulagin on 4/2/2016.
 */
public interface IShortTweet {

//  long getId();
//
//  void setId(long id);

  long getCreateDate();

  void setCreateDate(long createDate);

  long getAuthorId();

  void setAuthorId(long authorId);

  long[] getMentions();

  void setMentions(long[] mentions);
}
