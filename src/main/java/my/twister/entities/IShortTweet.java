package my.twister.entities;

/**
 * Created by kkulagin on 4/2/2016.
 */
public interface IShortTweet {

  public long getId();

  public void setId(long id);

  public long getCreateDate();

  public void setCreateDate(long createDate);

  public long getAuthorId();

  public void setAuthorId(long authorId);

  public long[] getMentions();

  public void setMentions(long[] mentions);
}
