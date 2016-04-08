package my.twister.entities;

import my.twister.utils.Constants;
import net.openhft.chronicle.values.Array;

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


  @Array(length = Constants.MAX_MENTIONS_SIZE)
  long getMentionAt(int index);

  void setMentionsAt(int index, long value);

}
