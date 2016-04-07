package my.twister.chronicle;

import my.twister.entities.IShortTweet;

import java.io.Serializable;

/**
 * Created by kkulagin on 4/7/2016.
 */
class SampleShortTweet implements IShortTweet, Serializable {

//  public long id = System.currentTimeMillis();
  public long createDate = System.currentTimeMillis();
  public long authorId = System.currentTimeMillis();
  public long[] mentions = new long[]{1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L};

//  @Override
//  public long getId() {
//    return id;
//  }
//
//  @Override
//  public void setId(long id) {
//    this.id = id;
//  }

  @Override
  public long getCreateDate() {
    return createDate;
  }

  @Override
  public void setCreateDate(long createDate) {
    this.createDate = createDate;
  }

  @Override
  public long getAuthorId() {
    return authorId;
  }

  @Override
  public void setAuthorId(long authorId) {
    this.authorId = authorId;
  }

  @Override
  public long[] getMentions() {
    return mentions;
  }

  @Override
  public void setMentions(long[] mentions) {
    this.mentions = mentions;
  }
}
