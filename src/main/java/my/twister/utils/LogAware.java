package my.twister.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kkulagin
 * @since 24.10.2015
 */
public interface LogAware {

  default Logger log() {
    return LoggerFactory.getLogger(getClass());
  }
}
