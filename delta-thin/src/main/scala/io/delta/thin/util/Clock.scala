package io.delta.thin.util


/**
 * An interface to represent clocks, so that they can be mocked out in unit tests.
 */
private[thin] trait Clock {
  def getTimeMillis(): Long
  def waitTillTime(targetTime: Long): Long
}

/**
 * A clock backed by the actual time from the OS as reported by the `System` API.
 */
private[thin] class SystemClock extends Clock {

  val minPollTime = 25L

  /**
   * @return the same time (milliseconds since the epoch)
   *         as is reported by `System.currentTimeMillis()`
   */
  def getTimeMillis(): Long = System.currentTimeMillis()

  /**
   * @param targetTime block until the current time is at least this value
   * @return current system time when wait has completed
   */
  def waitTillTime(targetTime: Long): Long = {
    var currentTime = 0L
    currentTime = System.currentTimeMillis()

    var waitTime = targetTime - currentTime
    if (waitTime <= 0) {
      return currentTime
    }

    val pollTime = math.max(waitTime / 10.0, minPollTime).toLong

    while (true) {
      currentTime = System.currentTimeMillis()
      waitTime = targetTime - currentTime
      if (waitTime <= 0) {
        return currentTime
      }
      val sleepTime = math.min(waitTime, pollTime)
      Thread.sleep(sleepTime)
    }
    -1
  }
}
