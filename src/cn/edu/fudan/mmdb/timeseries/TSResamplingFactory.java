package cn.edu.fudan.mmdb.timeseries;

/**
 * Timeseries manipulation factory.
 * 
 * @author psenin
 * 
 */
public final class TSResamplingFactory {

  /**
   * Performs a check for timeseries regularity. A time series can either be irregular (unequally
   * spaced) or regular (equally spaced).
   * 
   * @param ts The timeseries to check.
   * @return true if the series is regular.
   */
  public static boolean isRegular(Timeseries ts) {
    return false;
  }

  /**
   * Align timeseries values with provided timestamps without interpolation. It will just update the
   * timestamps.
   * 
   * @param ts The timeseries to update.
   * @param newTimestamps The newtimestamps array.
   * @return Updated series.
   */
  public static Timeseries align(Timeseries ts, long[] newTimestamps) {
    return null;
  }

}
