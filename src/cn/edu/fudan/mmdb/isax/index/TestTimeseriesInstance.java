package cn.edu.fudan.mmdb.isax.index;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

import cn.edu.fudan.mmdb.timeseries.TPoint;
import cn.edu.fudan.mmdb.timeseries.Timeseries;

public class TestTimeseriesInstance {

  @Test
  public void testAddOccurences() {

//    double[] ts = { 1.0, -0.5, 0.25, 0.0, 0.25, 0.50, 0.75, 0.0 };
    Timeseries ts = new Timeseries();
    ts.add(new TPoint(1.0D, 0L));
    ts.add(new TPoint(-0.5D, 1L));
    ts.add(new TPoint(0.25D, 2L));
    ts.add(new TPoint(0.0D, 3L));
    ts.add(new TPoint(0.25D, 4L));
    ts.add(new TPoint(0.5D, 5L));
    ts.add(new TPoint(0.75D, 6L));
    ts.add(new TPoint(0.0D, 7L));
    TimeseriesInstance tsi = new TimeseriesInstance(ts);

    tsi.AddOccurence("foo.txt", 20);
    tsi.AddOccurence("foo.txt", 30);

    assertEquals("occurences test", tsi.getOccurences().size(), 2);

//    double[] ts2 = { 1.0, -0.5, 0.25, 0.0, 0.25, 0.50, 0.75, 0.0 };
    Timeseries ts2 = new Timeseries();
    ts2.add(new TPoint(1.0D, 0L));
    ts2.add(new TPoint(-0.5D, 1L));
    ts2.add(new TPoint(0.25D, 2L));
    ts2.add(new TPoint(0.0D, 3L));
    ts2.add(new TPoint(0.25D, 4L));
    ts2.add(new TPoint(0.5D, 5L));
    ts2.add(new TPoint(0.75D, 6L));
    ts2.add(new TPoint(0.0D, 7L));
    TimeseriesInstance tsi2 = new TimeseriesInstance(ts2);

    tsi.AddOccurence("foo2.txt", 4420);
    tsi.AddOccurence("foo2.txt", 44312330);

    tsi.AddOccurences(tsi2);

    assertEquals("occrences test 2", tsi.getOccurences().size(), 4);

  }

  @Test
  public void testEquals() {

//    double[] ts1 = { 1.0, 0.0 };
	Timeseries ts1 = new Timeseries();
	ts1.add(new TPoint(1.0D, 0L));
	ts1.add(new TPoint(0.0D, 1L));

    TimeseriesInstance A = new TimeseriesInstance(ts1);

//    double[] ts2 = { 1.0, 0.0 };
    Timeseries ts2 = new Timeseries();
    ts2.add(new TPoint(1.0D, 0L));
    ts2.add(new TPoint(0.0D, 1L));
    TimeseriesInstance B = new TimeseriesInstance(ts2);

//    double[] ts3 = { 1.0, 1.0 };
    Timeseries ts3 = new Timeseries();
    ts3.add(new TPoint(1.0D, 0L));
    ts3.add(new TPoint(1.0D, 1L));
    TimeseriesInstance C = new TimeseriesInstance(ts3);

    assertEquals("tsi equals base test", true, A.equals(B));

    assertEquals("tsi not-equals base test", false, A.equals(C));

  }

  @Test
  public void testCompareTo_0() {

    System.out.println("\n\n------ testCompareTo() --------");

//    double[] ts1 = { 1.0, 0.0 };
    Timeseries ts1 = new Timeseries();
    ts1.add(new TPoint(1.0D, 0L));
    ts1.add(new TPoint(0.0D, 1L));
    TimeseriesInstance A = new TimeseriesInstance(ts1);

//    double[] ts2 = { 1.0, 0.0 };

    Timeseries ts2 = new Timeseries();
    ts2.add(new TPoint(1.0D, 0L));
    ts2.add(new TPoint(0.0D, 1L));
    TimeseriesInstance B = new TimeseriesInstance(ts2);

//    double[] ts3 = { 0.0, 0.0 };
    Timeseries ts3 = new Timeseries();
    ts3.add(new TPoint(0.0D, 0L));
    ts3.add(new TPoint(0.0D, 1L));
    A.setComparableReferencePoint(ts3);
    B.setComparableReferencePoint(ts3);

    int comp = A.compareTo(B);

    System.out.println("compareTo: " + comp);

    assertEquals("testCompareTo", 0, comp);
  }

  @Test
  public void testCompareTo_1() {

    System.out.println("\n\n------ testCompareTo() 1 --------");

//    double[] ts1 = { 1.0, 1.0 };
    Timeseries ts1 = new Timeseries();
    ts1.add(new TPoint(1.0D, 0L));
    ts1.add(new TPoint(1.0D, 1L));
    TimeseriesInstance A = new TimeseriesInstance(ts1);

//    double[] ts2 = { 0.0, 0.0 };
    Timeseries ts2 = new Timeseries();
    ts2.add(new TPoint(0.0D, 0L));
    ts2.add(new TPoint(0.0D, 1L));
    TimeseriesInstance B = new TimeseriesInstance(ts2);

//    double[] ts3 = { 0.0, 0.0 };
    Timeseries ts3 = new Timeseries();
    ts3.add(new TPoint(0.0D, 0L));
    ts3.add(new TPoint(0.0D, 1L));
    A.setComparableReferencePoint(ts3);
    B.setComparableReferencePoint(ts3);

    int comp = A.compareTo(B);

    System.out.println("compareTo: " + comp);

    assertEquals("testCompareTo 1", 1, comp);

    int comp2 = B.compareTo(A);

    assertEquals("testCompareTo -1", -1, comp2);

  }

}
