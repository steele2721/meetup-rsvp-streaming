import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructType}

package object RSVPmodel {
  val MeetupSchema = new StructType()
    .add("event",
      new StructType()
        .add("eventID", StringType, true)
        .add("eventName", StringType, true)
        .add("eventURL", StringType, true)
        .add("time", LongType, true))
    .add("group",
      new StructType()
        .add("group_city", StringType, true)
        .add("group_country", StringType, true)
        .add("group_id", LongType, true)
        .add("group_lat", DoubleType, true)
        .add("group_lon", DoubleType, true)
        .add("group_name", StringType, true)
        .add("group_state", StringType, true))
    .add("guests", LongType, true)
    .add("member",
      new StructType()
        .add("member_id", LongType, true)
        .add("member_name", LongType, true))
    .add("m_time", LongType, true)
    .add("response", LongType, true)
    .add("rsvp_id", LongType, true)
    .add("venue",
      new StructType()
        .add("lat", DoubleType, true)
        .add("lon", DoubleType, true)
        .add("venue_id", LongType, true)
        .add("venue_name", StringType, true))
    .add("visibility", StringType, true)

}
