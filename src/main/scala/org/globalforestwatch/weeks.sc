import java.text.SimpleDateFormat
import java.time.{LocalDate, Instant}
import java.util.Date
import java.util.Calendar
import java.util.GregorianCalendar

val calendar = new GregorianCalendar()

val start = LocalDate.of(2015, 1, 1)
val end = LocalDate.now()
val dateRange = start until end

// Create List of `LocalDate` for the period between start and end date

val dates: IndexedSeq[LocalDate] = (0L to (end.toEpochDay - start.toEpochDay))
  .map(days => start.plusDays(days))
val weekFormatter = new SimpleDateFormat("w")

val weeks = for (day <- dates) yield {
  Set(day.getYear, Integer.parseInt(weekFormatter.format(day)))
}
val i = new Instant()

val startD = Date.from()
UTC(2015, 1, 1, 0, 0, 0)

calendar.setTime(sail_dt)
calendar.get(Calendar.WEEK_OF_YEAR)