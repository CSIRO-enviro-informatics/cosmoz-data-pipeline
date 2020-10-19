# -*- coding: utf-8 -*-
#
from datetime import datetime, timedelta, timezone
import requests
from .intensity import Intensity
from .mongo_db import get_nmdb_from_site_no
class DataGetter(object):
    __slots__ = ("site_no", "nmdb", "timestamp", "session")
    debug_writer = None
    getter_cache = {}

    def __new__(cls, site_no, timestamp):
        """
        :param site_no:
        :type site_no: int
        :param timestamp:
        :type timestamp: datetime
        :return:
        """
        self = super(DataGetter, cls).__new__(cls)
        self.site_no = site_no
        self.nmdb = get_nmdb_from_site_no(site_no)
        new_timestamp = datetime(timestamp.year, timestamp.month, timestamp.day, timestamp.hour, 0, 0, 0, tzinfo=timestamp.tzinfo)
        self.timestamp = new_timestamp
        self.session = requests.session()
        return self

    # /**
    #  * Using a dynamically generated URL, this method queries the
    #  * NEST webpage for the last intensity value. This data is then
    #  * read in as a String and 'taken apart' to find the actual
    #  * intensity.<br>
    #  *
    #  * @return
    #  *         A new <code>Intensity</code> object containing the
    #  * retrieved intensity or <code>null</code> if there was no
    #  * data returned
    #  */
    def get_intensity_from_nmdb(self):
        cache = self.__class__.getter_cache  # type: dict
        if self.nmdb is None or self.timestamp is None:
            return None
        cache_key = (str(self.nmdb), self.timestamp.year, self.timestamp.month, self.timestamp.day, self.timestamp.hour)
        in_cache = cache.get(cache_key, None)
        if in_cache:
            return in_cache
        new_timestamp = datetime(self.timestamp.year, self.timestamp.month,
                                 self.timestamp.day, self.timestamp.hour,
                                 0, 0, 0, tzinfo=self.timestamp.tzinfo)
        negative_hour = timedelta(hours=-1)
        two_hours = timedelta(hours=2)

        new_timestamp = new_timestamp + negative_hour  # type: datetime
        startYear = str(new_timestamp.year)
        startMonth = str(new_timestamp.month)
        startDay = str(new_timestamp.day)
        startHour = str(new_timestamp.hour)
        startMin = "00"

        new_timestamp = new_timestamp + two_hours  # type: datetime
        endYear = str(new_timestamp.year)
        endMonth = str(new_timestamp.month)
        endDay = str(new_timestamp.day)
        endHour = str(new_timestamp.hour)
        endMin = "59"

        url_address = "http://nmdb.eu/nest/draw_graph.php?formchk=1&stations%5B%5D=" + str(self.nmdb)\
            + "&last_days=1&last_label=days_label&date_choice=bydate&start_day=" + startDay \
            + "&start_month=" + startMonth + "&start_year=" + startYear + "&start_hour=" + startHour \
            + "&start_min=" + startMin + "&end_day=" + endDay + "&end_month=" + endMonth \
            + "&end_year=" + endYear + "&end_hour=" + endHour + "&end_min=" + endMin \
            + "&tresolution=60&output=ascii&tabchoice=revori&dtype=corr_for_efficiency&yunits=0"

        try:
            if self.debug_writer:
                self.debug_writer.write("\n===== Data for SiteNo " + str(self.site_no) + " =====\n")
            resp = self.session.get(url_address)
            resp_bytes = resp.content
            resp_lines = resp_bytes.splitlines()
            line_iter = iter(resp_lines)
            line = next(line_iter)
            while b"RCORR_E" not in line or b"DATA TYPE" in line:
                try:
                    line = next(line_iter)
                except StopIteration as s:
                    if self.debug_writer:
                        self.debug_writer.write("No value for NMDB {} at {}".format(self.nmdb, str(self.timestamp)))
                    return None
            line = next(line_iter)  # skip the 1h ago line
            line = next(line_iter)  # get this line



            # String line = reader.readLine();
            # try {
            #     while(!(line.contains("RCORR_E") && (line.contains("start_date_time")))) {
            #         line = reader.readLine();
            #     }
            # } catch (NullPointerException npe) {
            #     try {
            #         writer.write("No value for " + getTimestampAsString() + "!\n");
            #     } catch (IOException e) {}
            #     return null;
            # }
            # line = reader.readLine(); // Don't want this!
            # line = reader.readLine(); // Do want this!
            # reader.close();
            # writer.write("NMDB Row: " + line + "\n");
            #
            # return new Intensity(siteNo, timestamp, Float.parseFloat(line.split(";")[1].trim()), 0);
        except Exception as e:

            # try {
            #     writer.write("Exception with URL or reading data!\n");
            #     writer.write("Cause: " + e.getMessage() + "\n");
            #     writer.write("Stack Trace:\n");
            #     for (int i = 0; i < e.getStackTrace().length; i++)
            #         writer.write(e.getStackTrace()[i].toString() + "\n");
            # } catch (IOException e1) {}
            return None
        intensity = Intensity(self.site_no, self.timestamp, float(line.split(b";")[1]))
        cache[cache_key] = intensity
        return intensity

    # /**
    #  * Using a dynamically generated URL, this method queries the
    #  * NEST webpage for the intensity values. This data is then
    #  * read in as a String and 'taken apart' to find the actual
    #  * intensities.<br>
    #  *
    #  * @return
    #  *         A new <code>List[Intensity]</code> object containing the
    #  * retrieved intensity or <code>null</code> if there was no
    #  * data returned
    #  */
    def get_intensities_from_nmdb(self, startdate, enddate):
        """

        :param startdate:
        :param enddate:
        :return:
        :rtyle: list[Intensity]
        """
        if self.nmdb is None:
            return None

        new_startdate = datetime(startdate.year, startdate.month,
                                 startdate.day, startdate.hour,
                                 0, 0, 0, tzinfo=startdate.tzinfo)
        new_enddate = datetime(enddate.year, enddate.month,
                               enddate.day, enddate.hour,
                               0, 0, 0, tzinfo=enddate.tzinfo)
        negative_hour = timedelta(hours=-1)
        two_hours = timedelta(hours=2)

        new_startdate = new_startdate + negative_hour  # type: datetime
        startYear = str(new_startdate.year)
        startMonth = str(new_startdate.month)
        startDay = str(new_startdate.day)
        startHour = str(new_startdate.hour)
        startMin = "00"

        new_enddate = new_enddate + two_hours  # type: datetime
        endYear = str(new_enddate.year)
        endMonth = str(new_enddate.month)
        endDay = str(new_enddate.day)
        endHour = str(new_enddate.hour)
        endMin = "59"

        url_address = "http://nmdb.eu/nest/draw_graph.php?formchk=1&stations%5B%5D=" + str(self.nmdb)\
            + "&last_days=1&last_label=days_label&date_choice=bydate&start_day=" + startDay \
            + "&start_month=" + startMonth + "&start_year=" + startYear + "&start_hour=" + startHour \
            + "&start_min=" + startMin + "&end_day=" + endDay + "&end_month=" + endMonth \
            + "&end_year=" + endYear + "&end_hour=" + endHour + "&end_min=" + endMin \
            + "&tresolution=60&output=ascii&tabchoice=revori&dtype=corr_for_efficiency&yunits=0"
        intensities = []
        try:
            if self.debug_writer:
                self.debug_writer.write("\n===== Data for SiteNo " + str(self.site_no) + " =====\n")
            resp = self.session.get(url_address)
            resp_bytes = resp.content
            resp_lines = resp_bytes.splitlines()
            line_iter = iter(resp_lines)
            line = next(line_iter)
            while b"RCORR_E" not in line or b"DATA TYPE" in line:
                try:
                    line = next(line_iter)
                except StopIteration as s:
                    if self.debug_writer:
                        self.debug_writer.write("No value for NMDB {} at {}".format(self.nmdb, str(self.timestamp)))
                    return None
            line = next(line_iter)  # skip the 1h ago line
            line = next(line_iter)  # get this line
            while b';' in line:
                try:
                    parts = line.split(b';')
                    this_time = datetime.strptime(parts[0].decode('latin-1'), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                    val = float(parts[1])
                    intensity = Intensity(self.site_no, this_time, val)
                    intensities.append(intensity)
                except IndexError:
                    break
                except BaseException as e:
                    print(e)
                    break
                try:
                    line = next(line_iter)  # get the next line
                except StopIteration:
                    break


            # String line = reader.readLine();
            # try {
            #     while(!(line.contains("RCORR_E") && (line.contains("start_date_time")))) {
            #         line = reader.readLine();
            #     }
            # } catch (NullPointerException npe) {
            #     try {
            #         writer.write("No value for " + getTimestampAsString() + "!\n");
            #     } catch (IOException e) {}
            #     return null;
            # }
            # line = reader.readLine(); // Don't want this!
            # line = reader.readLine(); // Do want this!
            # reader.close();
            # writer.write("NMDB Row: " + line + "\n");
            #
            # return new Intensity(siteNo, timestamp, Float.parseFloat(line.split(";")[1].trim()), 0);
        except Exception as e:

            # try {
            #     writer.write("Exception with URL or reading data!\n");
            #     writer.write("Cause: " + e.getMessage() + "\n");
            #     writer.write("Stack Trace:\n");
            #     for (int i = 0; i < e.getStackTrace().length; i++)
            #         writer.write(e.getStackTrace()[i].toString() + "\n");
            # } catch (IOException e1) {}
            return None
        return intensities


    #
    # /**
    #  * Sets a timestamp as a new <code>GregorianCalendar</code> object
    #  * to avoid any issues.
    #  *
    #  * @param timestamp
    #  *         The timestamp to set values to.
    #  */
    # public void setTimestamp(GregorianCalendar timestamp) {
    #     GregorianCalendar newTimestamp = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
    #     newTimestamp.set(Calendar.YEAR, timestamp.get(Calendar.YEAR));
    #     newTimestamp.set(Calendar.MONTH, timestamp.get(Calendar.MONTH));
    #     newTimestamp.set(Calendar.DATE, timestamp.get(Calendar.DATE));
    #     newTimestamp.set(Calendar.HOUR_OF_DAY, timestamp.get(Calendar.HOUR_OF_DAY));
    #     newTimestamp.set(Calendar.MINUTE, timestamp.get(Calendar.MINUTE));
    #     newTimestamp.set(Calendar.SECOND, timestamp.get(Calendar.SECOND));
    #     newTimestamp.set(Calendar.MILLISECOND, timestamp.get(Calendar.MILLISECOND));
    #     this.timestamp = newTimestamp;
    # }

    def __str__(self):
        return  "DataGetter [nmdb=" + self.nmdb + ", timestamp=" + str(self.timestamp) + "]"

    # /**
    #  * Returns a string representation of this object including Timestamp
    #  * in the form YYYY-MM-DD HH:mm:ss
    #  */
    # @Override
    # public String toString() {
    #     return "DataGetter [nmdb=" + nmdb + ", timestamp=" + getTimestampAsString()
    #             + "]";
    # }

    # /**
    #  * Constructs and returns a string representation of the Timestamp
    #  * attribute in the form YYYY-MM-DD HH:mm:ss
    #  *
    #  * @return
    #  *         A string representation of the Timestamp attribute in the
    #  * form YYYY-MM-DD HH:mm:ss
    #  */
    # public String getTimestampAsString() {
    #     StringBuilder builder = new StringBuilder();
    #     int month = timestamp.get(Calendar.MONTH) + 1;
    #     int date = timestamp.get(Calendar.DATE);
    #     int hour = timestamp.get(Calendar.HOUR_OF_DAY);
    #     int minute = 0;
    #     int second = 0;
    #     String monthAsString = "" + month;
    #     String dateAsString = "" + date;
    #     String hourAsString = "" + hour;
    #     String minuteAsString = "" + minute;
    #     String secondAsString = "" + second;
    #
    #     if (month < 10)
    #         monthAsString = "0" + month;
    #     if (date < 10)
    #         dateAsString = "0" + date;
    #     if (hour < 10)
    #         hourAsString = "0" + hour;
    #     if (minute < 10)
    #         minuteAsString = "0" + minute;
    #     if (second < 10)
    #         secondAsString = "0" + second;
    #
    #     builder.append(timestamp.get(Calendar.YEAR)).append("-")
    #             .append(monthAsString).append("-").append(dateAsString)
    #             .append(" ").append(hourAsString).append(":")
    #             .append(minuteAsString).append(":").append(secondAsString);
    #     return builder.toString();
    # }

