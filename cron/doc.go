// Package cron parses cron specs and computes when they fire. kiln uses it for recurring jobs.
//
// A spec has five fields, minute, hour, day of month, month and day of week, or six with a leading
// field for seconds. A field holds *, a number, a range such as 1-5, or a comma-separated list of
// those, and any of them can take a step: */15, 5/15 (from 5 to the end) or 9-17/2. Months and days
// of the week can also be named, JAN to DEC and SUN to SAT, in any case; Sunday is 0 or 7, and ?
// means * in the two day fields. The day of month also accepts L for the last day, L-3 for three
// days before it, 15W for the weekday nearest the 15th within the same month, and LW for the last
// weekday. The day of week also accepts 5L for the last Friday of the month and MON#2 for its
// second Monday. When both day fields are restricted, a day that matches either one fires, as in
// Vixie cron.
//
// A spec can also be one of the descriptors @yearly (or @annually), @monthly, @weekly, @daily (or
// @midnight) and @hourly, or @every followed by a duration of at least one second, such as
// "@every 1h30m".
//
// Across daylight saving changes, a spec with fixed hours keeps to its times of day: a time skipped
// when the clocks go forward fires at the first instant after the gap, and a time repeated when
// they go back fires the first time only. A spec whose hour field is * or a step follows the clock
// instead, skipping the missing hour and firing in both copies of the repeated one.
package cron
