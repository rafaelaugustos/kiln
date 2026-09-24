package cron

var monthLen = [13]int{0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}

func daysIn(y, m int) int {
	if m == 2 && y%4 == 0 && (y%100 != 0 || y%400 == 0) {
		return 29
	}
	return monthLen[m]
}

func epochDays(y, m, d int) int64 {
	if m <= 2 {
		y--
	}
	era := floorDiv(int64(y), 400)
	yoe := int64(y) - era*400
	doy := int64((153*((m+9)%12)+2)/5 + d - 1)
	doe := yoe*365 + yoe/4 - yoe/100 + doy
	return era*146097 + doe - 719468
}

func civil(z int64) (y, m, d int) {
	z += 719468
	era := floorDiv(z, 146097)
	doe := z - era*146097
	yoe := (doe - doe/1460 + doe/36524 - doe/146096) / 365
	doy := doe - (365*yoe + yoe/4 - yoe/100)
	mp := (5*doy + 2) / 153
	d = int(doy - (153*mp+2)/5 + 1)
	m = int(mp+2)%12 + 1
	y = int(yoe + era*400)
	if m <= 2 {
		y++
	}
	return y, m, d
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b < 0 {
		q--
	}
	return q
}
