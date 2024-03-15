package main

import (
	"fmt"
	"net/url"
	"os"
	"time"
)

func rangeDate(start, end time.Time) []time.Time {
	var dates []time.Time
	for d := start; d.Before(end); d = d.AddDate(0, 0, 1) {
		dates = append(dates, d)
	}
	return dates
}

func parseDate(date string) (time.Time, error) {

	dt, err := time.Parse("20060102", date)
	if err != nil {
		return time.Time{}, err
	}
	return dt, nil
}

func main() {
	// コマンドライン引数では、シンボル名、スタート、エンドを受け取る。
	if len(os.Args) != 4 {
		fmt.Println("Invalid arguments")
		os.Exit(1)
	}

	// convert start and end to time
	st, err := parseDate(os.Args[2])
	if err != nil {
		fmt.Println("Invalid start date. use YYYYMMDD format.")
		os.Exit(1)
	}
	ed, err := parseDate(os.Args[3])
	if err != nil {
		fmt.Println("Invalid start date. use YYYYMMDD format.")
		os.Exit(1)
	}

	// make url
	for _, dt := range rangeDate(st, ed) {

		endpoint := "https://public.bybit.com/trading/"
		fileName := os.Args[1] + dt.Format("2006-01-02") + ".csv.gz"

		endpoint, err = url.JoinPath(endpoint, os.Args[1], fileName)
		if err != nil {
			fmt.Println("Invalid symbol name")
			os.Exit(1)
		}

		fmt.Println(endpoint)
	}
}
