package main

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/go-gota/gota/dataframe"
)

const baseURL = "https://public.bybit.com/trading/"

type BybitTradingRow struct {
	Timestamp time.Time
	Symbol    string
	Side      string
	Size      string
	Price     string
}

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

func makeURLs(symbol string, start, end time.Time) []string {
	var urls []string

	for _, dt := range rangeDate(start, end) {
		fileName := symbol + dt.Format("2006-01-02") + ".csv.gz"
		url, err := url.JoinPath(baseURL, symbol, fileName)
		if err != nil {
			fmt.Println("Invalid symbol name")
			os.Exit(1)
		}

		urls = append(urls, url)
	}
	return urls
}

func downloadTradingData(url string) []BybitTradingRow {

	client := http.Client{
		Timeout: 30 * time.Second,
	}

	req, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		url,
		nil,
	)
	if err != nil {
		fmt.Println("Error: ", err)
		os.Exit(1)
	}

	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error: ", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		fmt.Println("Error: ", err)
		os.Exit(1)
	}

	r := csv.NewReader(gz)

	btrs := []BybitTradingRow{}
	// skip header
	r.Read()
	for {
		record, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Error: ", err)
			os.Exit(1)
		}

		// convert string of Unix timestamp(ms) to time.Time
		t, err := strconv.ParseFloat(record[0], 64)
		if err != nil {
			fmt.Println("Error: ", err)
			os.Exit(1)
		}
		tm := time.UnixMilli(int64(t * 1000))

		btd := BybitTradingRow{
			Timestamp: tm,
			Symbol:    record[1],
			Side:      record[2],
			Size:      record[3],
			Price:     record[4],
		}

		btrs = append(btrs, btd)
	}

	return btrs
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

	urls := makeURLs(os.Args[1], st, ed)
	for _, url := range urls {

		fmt.Println("Downloading: ", url)

		trd := downloadTradingData(url)
		df := dataframe.LoadStructs(trd)

		fmt.Println(df)
	}
}
