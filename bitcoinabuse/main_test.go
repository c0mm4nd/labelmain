package main

import (
	"fmt"
	"io"
	"net/http"
	"testing"
)

func TestLoadDetails(t *testing.T) {
	loadDetail("1C2ek9b57xdVY9rPUaUnczxN5vGjVS8EhA")
}

func TestGetTodayMaxPage(t *testing.T) {
	resp, err := http.Get(fmt.Sprintf("https://www.bitcoinabuse.com/reports?page=%d", 1))
	chk(err)
	body, err := io.ReadAll(resp.Body)
	chk(err)
	fmt.Println(getTodayMaxPage(string(body)))
}
