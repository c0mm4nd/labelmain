package main

import (
	"fmt"
	"testing"
)

func TestLoadAllWallets(t *testing.T) {
	t.Log(loadWalletMap())
}

func TestLoadWalletAddrs(t *testing.T) {

	fmt.Println(loadAddrsByWalletName("CoinJar.com"))
}
