package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/antchfx/htmlquery"
	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func chk(err error) {
	if err != nil {
		panic(err)
	}
}

func retry(err error) bool {
	if err != nil {
		log.Println(err)
		return true
	}

	return false
}

var defaultLastSleep = 20 * time.Second

// WalletAddressResponse represents the API response structure
type WalletAddressResponse struct {
	Found          bool      `json:"found"`
	Error          string    `json:"error,omitempty"`
	Label          string    `json:"label"`
	WalletID       string    `json:"wallet_id"`
	AddressesCount int       `json:"addresses_count"`
	Addresses      []Address `json:"addresses"`
	UpdatedToBlock int       `json:"updated_to_block"`
}

type Address struct {
	Address        string  `json:"address"`
	Balance        float64 `json:"balance"`
	IncomingTxs    int     `json:"incoming_txs"`
	LastUsedInBlock int    `json:"last_used_in_block"`
}

func main() {
	flag.Parse()

	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	mongoURI := os.Getenv("MONGO_URI")

	// Use the SetServerAPIOptions() method to set the Stable API version to 1
	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI(mongoURI).SetServerAPIOptions(serverAPI)
	// Create a new client and connect to the server
	client, err := mongo.Connect(context.TODO(), opts)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err = client.Disconnect(context.TODO()); err != nil {
			panic(err)
		}
	}()

	// Send a ping to confirm a successful connection
	if err := client.Database("admin").RunCommand(context.TODO(), bson.D{{Key: "ping", Value: 1}}).Err(); err != nil {
		panic(err)
	}
	fmt.Println("Pinged your deployment. You successfully connected to MongoDB!")

	db := client.Database("labels")
	coll := db.Collection("bitcoinLabels")

	walletMap := loadWalletMap()
	log.Println(walletMap)

	ticker := time.NewTicker(8 * time.Hour)

	for {
		for walletType, walletNames := range walletMap {
			log.Printf("Processing wallet type '%s' with %d wallets: %s", walletType, len(walletNames), strings.Join(walletNames, ", "))

			ctx := context.TODO()
			for _, walletName := range walletNames {
				if len(walletName) == 0 {
					continue
				}

				from := 0
				const count = 100

				for {
					addrs := loadAddrsByWalletNameAndFrom(walletName, from, count)
					if len(addrs) == 0 {
						break
					}

					models := make([]mongo.WriteModel, 0, len(addrs))
					for _, addr := range addrs {
						doc := bson.M{
							"$set": bson.M{"addr": addr},
							"$addToSet": bson.M{
								"labels": bson.A{bson.M{
									"name": walletName,
									"type": walletType,
									"src":  "walletExplorer",
								}},
							},
						}

						model := mongo.NewUpdateOneModel()
						model.SetUpsert(true)
						model.SetFilter(bson.M{"addr": addr})
						model.SetUpdate(doc)
						models = append(models, model)
					}

				BULKWRITE:
					results, err := coll.BulkWrite(ctx, models)
					if retry(err) {
						log.Println(err)
						time.Sleep(defaultLastSleep)
						goto BULKWRITE
					}

					log.Printf("%s.%s.%d: %d matched, %d upserted, %d modified", walletType, walletName, from, results.MatchedCount, results.UpsertedCount, results.ModifiedCount)
					
					// If we got fewer than 100 addresses, we've reached the end
					if len(addrs) < count {
						break
					}

					from += count
					log.Printf("fetched %d addrs from %s, moving to from=%d", len(addrs), walletName, from)
				}

				log.Printf("done %s.%s", walletType, walletName)
			}
		}

		log.Println("today done")

		<-ticker.C
	}
}

// deprecated: will OOM here due to toooo large addrs
func loadAddrsByWalletName(walletName string) []string {
	page := 1
	addrs := make([]string, 0)

	for ; ; page += 1 {
		lastSleep := defaultLastSleep

		url := fmt.Sprintf("https://www.walletexplorer.com/wallet/%s/addresses?page=%d", walletName, page)
	ADDR_LIST_RETRY:
		req, _ := http.NewRequest("GET", url, nil)
		// avoid limit
		req.Header.Set("Host", "www.walletexplorer.com")
		req.Header.Set("Referer", url)
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")

		resp, err := http.DefaultClient.Do(req)
		if retry(err) {
			log.Println("sleep", lastSleep)
			time.Sleep(lastSleep)
			lastSleep += time.Second

			goto ADDR_LIST_RETRY
		}

		body, err := io.ReadAll(resp.Body)
		if retry(err) {
			log.Println("sleep", lastSleep)
			time.Sleep(lastSleep)
			lastSleep += time.Second

			goto ADDR_LIST_RETRY
		}

		if bytes.Contains(body, []byte("limit")) {
			log.Println("sleep due to limit", lastSleep)
			time.Sleep(lastSleep)
			lastSleep += time.Second

			goto ADDR_LIST_RETRY
		}

		if bytes.Contains(body, []byte("Too many requests")) {
			log.Println("sleep due to too many requests", lastSleep)
			time.Sleep(lastSleep)
			lastSleep += time.Second

			goto ADDR_LIST_RETRY
		}

		doc, err := htmlquery.Parse(bytes.NewBuffer(body))
		if retry(err) {
			log.Println("sleep", lastSleep)
			time.Sleep(lastSleep)
			lastSleep += time.Second

			goto ADDR_LIST_RETRY
		}

		tds := htmlquery.Find(doc, "//table/tbody/tr/td[1]")
		for _, td := range tds {
			addr := htmlquery.InnerText(td)
			addrs = append(addrs, addr)
		}

		log.Printf("fetched %d addrs from %s.%d", len(tds), walletName, page)

		if len(tds) < 100 {
			break
		}
	}

	return addrs
}

// loadAddrsByWalletNameAndFrom fetches addresses using the WalletExplorer API
func loadAddrsByWalletNameAndFrom(walletName string, from, count int) []string {
	addrs := make([]string, 0)
	lastSleep := defaultLastSleep

	url := fmt.Sprintf("https://www.walletexplorer.com/api/1/wallet-addresses?wallet=%s&from=%d&count=%d", walletName, from, count)

ADDR_LIST_RETRY:
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("Failed to read error response body: %v, sleeping %v", err, lastSleep)
		time.Sleep(lastSleep)
		lastSleep += time.Second
		goto ADDR_LIST_RETRY
	}
	
	if resp.StatusCode == 429 {
		log.Printf("API request failed (status: %d), sleeping %v", resp.StatusCode, lastSleep)
		resp.Body.Close()
		time.Sleep(lastSleep)
		lastSleep += time.Second
		goto ADDR_LIST_RETRY
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Failed to read response body: %v, sleeping %v", err, lastSleep)
		time.Sleep(lastSleep)
		lastSleep += time.Second
		goto ADDR_LIST_RETRY
	}

	// Check for rate limit or error messages in response body
	if bytes.Contains(body, []byte("limit")) || bytes.Contains(body, []byte("Too many requests")) {
		log.Printf("Rate limit detected, sleeping %v", lastSleep)
		time.Sleep(lastSleep)
		lastSleep += time.Second
		goto ADDR_LIST_RETRY
	}

	var apiResp WalletAddressResponse
	if err := json.Unmarshal(body, &apiResp); err != nil {
		log.Printf("Failed to parse JSON response: %v, sleeping %v", err, lastSleep)
		time.Sleep(lastSleep)
		lastSleep += time.Second
		goto ADDR_LIST_RETRY
	}

	// Check if the API call was successful
	if !apiResp.Found {
		if apiResp.Error != "" {
			log.Printf("API error for wallet %s: %s", walletName, apiResp.Error)
		} else {
			log.Printf("Wallet %s not found", walletName)
		}
		return addrs // Return empty slice
	}

	// Extract addresses from the API response
	for _, addr := range apiResp.Addresses {
		addrs = append(addrs, addr.Address)
	}

	log.Printf("fetched %d addrs from %s (from=%d, total_count=%d)", len(addrs), walletName, from, apiResp.AddressesCount)

	// Add a small delay to respect rate limits (2 requests/sec)
	time.Sleep(500 * time.Millisecond)

	return addrs
}

func loadWalletMap() map[string][]string {
	const walletMapFile = "walletexplorer_wallet_map.json"
	
	// Try to load from local file first
	if data, err := os.ReadFile(walletMapFile); err == nil {
		var wallets map[string][]string
		if json.Unmarshal(data, &wallets) == nil {
			log.Printf("Loaded wallet map from local file: %s", walletMapFile)
			return wallets
		}
		log.Printf("Failed to parse local wallet map file, fetching from web...")
	} else {
		log.Printf("Local wallet map file not found, fetching from web...")
	}

	// Fetch from web if local file doesn't exist or is invalid
	wallets := fetchWalletMapFromWeb()
	
	// Save to local file for future use
	if data, err := json.MarshalIndent(wallets, "", "  "); err == nil {
		if err := os.WriteFile(walletMapFile, data, 0644); err == nil {
			log.Printf("Saved wallet map to local file: %s", walletMapFile)
		} else {
			log.Printf("Failed to save wallet map to file: %v", err)
		}
	} else {
		log.Printf("Failed to marshal wallet map: %v", err)
	}

	return wallets
}

func fetchWalletMapFromWeb() map[string][]string {
	lastSleep := defaultLastSleep

	wallets := make(map[string][]string)

	url := "https://www.walletexplorer.com/"
LOAD_ALL_RETRY:
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Host", "www.walletexplorer.com")
	req.Header.Set("Referer", url)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")

	resp, err := http.DefaultClient.Do(req)
	if retry(err) {
		time.Sleep(lastSleep)
		lastSleep += time.Second

		goto LOAD_ALL_RETRY
	}
	doc, err := htmlquery.Parse(resp.Body)
	if retry(err) {
		time.Sleep(lastSleep)
		lastSleep += time.Second

		goto LOAD_ALL_RETRY
	}

	// log.Println(htmlquery.InnerText(doc))

	tds := htmlquery.Find(doc, "//table/tbody/tr/td")
	for _, td := range tds {
		h3 := td.FirstChild
		walletTypeWithColon := htmlquery.InnerText(h3)
		walletType := strings.ToLower(walletTypeWithColon)[:len(walletTypeWithColon)-1]
		ul := td.LastChild

		exp, err := regexp.Compile("/wallet/([\\w_.-]+)\"")
		chk(err)

		matchedHrefs := exp.FindAllStringSubmatch(htmlquery.OutputHTML(ul, true), -1)
		wallets[walletType] = make([]string, len(matchedHrefs))
		for _, matched := range matchedHrefs {
			name := strings.TrimSpace(matched[1])
			if len(name) > 0 {
				wallets[walletType] = append(wallets[walletType], name)
			}
		}
	}

	return wallets
}
