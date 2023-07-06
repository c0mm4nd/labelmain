package main

import (
	"bytes"
	"context"
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

var initFlag = flag.Bool("init", true, "init mongodb by interval loading pages")

var defaultLastSleep = 5 * time.Second

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
			log.Println(strings.Join(walletNames, ", "))

			ctx := context.TODO()
			for _, walletName := range walletNames {
				if len(walletName) == 0 {
					continue
				}

				addrs := loadAddrsByWalletName(walletName)
				if len(addrs) == 0 {
					log.Printf("failed to load address from %s.%s", walletType, walletName)
					return
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

				results, err := coll.BulkWrite(ctx, models)
				chk(err)

				log.Printf("%s.%s: %d matched, %d upserted, %d modified", walletType, walletName, results.MatchedCount, results.UpsertedCount, results.ModifiedCount)
			}
		}

		log.Println("today done")

		<-ticker.C
	}
}

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
			lastSleep += lastSleep
			log.Println("sleep", lastSleep)
			time.Sleep(lastSleep)

			goto ADDR_LIST_RETRY
		}

		body, err := io.ReadAll(resp.Body)
		if retry(err) {
			lastSleep += lastSleep
			log.Println("sleep", lastSleep)
			time.Sleep(lastSleep)

			goto ADDR_LIST_RETRY
		}

		if bytes.Contains(body, []byte("limit")) {
			lastSleep += lastSleep
			log.Println("sleep due to limit", lastSleep)
			time.Sleep(lastSleep)

			goto ADDR_LIST_RETRY
		}

		if bytes.Contains(body, []byte("Too many requests")) {
			lastSleep += lastSleep
			log.Println("sleep due to limit", lastSleep)
			time.Sleep(lastSleep)

			goto ADDR_LIST_RETRY
		}

		doc, err := htmlquery.Parse(bytes.NewBuffer(body))
		if retry(err) {
			lastSleep += lastSleep
			log.Println("sleep", lastSleep)
			time.Sleep(lastSleep)

			goto ADDR_LIST_RETRY
		}

		tds := htmlquery.Find(doc, "//table/tbody/tr/td[1]")
		for _, td := range tds {
			addr := htmlquery.InnerText(td)
			addrs = append(addrs, addr)
		}

		if len(tds) < 100 {
			break
		}
	}

	return addrs
}

func loadWalletMap() map[string][]string {
	wallets := make(map[string][]string)

	url := fmt.Sprintf("https://www.walletexplorer.com/")
LOAD_ALL_RETRY:
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Host", "www.walletexplorer.com")
	req.Header.Set("Referer", url)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")

	resp, err := http.DefaultClient.Do(req)
	doc, err := htmlquery.Parse(resp.Body)
	if retry(err) {
		lastSleep += defaultLastSleep
		time.Sleep(lastSleep)
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
