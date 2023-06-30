package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/antchfx/htmlquery"
	"github.com/jellydator/ttlcache/v3"
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

var initFlag = flag.Bool("init", true, "init redis db by interval loading pages")

var defaultLastSleep = 5 * time.Second
var lastSleep = 5 * time.Second

var cache = ttlcache.New[string, struct{}](
	ttlcache.WithTTL[string, struct{}](12 * time.Hour),
)

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
	coll := db.Collection("bitcoinAbuse")

	workerCh := make(chan string, 0)
	go loadDetailThread(coll, workerCh)

	yesterdayMaxPage := 1

	if *initFlag {
		page := 1
		for {
		INIT_RETRY:
			resp, err := http.Get(fmt.Sprintf("https://www.bitcoinabuse.com/reports?page=%d", page))
			if retry(err) {
				lastSleep += defaultLastSleep
				time.Sleep(lastSleep)
				goto INIT_RETRY
			}
			body, err := io.ReadAll(resp.Body)
			chk(err)

			yesterdayMaxPage = getTodayMaxPage(string(body))

			exp, err := regexp.Compile("href=\"/reports/(\\w{8,}?)\">")
			chk(err)
			matches := exp.FindAllStringSubmatch(string(body), -1)
			// urls := make([]string, len(matches))
			for i := range matches {
				// urls[i] = matches[i][1]
				// rdb.SAdd(context.TODO(), "addr:btc", matches[i][1])
				addr := matches[i][1]
				workerCh <- addr
			}

			log.Printf("page %d: %d", page, len(matches))
			lastSleep = defaultLastSleep
			time.Sleep(lastSleep)

			if len(matches) < 100 {
				break
			}
			page += 1
		}
	}

	page := 1

	for {
	MAIN_RETRY:
		resp, err := http.Get(fmt.Sprintf("https://www.bitcoinabuse.com/reports?page=%d", page))
		if retry(err) {
			lastSleep += defaultLastSleep
			time.Sleep(lastSleep)
			goto MAIN_RETRY
		}
		body, err := io.ReadAll(resp.Body)
		chk(err)
		exp, err := regexp.Compile("href=\"/reports/(\\w{8,}?)\">")
		chk(err)
		matches := exp.FindAllStringSubmatch(string(body), -1)

		todayMaxPage := getTodayMaxPage(string(body))

		// urls := make([]string, len(matches))
		// sum := int64(0)
		for i := range matches {
			// ctx := context.TODO()
			// urls[i] = matches[i][1]
			addr := matches[i][1]
			// intCmd := rdb.SAdd(ctx, "addr:btc", addr)
			// v, err := intCmd.Result()
			// chk(err)
			workerCh <- addr

			// sum += v
		}

		// go func(matches [][]string) {
		// 	ctx := context.TODO()
		// 	for i := range matches {
		// 		addr := matches[i][1]
		// 		details := loadDetail(addr)
		// 		for key, detail := range details {
		// 			result := rdb.HSet(ctx, "btc:"+addr,
		// 				"date:"+strconv.Itoa(key), detail[0],
		// 				"type:"+strconv.Itoa(key), detail[1],
		// 				"desc:"+strconv.Itoa(key), detail[2],
		// 			)
		// 			chk(result.Err())
		// 		}
		// 	}
		// }(matches)

		// log.Printf("page %d: %d", page, sum)

		lastSleep = defaultLastSleep
		time.Sleep(lastSleep * 2)

		if page <= todayMaxPage-yesterdayMaxPage {
			page += 1
		} else {
			log.Printf("today all task done")
			yesterdayMaxPage = todayMaxPage
			page = 1
			time.Sleep(24 * time.Hour)
		}
	}
}

func getTodayMaxPage(body string) int {
	exp, err := regexp.Compile("page=([0-9]*)\"")
	chk(err)
	matchePageNums := exp.FindAllStringSubmatch(string(body), -1)

	maxPage := 0

	for _, pageNumMatch := range matchePageNums {
		// for _, pageNumStr := range pageNumMatch {
		pageNumStr := pageNumMatch[1]
		pageNum, err := strconv.Atoi(pageNumStr)
		chk(err)
		if pageNum > maxPage {
			maxPage = pageNum
		}
		// }

	}

	return maxPage
}

func loadDetailThread(coll *mongo.Collection, workerCh chan string) {
	ctx := context.TODO()

	// members, err := rdb.SMembers(ctx, "addr:btc").Result()
	// chk(err)

	// for _, addr := range members {
	for {
		addr := <-workerCh

		if cache.Has(addr) {
			log.Printf("pass %s", addr)
			continue
		}

		log.Printf("checking %s", addr)
		details := loadDetail(addr)

		reports := bson.A{}

		for _, detail := range details {
			reportDoc := &bson.M{
				"date": detail[0],
				"type": detail[1],
				"desc": detail[2],
			}
			reports = append(reports, reportDoc)
		}

		addrDoc := bson.M{
			"addr":    addr,
			"reports": reports,
		}

		opts := options.Update().SetUpsert(true)
		result, err := coll.UpdateOne(ctx, bson.M{"addr": addr}, bson.M{"$set": addrDoc}, opts)
		chk(err)

		fmt.Printf("Number of documents updated: %v\n", result.ModifiedCount)
		fmt.Printf("Number of documents upserted: %v\n", result.UpsertedCount)
	}
}

func loadDetail(addr string) [][]string {
	page := 1
	reports := make([][]string, 0)

	for ; ; page += 1 {
		url := fmt.Sprintf("https://www.bitcoinabuse.com/reports/%s?page=%d", addr, page)
		// resp, err := http.Get(url)
		// chk(err)
		// body, err := io.ReadAll(resp.Body)
		// chk(err)
		// opts := xmlquery.ParserOptions{
		// 	Decoder: &xmlquery.DecoderOptions{
		// 		Strict: false,
		// 		AutoClose: []string{"div", "td", "tr"},
		// 	},
		// }
	DETAIL_RETRY:
		doc, err := htmlquery.LoadURL(url)
		if retry(err) {
			lastSleep += defaultLastSleep
			time.Sleep(lastSleep)
			goto DETAIL_RETRY
		}

		nodes := htmlquery.Find(doc, "/html/body/div/main/div[2]/table/tbody/tr/td")

		report := make([]string, 3)
		for i, node := range nodes {
			// fmt.Printf("%#v", node.FirstChild.Data)
			report[i%3] = node.FirstChild.Data
			if i%3 == 2 {
				reports = append(reports, report)
				report = make([]string, 3)
			}
		}

		if len(nodes) < 3*10 {
			break
		}
	}

	return reports
}
