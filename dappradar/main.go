package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// https://apilist.tronscanapi.com/api/search/v2?term=token&type=allstart=0&limit=20

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

var defaultLastSleep = 5 * time.Second
var lastSleep = 5 * time.Second

var cache = ttlcache.New[string, struct{}](
	ttlcache.WithTTL[string, struct{}](12 * time.Hour),
)

type Result struct {
	Success        bool `json:"success"`
	Results        []any
	Page           int `json:"page"`
	PageCount      int `json:"pageCount"`
	ResultCount    int `json:"resultCount"`
	ResultsPerPage int `json:"resultsPerPage"`
}

func dump_dapps(database *mongo.Database, chain string, startPage int) {
	page := startPage
	total := 0
PAGE_CHANGE:
	coll := database.Collection(fmt.Sprintf("dappradar%sLabels", cases.Title(language.English, cases.NoLower).String(chain)))
	token_url := fmt.Sprintf("https://apis.dappradar.com/v2/dapps?resultsPerPage=50&page=%d&chain=%s", page, chain)
RETRY:
	req, err := http.NewRequest("GET", token_url, nil)
	chk(err)
	req.Header.Set("X-API-KEY", os.Getenv("DAPPRADAR_API_KEY"))

	time.Sleep(1 * time.Second / 5)
	client := &http.Client{}
	resp, err := client.Do(req)
	if retry(err) {
		lastSleep += defaultLastSleep
		time.Sleep(lastSleep)
		goto RETRY
	}

	body, err := io.ReadAll(resp.Body)
	chk(err)

	if resp.StatusCode >= 400 {
		log.Printf("%d bad request", resp.StatusCode)
		log.Println(string(body))
		return
	}

	var result Result
	json.Unmarshal(body, &result)

	models := make([]mongo.WriteModel, len(result.Results))
	for i, item := range result.Results {
		model := mongo.NewUpdateOneModel().SetUpsert(true).SetFilter(bson.M{"dappId": item.(map[string]any)["dappId"]}).SetUpdate(bson.M{"$set": item})
		models[i] = model
	}

	if len(models) != 0 {
		_, err = coll.BulkWrite(context.TODO(), models)
		if err != nil {
			log.Println(err)
		}
		log.Printf("since %d/%d: %d/%d", page, result.PageCount, total+len(models), result.ResultCount)
	}

	total += len(models)

	if len(models) == 50 {
		page += 1
		goto PAGE_CHANGE
	}
}

// chain flag
var chain = flag.String("chain", "tron", "Chain to collect dapps for (e.g., tron, ethereum, etc.)")

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

	log.Println("This tool only collects dapps from dappradar, not the address labels")
	log.Println("You must have a PRO account to gather the addresses")

	dump_dapps(db, *chain, 1)
}
