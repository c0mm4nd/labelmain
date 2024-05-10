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

func dump_dapps(database *mongo.Database) {
	offset := 0
	limit := 1_000_000_000
OFFSET_CHANGE:
	coll := database.Collection("coincodexDappLabels")
	url := fmt.Sprintf("https://coincodex.com/api/coincodexdapps/get_filter?platform=&offset=%d&limit=%d&category_id=&search=&order_by=activity_users_1d&order_direction=desc", offset, limit)
RETRY:
	req, err := http.NewRequest("GET", url, nil)
	chk(err)

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

	var result []any
	json.Unmarshal(body, &result)

	models := make([]mongo.WriteModel, len(result))
	for i, item := range result {
		model := mongo.NewUpdateOneModel().SetUpsert(true).SetFilter(bson.M{"slug": item.(map[string]any)["slug"]}).SetUpdate(bson.M{"$set": item})
		models[i] = model
	}

	if len(models) != 0 {
		_, err = coll.BulkWrite(context.TODO(), models)
		if err != nil {
			log.Println(err)
		}
	}

	if len(models) == limit {
		offset += limit
		goto OFFSET_CHANGE
	}
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

	// check from "" to arbitrarily length string. stop length increase when total is 0
	dump_dapps(db)
}
