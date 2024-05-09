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

func dump_protocols(database *mongo.Database) {
	coll := database.Collection("defillamaLabels")
	token_url := "https://apis.llama.fi/protocols"
RETRY:
	req, err := http.NewRequest("GET", token_url, nil)
	chk(err)
	req.Header.Set("X-API-KEY", os.Getenv("DAPPRADAR_API_KEY_1"))

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
		model := mongo.NewUpdateOneModel().SetUpsert(true).SetFilter(bson.M{"id": item.(map[string]any)["id"]}).SetUpdate(bson.M{"$set": item})
		models[i] = model
	}

	if len(models) != 0 {
		_, err = coll.BulkWrite(context.TODO(), models)
		if err != nil {
			log.Println(err)
		}
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
	dump_protocols(db)
}
