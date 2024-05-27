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
	Coin map[string]any `json:"coin"`
	Data []any          `json:"data"`
}

func dump_prices(labelDB, priceDB *mongo.Database, slug string) {
	yesterday := time.Now().UTC().AddDate(0, 0, -1)
	url := fmt.Sprintf("https://coincodex.com/api/coincodexcoins/get_historical_data_by_slug/%s/2008-01-01/%s/1", slug, yesterday.Format("2006-01-02"))
RETRY:
	req, err := http.NewRequest("GET", url, nil)
	chk(err)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

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

	// write coin
	log.Println("writing coin", slug)
	opt := options.Update().SetUpsert(true)
	_, err = labelDB.Collection("coincodex").UpdateOne(context.TODO(), bson.M{"slug": slug}, bson.M{"$set": result.Coin}, opt)
	if err != nil {
		log.Println(err)
	}

	// write prices
	log.Println("writing prices", slug)
	// models := make([]mongo.WriteModel, len(result.Data))
	// for i, item := range result.Data {
	// 	model := mongo.NewUpdateOneModel().SetUpsert(true).SetFilter(bson.M{"time_start": item.(map[string]any)["time_start"], "time_end": item.(map[string]any)["time_end"]}).SetUpdate(bson.M{"$set": item})
	// 	models[i] = model
	// }

	// if len(models) != 0 {
	// 	_, err = priceDB.Collection(slug).BulkWrite(context.TODO(), models)
	// 	if err != nil {
	// 		log.Println(err)
	// 	}
	// }
	_, err = priceDB.Collection("coincodex").UpdateOne(context.TODO(), bson.M{"slug": slug}, bson.M{"$set": result}, opt)
	if err != nil {
		log.Println(err)
	}

	log.Printf("fetched %d prices from %s", len(result.Data), slug)
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

	url := "https://coincodex.com/api/v1/assets/get_asset_list?limit=100000000000000000000000000&order_by=last_market_cap_usd&order_direction=desc&type=crypto"
	req, err := http.NewRequest("GET", url, nil)
	chk(err)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

	httpClient := &http.Client{}
	resp, err := httpClient.Do(req)
	chk(err)

	body, err := io.ReadAll(resp.Body)
	chk(err)

	if resp.StatusCode >= 400 {
		log.Printf("%d bad request", resp.StatusCode)
		log.Println(string(body))
		return
	}

	var result Result
	json.Unmarshal(body, &result)

	// write as labels
	models := make([]mongo.WriteModel, len(result.Data))
	for i, item := range result.Data {
		model := mongo.NewUpdateOneModel().SetUpsert(true).SetFilter(bson.M{"slug": item.(map[string]any)["slug"]}).SetUpdate(bson.M{"$set": item})
		models[i] = model
	}

	labelDB := client.Database("labels")
	if len(models) != 0 {
		_, err = labelDB.Collection("coincodexAssetLabels").BulkWrite(context.TODO(), models)
		if err != nil {
			log.Println(err)
		}
	}

	priceDB := client.Database("coincodexPrices")
	for _, item := range result.Data {
		slug := item.(map[string]any)["slug"].(string)
		dump_prices(labelDB, priceDB, slug)
	}
}
