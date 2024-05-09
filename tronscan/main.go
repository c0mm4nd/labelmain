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
	"regexp"
	"strconv"
	"strings"
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

var defaultSleep = time.Minute

var cache = ttlcache.New[string, struct{}](
	ttlcache.WithTTL[string, struct{}](12 * time.Hour),
)

type Result struct {
	Total    int    `json:"total"`
	Token    []any  `json:"token"`
	Address  []any  `json:"address"`
	Contract []any  `json:"contract"`
	Message  string `json:"message"`
	Error    string `json:"error"`
}

const alphabet = "123456789abcdefghijklmnopqrstuvwxyz_-"

func extractNumber(str string) int {
	pattern := `\d+\s*s`
	re := regexp.MustCompile(pattern)
	match := re.FindString(str)
	if match != "" {
		match = strings.TrimSuffix(match, " s")
		num, _ := strconv.Atoi(match)
		return num
	}
	return 0
}

func search_tronscan(database *mongo.Database, search_type, term string) {
	start := 0
	total := 0
	// START_CHANGE:
	coll := database.Collection(fmt.Sprintf("tron%sLabels", cases.Title(language.English, cases.NoLower).String(search_type)))
	token_url := fmt.Sprintf("https://apilist.tronscanapi.com/api/search/v2?term=%s&type=%s&start=%d&limit=50", term, search_type, start)
RETRY:
	req, err := http.NewRequest("GET", token_url, nil)
	chk(err)
	req.Header.Set("TRON-PRO-API-KEY", os.Getenv("TRONSCAN_API_KEY"))

	time.Sleep(1 * time.Second / 5)
	client := &http.Client{}
	resp, err := client.Do(req)
	if retry(err) {
		time.Sleep(defaultSleep)
		goto RETRY
	}
	body, err := io.ReadAll(resp.Body)
	chk(err)

	var result Result
	json.Unmarshal(body, &result)

	if result.Message != "" {
		log.Println("Message: ", result.Message)
		time.Sleep(defaultSleep)
		goto RETRY
	}

	if result.Error != "" {
		log.Println("Error: ", result.Error)
		sleepTime := extractNumber(result.Error)
		if sleepTime == 0 {
			time.Sleep(defaultSleep)
		} else {
			time.Sleep(time.Duration(sleepTime) * time.Second)
		}

		goto RETRY
	}

	var models []mongo.WriteModel
	switch search_type {
	case "token":
		models = make([]mongo.WriteModel, len(result.Token))
		for i, item := range result.Token {
			model := mongo.NewUpdateOneModel().SetUpsert(true).SetFilter(bson.M{"token_id": item.(map[string]any)["token_id"]}).SetUpdate(bson.M{"$set": item})
			models[i] = model
		}
	case "address":
		models = make([]mongo.WriteModel, len(result.Address))
		for i, item := range result.Address {
			model := mongo.NewUpdateOneModel().SetUpsert(true).SetFilter(bson.M{"address": item.(map[string]any)["address"]}).SetUpdate(bson.M{"$set": item})
			models[i] = model
		}
	case "contract":
		models = make([]mongo.WriteModel, len(result.Contract))
		for i, item := range result.Contract {
			model := mongo.NewUpdateOneModel().SetUpsert(true).SetFilter(bson.M{"contract_address": item.(map[string]any)["contract_address"]}).SetUpdate(bson.M{"$set": item})
			models[i] = model
		}
	default:
		panic("unknown search type")
	}

	if len(models) != 0 {
		_, err = coll.BulkWrite(context.TODO(), models)
		if err != nil {
			log.Println(err)
		}
	}

	total += len(models)

	// if len(models) == 50 {
	// 	start += 50
	// 	goto START_CHANGE
	// }

	if total == 0 {
		// log.Printf("end %s: 0", term)
		return
	} else {
		log.Printf("done %s: %d", term, total)
	}

	for _, char := range alphabet {
		nextTerm := term + string(char)
		search_tronscan(database, search_type, nextTerm)
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

	startPrefix := os.Getenv("TRONSCAN_START_PREFIX")
	isStarted := false
	if startPrefix == "" {
		isStarted = true
	}

	// check from "" to arbitrarily length string. stop length increase when total is 0
	for _, c0 := range alphabet {
		for _, c1 := range alphabet {
			for _, c2 := range alphabet {
				if startPrefix != "" {
					if startPrefix == string(c0)+string(c1)+string(c2) {
						isStarted = true
					}
				}

				if isStarted {
					search_tronscan(db, "token", string(c0)+string(c1)+string(c2))
					search_tronscan(db, "address", string(c0)+string(c1)+string(c2))
					search_tronscan(db, "contract", string(c0)+string(c1)+string(c2))
				}

			}
		}
	}
}
