package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type ImportData struct {
	TotalCount int              `json:"total_count"`
	UpdatedAt  string           `json:"updated_at"`
	Addresses  []CryptoAddress  `json:"addresses"`
	ByAsset    map[string]int   `json:"by_asset"`
}

type CryptoAddress struct {
	Address        string   `json:"address"`
	Asset          string   `json:"asset"`
	Chain          string   `json:"chain"`
	EntityName     string   `json:"entity_name"`
	Aliases        []string `json:"aliases,omitempty"`
	EntityType     string   `json:"entity_type"`
	DateOfBirth    string   `json:"date_of_birth,omitempty"`
	Nationality    string   `json:"nationality,omitempty"`
	Location       string   `json:"location,omitempty"`
	Emails         []string `json:"emails,omitempty"`
	PhoneNumbers   []string `json:"phone_numbers,omitempty"`
	Programs       []string `json:"programs,omitempty"`
	ListedDate     string   `json:"listed_date,omitempty"`
	SDNType        string   `json:"sdn_type,omitempty"`
	FixedRef       int      `json:"fixed_ref"`
	Remarks        string   `json:"remarks,omitempty"`
	CollectionDate string   `json:"collection_date"`
}

func main() {
	// Load environment variables
	err := godotenv.Load()
	if err != nil {
		log.Println("Warning: Error loading .env file, using system environment variables")
	}

	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		log.Fatal("MONGO_URI environment variable is not set")
	}

	// Add authSource if not present
	if mongoURI[len(mongoURI)-1] == '/' {
		mongoURI = mongoURI + "?authSource=admin"
	} else {
		mongoURI = mongoURI + "/?authSource=admin"
	}

	// Connect to MongoDB
	ctx := context.Background()
	opts := options.Client().ApplyURI(mongoURI)
	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		log.Fatal("Failed to connect to MongoDB:", err)
	}
	defer client.Disconnect(ctx)

	// Ping MongoDB
	if err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "ping", Value: 1}}).Err(); err != nil {
		log.Fatal("Failed to ping MongoDB:", err)
	}
	log.Println("Successfully connected to MongoDB!")

	db := client.Database("labels")
	coll := db.Collection("ofacLabels")

	// Read JSON file
	log.Println("Reading ofac_addresses.json...")
	jsonData, err := os.ReadFile("ofac_addresses.json")
	if err != nil {
		log.Fatal("Failed to read JSON file:", err)
	}

	var importData ImportData
	if err := json.Unmarshal(jsonData, &importData); err != nil {
		log.Fatal("Failed to parse JSON:", err)
	}

	log.Printf("Loaded %d addresses from JSON file", importData.TotalCount)

	// Delete existing documents
	log.Println("Deleting existing OFAC documents...")
	deleteResult, err := coll.DeleteMany(ctx, bson.M{"source": "OFAC"})
	if err != nil {
		log.Printf("Warning: Failed to delete existing documents: %v", err)
	} else {
		log.Printf("Deleted %d existing documents", deleteResult.DeletedCount)
	}

	// Prepare documents for insertion
	log.Println("Preparing documents for insertion...")
	var documents []interface{}
	for _, addr := range importData.Addresses {
		doc := bson.M{
			"address":         addr.Address,
			"asset":           addr.Asset,
			"chain":           addr.Chain,
			"entity_name":     addr.EntityName,
			"aliases":         addr.Aliases,
			"entity_type":     addr.EntityType,
			"date_of_birth":   addr.DateOfBirth,
			"nationality":     addr.Nationality,
			"location":        addr.Location,
			"emails":          addr.Emails,
			"phone_numbers":   addr.PhoneNumbers,
			"programs":        addr.Programs,
			"listed_date":     addr.ListedDate,
			"sdn_type":        addr.SDNType,
			"fixed_ref":       addr.FixedRef,
			"remarks":         addr.Remarks,
			"source":          "OFAC",
			"collection_date": addr.CollectionDate,
			"updated_at":      time.Now(),
		}
		documents = append(documents, doc)
	}

	// Insert documents in batches
	batchSize := 100
	totalInserted := 0

	for i := 0; i < len(documents); i += batchSize {
		end := i + batchSize
		if end > len(documents) {
			end = len(documents)
		}

		batch := documents[i:end]
		opts := options.InsertMany().SetOrdered(false)
		result, err := coll.InsertMany(ctx, batch, opts)

		if err != nil {
			if bulkErr, ok := err.(mongo.BulkWriteException); ok {
				insertedCount := 0
				if result != nil {
					insertedCount = len(result.InsertedIDs)
				}
				totalInserted += insertedCount
				log.Printf("Batch %d-%d: Inserted %d documents, %d errors",
					i, end, insertedCount, len(bulkErr.WriteErrors))

				if len(bulkErr.WriteErrors) > 0 {
					log.Printf("First error in batch: %v", bulkErr.WriteErrors[0])
				}
			} else {
				log.Printf("Error inserting batch %d-%d: %v", i, end, err)
			}
		} else {
			insertedCount := len(result.InsertedIDs)
			totalInserted += insertedCount
			log.Printf("Batch %d-%d: Successfully inserted %d documents", i, end, insertedCount)
		}
	}

	log.Printf("\n=== Import Summary ===")
	log.Printf("Total documents to import: %d", importData.TotalCount)
	log.Printf("Total documents inserted: %d", totalInserted)
	log.Printf("Import completed!")

	// Verify the import
	count, err := coll.CountDocuments(ctx, bson.M{"source": "OFAC"})
	if err != nil {
		log.Printf("Warning: Failed to count documents: %v", err)
	} else {
		log.Printf("Current OFAC documents in database: %d", count)
	}

	// Show sample documents
	log.Println("\nSample documents from database:")
	cursor, err := coll.Find(ctx, bson.M{"source": "OFAC"}, options.Find().SetLimit(3))
	if err != nil {
		log.Printf("Warning: Failed to query sample documents: %v", err)
	} else {
		defer cursor.Close(ctx)
		count := 0
		for cursor.Next(ctx) {
			var doc bson.M
			if err := cursor.Decode(&doc); err != nil {
				log.Printf("Error decoding document: %v", err)
				continue
			}
			count++
			log.Printf("%d. %s (%s) - %s", count, doc["address"], doc["asset"], doc["entity_name"])
		}
	}
}
