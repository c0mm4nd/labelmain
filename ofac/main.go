package main

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	SDN_XML_URL = "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN_ADVANCED.XML"
	NAMESPACE   = "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/ADVANCED_XML"
)

// Supported cryptocurrency assets
var SUPPORTED_ASSETS = []string{
	"XBT", "ETH", "XMR", "LTC", "ZEC", "DASH", "BTG", "ETC",
	"BSV", "BCH", "XVG", "USDT", "XRP", "ARB", "BSC", "USDC", "TRX",
}

type CryptoAddress struct {
	Address         string   `json:"address"`
	Asset           string   `json:"asset"`
	Chain           string   `json:"chain"`
	EntityName      string   `json:"entity_name"`
	Aliases         []string `json:"aliases,omitempty"`
	EntityType      string   `json:"entity_type"`
	DateOfBirth     string   `json:"date_of_birth,omitempty"`
	Nationality     string   `json:"nationality,omitempty"`
	Location        string   `json:"location,omitempty"`
	Emails          []string `json:"emails,omitempty"`
	PhoneNumbers    []string `json:"phone_numbers,omitempty"`
	Programs        []string `json:"programs,omitempty"`
	ListedDate      string   `json:"listed_date,omitempty"`
	SDNType         string   `json:"sdn_type,omitempty"`
	FixedRef        int      `json:"fixed_ref"`
	Remarks         string   `json:"remarks,omitempty"`
	CollectionDate  string   `json:"collection_date"`
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

	// Connect to MongoDB
	ctx := context.Background()

	// Add authSource if not present in URI
	if !strings.Contains(mongoURI, "authSource") {
		// Ensure URI has proper format for query parameters
		if strings.Contains(mongoURI, "?") {
			mongoURI = mongoURI + "&authSource=admin"
		} else if strings.HasSuffix(mongoURI, "/") {
			mongoURI = mongoURI + "?authSource=admin"
		} else {
			mongoURI = mongoURI + "/?authSource=admin"
		}
		log.Printf("Added authSource=admin to MongoDB URI")
	}

	opts := options.Client().ApplyURI(mongoURI)
	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		log.Fatal("Failed to connect to MongoDB:", err)
	}
	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			log.Println("Error disconnecting from MongoDB:", err)
		}
	}()

	// Ping MongoDB
	if err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "ping", Value: 1}}).Err(); err != nil {
		log.Fatal("Failed to ping MongoDB:", err)
	}
	log.Println("Successfully connected to MongoDB!")

	db := client.Database("labels")
	coll := db.Collection("ofacLabels")

	// Fetch and parse OFAC SDN XML
	var addresses []CryptoAddress

	// Try to use local file first if it exists
	localFile := "sdn_advanced.xml"
	if _, err := os.Stat(localFile); err == nil {
		log.Printf("Using local file: %s", localFile)
		addresses, err = parseOFACFile(localFile)
		if err != nil {
			log.Fatal("Failed to parse local OFAC file:", err)
		}
	} else {
		// Download from URL
		log.Println("Downloading OFAC SDN XML file from web...")
		addresses, err = fetchAndParseOFAC(SDN_XML_URL)
		if err != nil {
			log.Fatal("Failed to fetch and parse OFAC data:", err)
		}
	}

	log.Printf("Extracted %d cryptocurrency addresses", len(addresses))

	// Try to store to MongoDB
	err = storeToMongoDB(ctx, coll, addresses)
	if err != nil {
		log.Printf("MongoDB storage failed: %v", err)
		log.Println("Falling back to JSON file storage...")

		// Save to JSON file as fallback
		if err := saveToJSON(addresses, "ofac_addresses.json"); err != nil {
			log.Fatal("Failed to save to JSON:", err)
		}
		log.Println("Successfully saved to ofac_addresses.json")
	}

	log.Println("Successfully completed OFAC data collection!")
}

func parseOFACFile(filename string) ([]CryptoAddress, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	log.Println("Parsing XML data from file...")
	return parseOFACReader(file)
}

func fetchAndParseOFAC(url string) ([]CryptoAddress, error) {
	// Download XML
	client := &http.Client{
		Timeout: 5 * time.Minute,
	}
	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to download: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	log.Println("Parsing XML data from web...")
	return parseOFACReader(resp.Body)
}

func parseOFACReader(reader io.Reader) ([]CryptoAddress, error) {
	// Create a decoder
	decoder := xml.NewDecoder(reader)

	// Map to store feature type IDs
	featureTypeMap := make(map[int]string) // ID -> Asset type name

	// Extended party info
	type PartyInfo struct {
		FixedRef       int
		EntityName     string
		Aliases        []string
		PartySubTypeID int
		DateOfBirth    string
		Nationality    string
		Location       string
		Emails         []string
		PhoneNumbers   []string
		Remarks        string
		Addresses      []struct {
			Asset   string
			Address string
		}
	}
	parties := make(map[int]*PartyInfo)

	// Sanctions info
	type SanctionInfo struct {
		ListedDate string
		Programs   []string
		SDNType    string
	}
	sanctionsMap := make(map[int]*SanctionInfo) // FixedRef -> Sanctions

	var currentParty *PartyInfo
	var currentSanction *SanctionInfo
	var currentProfileID int
	var currentFeatureTypeID int
	var inFeatureType bool
	var inDistinctParty bool
	var inAlias bool
	var inFeature bool
	var inVersionDetail bool
	var inNamePartValue bool
	var inSanctionsEntry bool
	var inEntryEvent bool
	var inSanctionsMeasure bool
	var inComment bool
	var inDatePeriod bool
	var inDate bool
	var currentYear, currentMonth, currentDay string

	var currentFeature struct {
		TypeID int
	}

	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error parsing XML: %w", err)
		}

		switch se := token.(type) {
		case xml.StartElement:
			switch se.Name.Local {
			case "FeatureType":
				inFeatureType = true
				for _, attr := range se.Attr {
					if attr.Name.Local == "ID" {
						fmt.Sscanf(attr.Value, "%d", &currentFeatureTypeID)
					}
				}
			case "DistinctParty":
				inDistinctParty = true
				currentParty = &PartyInfo{}
				for _, attr := range se.Attr {
					if attr.Name.Local == "FixedRef" {
						fmt.Sscanf(attr.Value, "%d", &currentParty.FixedRef)
					}
				}
			case "Profile":
				if inDistinctParty {
					for _, attr := range se.Attr {
						if attr.Name.Local == "PartySubTypeID" {
							fmt.Sscanf(attr.Value, "%d", &currentParty.PartySubTypeID)
						}
					}
				}
			case "Alias":
				if inDistinctParty {
					inAlias = true
				}
			case "NamePartValue":
				if inDistinctParty {
					inNamePartValue = true
				}
			case "Feature":
				if inDistinctParty {
					inFeature = true
					for _, attr := range se.Attr {
						if attr.Name.Local == "FeatureTypeID" {
							fmt.Sscanf(attr.Value, "%d", &currentFeature.TypeID)
						}
					}
				}
			case "VersionDetail":
				if inFeature {
					inVersionDetail = true
				}
			case "DatePeriod":
				if inFeature {
					inDatePeriod = true
				}
			case "Comment":
				if inSanctionsMeasure {
					inComment = true
				}
			case "SanctionsEntry":
				inSanctionsEntry = true
				currentSanction = &SanctionInfo{Programs: []string{}}
				for _, attr := range se.Attr {
					if attr.Name.Local == "ProfileID" {
						fmt.Sscanf(attr.Value, "%d", &currentProfileID)
					}
				}
			case "EntryEvent":
				if inSanctionsEntry {
					inEntryEvent = true
				}
			case "Date":
				if inEntryEvent {
					inDate = true
					currentYear, currentMonth, currentDay = "", "", ""
				}
			case "Year":
				// Will capture in CharData
			case "Month":
				// Will capture in CharData
			case "Day":
				// Will capture in CharData
			case "SanctionsMeasure":
				if inSanctionsEntry {
					inSanctionsMeasure = true
				}
			}

		case xml.EndElement:
			switch se.Name.Local {
			case "FeatureType":
				inFeatureType = false
				currentFeatureTypeID = 0
			case "DistinctParty":
				if currentParty != nil && currentParty.FixedRef > 0 {
					parties[currentParty.FixedRef] = currentParty
				}
				inDistinctParty = false
				currentParty = nil
			case "Alias":
				inAlias = false
			case "NamePartValue":
				inNamePartValue = false
			case "Feature":
				inFeature = false
				currentFeature.TypeID = 0
			case "VersionDetail":
				inVersionDetail = false
			case "DatePeriod":
				inDatePeriod = false
			case "Comment":
				inComment = false
			case "SanctionsEntry":
				if currentSanction != nil && currentProfileID > 0 {
					sanctionsMap[currentProfileID] = currentSanction
				}
				inSanctionsEntry = false
				currentSanction = nil
				currentProfileID = 0
			case "EntryEvent":
				inEntryEvent = false
			case "Date":
				if inEntryEvent && currentYear != "" && currentSanction != nil {
					currentSanction.ListedDate = fmt.Sprintf("%s-%s-%s", currentYear, currentMonth, currentDay)
				}
				inDate = false
			case "SanctionsMeasure":
				inSanctionsMeasure = false
			}

		case xml.CharData:
			data := strings.TrimSpace(string(se))
			if data == "" {
				continue
			}

			if inFeatureType && currentFeatureTypeID > 0 {
				// Check if this is a digital currency address type
				if strings.HasPrefix(data, "Digital Currency Address - ") {
					asset := strings.TrimPrefix(data, "Digital Currency Address - ")
					for _, supportedAsset := range SUPPORTED_ASSETS {
						if asset == supportedAsset {
							featureTypeMap[currentFeatureTypeID] = asset
							log.Printf("Found feature type for %s with ID %d", asset, currentFeatureTypeID)
							break
						}
					}
				}
			} else if inNamePartValue && currentParty != nil {
				if currentParty.EntityName == "" {
					currentParty.EntityName = data
				} else if inAlias {
					// Add to aliases if different from main name
					if data != currentParty.EntityName {
						// Check if not already in aliases
						found := false
						for _, alias := range currentParty.Aliases {
							if alias == data {
								found = true
								break
							}
						}
						if !found {
							currentParty.Aliases = append(currentParty.Aliases, data)
						}
					}
				}
			} else if inVersionDetail && currentParty != nil {
				// Check if this feature is a crypto address
				if asset, ok := featureTypeMap[currentFeature.TypeID]; ok {
					currentParty.Addresses = append(currentParty.Addresses, struct {
						Asset   string
						Address string
					}{
						Asset:   asset,
						Address: data,
					})
				} else {
					// Handle other feature types
					switch currentFeature.TypeID {
					case 8: // Date of birth
						if inDatePeriod {
							// Will be captured from DatePeriod elements
						}
					case 21: // Email
						currentParty.Emails = append(currentParty.Emails, data)
					case 524: // Phone number
						currentParty.PhoneNumbers = append(currentParty.PhoneNumbers, data)
					}
				}
			} else if inComment && inSanctionsMeasure && currentSanction != nil {
				if data != "" {
					currentSanction.Programs = append(currentSanction.Programs, data)
				}
			} else if inDate {
				// Capturing date components
				if strings.Contains(string(se), data) {
					// Determine which component we're in by looking at recent elements
					// This is a simplified approach
					if currentYear == "" {
						currentYear = data
					} else if currentMonth == "" {
						currentMonth = data
						if len(currentMonth) == 1 {
							currentMonth = "0" + currentMonth
						}
					} else if currentDay == "" {
						currentDay = data
						if len(currentDay) == 1 {
							currentDay = "0" + currentDay
						}
					}
				}
			}
		}
	}

	log.Printf("Found %d feature types and %d parties", len(featureTypeMap), len(parties))
	log.Printf("Found %d sanctions entries", len(sanctionsMap))

	// Convert to CryptoAddress slice
	var addresses []CryptoAddress
	collectionDate := time.Now().Format("2006-01-02")

	for _, party := range parties {
		sanctions := sanctionsMap[party.FixedRef]

		// Determine entity type
		entityType := "Unknown"
		switch party.PartySubTypeID {
		case 1:
			entityType = "Aircraft"
		case 2:
			entityType = "Vessel"
		case 3:
			entityType = "Entity"
		case 4:
			entityType = "Individual"
		}

		for _, addr := range party.Addresses {
			cryptoAddr := CryptoAddress{
				Address:        addr.Address,
				Asset:          addr.Asset,
				Chain:          mapAssetToChain(addr.Asset),
				EntityName:     party.EntityName,
				Aliases:        party.Aliases,
				EntityType:     entityType,
				DateOfBirth:    party.DateOfBirth,
				Nationality:    party.Nationality,
				Location:       party.Location,
				Emails:         party.Emails,
				PhoneNumbers:   party.PhoneNumbers,
				FixedRef:       party.FixedRef,
				Remarks:        party.Remarks,
				CollectionDate: collectionDate,
			}

			if sanctions != nil {
				cryptoAddr.ListedDate = sanctions.ListedDate
				cryptoAddr.Programs = sanctions.Programs
				cryptoAddr.SDNType = sanctions.SDNType
			}

			addresses = append(addresses, cryptoAddr)
		}
	}

	return addresses, nil
}

func storeToMongoDB(ctx context.Context, coll *mongo.Collection, addresses []CryptoAddress) error {
	if len(addresses) == 0 {
		log.Println("No addresses to store")
		return nil
	}

	log.Printf("Storing %d addresses to MongoDB...", len(addresses))

	// Print first few addresses for verification
	log.Println("\nSample addresses extracted:")
	for i, addr := range addresses {
		if i >= 5 {
			break
		}
		log.Printf("  [%s] %s - %s", addr.Asset, addr.Address, addr.EntityName)
	}
	log.Println()

	successCount := 0
	errorCount := 0

	// Use InsertMany for better performance and batch error handling
	var documents []interface{}
	for _, addr := range addresses {
		doc := bson.M{
			"address":        addr.Address,
			"asset":          addr.Asset,
			"chain":          addr.Chain,
			"entity_name":    addr.EntityName,
			"aliases":        addr.Aliases,
			"entity_type":    addr.EntityType,
			"date_of_birth":  addr.DateOfBirth,
			"nationality":    addr.Nationality,
			"location":       addr.Location,
			"emails":         addr.Emails,
			"phone_numbers":  addr.PhoneNumbers,
			"programs":       addr.Programs,
			"listed_date":    addr.ListedDate,
			"sdn_type":       addr.SDNType,
			"fixed_ref":      addr.FixedRef,
			"remarks":        addr.Remarks,
			"source":         "OFAC",
			"collection_date": addr.CollectionDate,
			"updated_at":     time.Now(),
		}
		documents = append(documents, doc)
	}

	// Try batch insert with ordered=false to continue on errors
	opts := options.InsertMany().SetOrdered(false)
	result, err := coll.InsertMany(ctx, documents, opts)
	if err != nil {
		// Check if it's a bulk write error (some succeeded, some failed)
		if bulkErr, ok := err.(mongo.BulkWriteException); ok {
			successCount = len(bulkErr.WriteErrors)
			if result != nil {
				successCount = len(result.InsertedIDs)
			}
			errorCount = len(addresses) - successCount
			log.Printf("Batch insert completed with errors: %d successful, %d failed", successCount, errorCount)
			if len(bulkErr.WriteErrors) > 0 {
				log.Printf("First error: %v", bulkErr.WriteErrors[0])
			}
		} else {
			log.Printf("Error inserting documents: %v", err)
			return err
		}
	} else {
		successCount = len(result.InsertedIDs)
		log.Printf("Successfully inserted %d documents", successCount)
	}

	return nil
}

func saveToJSON(addresses []CryptoAddress, filename string) error {
	// Group addresses by asset for better organization
	type ExportData struct {
		TotalCount int              `json:"total_count"`
		UpdatedAt  string           `json:"updated_at"`
		Addresses  []CryptoAddress  `json:"addresses"`
		ByAsset    map[string]int   `json:"by_asset"`
	}

	byAsset := make(map[string]int)
	for _, addr := range addresses {
		byAsset[addr.Asset]++
	}

	data := ExportData{
		TotalCount: len(addresses),
		UpdatedAt:  time.Now().Format(time.RFC3339),
		Addresses:  addresses,
		ByAsset:    byAsset,
	}

	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}

	if err := os.WriteFile(filename, jsonData, 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	log.Printf("Saved %d addresses to %s", len(addresses), filename)
	log.Printf("Address count by asset: %v", byAsset)

	return nil
}

func mapAssetToChain(asset string) string {
	// Map crypto assets to blockchain names
	chainMap := map[string]string{
		"XBT":  "Bitcoin",
		"ETH":  "Ethereum",
		"XMR":  "Monero",
		"LTC":  "Litecoin",
		"ZEC":  "Zcash",
		"DASH": "Dash",
		"BTG":  "Bitcoin Gold",
		"ETC":  "Ethereum Classic",
		"BSV":  "Bitcoin SV",
		"BCH":  "Bitcoin Cash",
		"XVG":  "Verge",
		"USDT": "Tether",
		"XRP":  "Ripple",
		"ARB":  "Arbitrum",
		"BSC":  "Binance Smart Chain",
		"USDC": "USD Coin",
		"TRX":  "Tron",
	}

	if chain, ok := chainMap[asset]; ok {
		return chain
	}
	return asset
}
