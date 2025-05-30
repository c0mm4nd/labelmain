package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// GraphQL request structures
type GraphQLRequest struct {
	OperationName string                 `json:"operationName"`
	Variables     map[string]interface{} `json:"variables"`
	Query         string                 `json:"query"`
}

// Response structures
type PageInfo struct {
	HasNextPage     bool   `json:"hasNextPage"`
	HasPreviousPage bool   `json:"hasPreviousPage"`
	StartCursor     string `json:"startCursor"`
	EndCursor       string `json:"endCursor"`
	Typename        string `json:"__typename"`
}

type ReportEdge struct {
	Cursor string      `json:"cursor"`
	Node   interface{} `json:"node"`
}

type Reports struct {
	PageInfo   PageInfo     `json:"pageInfo"`
	Edges      []ReportEdge `json:"edges"`
	Count      int          `json:"count"`
	TotalCount int          `json:"totalCount"`
	Typename   string       `json:"__typename"`
}

type ReportsData struct {
	Reports Reports `json:"reports"`
}

type GraphQLResponse struct {
	Data   *ReportsData           `json:"data"`
	Errors []interface{}          `json:"errors,omitempty"`
	Raw    map[string]interface{} `json:"-"`
}

const graphqlQuery = `query GetReports($input: ReportsInput, $after: String, $before: String, $last: Float, $first: Float) {
  reports(
    input: $input
    after: $after
    before: $before
    last: $last
    first: $first
  ) {
    pageInfo {
      hasNextPage
      hasPreviousPage
      startCursor
      endCursor
      __typename
    }
    edges {
      cursor
      node {
        ...Report
        __typename
      }
      __typename
    }
    count
    totalCount
    __typename
  }
}

fragment Report on Report {
  id
  isPrivate
  ...ReportPreviewDetails
  ...ReportAccusedScammers
  ...ReportAuthor
  ...ReportAddresses
  ...ReportEvidences
  ...ReportCompromiseIndicators
  ...ReportTokenIDs
  ...ReportTransactionHashes
  __typename
}

fragment ReportPreviewDetails on Report {
  createdAt
  scamCategory
  categoryDescription
  biDirectionalVoteCount
  viewerDidVote
  description
  lexicalSerializedDescription
  commentsCount
  source
  checked
  __typename
}

fragment ReportAccusedScammers on Report {
  accusedScammers {
    id
    info {
      id
      contact
      type
      __typename
    }
    __typename
  }
  __typename
}

fragment ReportAuthor on Report {
  reportedBy {
    id
    username
    trusted
    __typename
  }
  __typename
}

fragment ReportAddresses on Report {
  addresses {
    id
    address
    chain
    domain
    label
    __typename
  }
  __typename
}

fragment ReportEvidences on Report {
  evidences {
    id
    description
    photo {
      id
      name
      description
      url
      __typename
    }
    __typename
  }
  __typename
}

fragment ReportCompromiseIndicators on Report {
  compromiseIndicators {
    id
    type
    value
    __typename
  }
  __typename
}

fragment ReportTokenIDs on Report {
  tokens {
    id
    tokenId
    __typename
  }
  __typename
}

fragment ReportTransactionHashes on Report {
  transactionHashes {
    id
    hash
    chain
    label
    __typename
  }
  __typename
}`

// get50ReportsAfterCursor fetches 50 reports after the given cursor
func get50ReportsAfterCursor(cursor *string) (*GraphQLResponse, error) {
	reqData := GraphQLRequest{
		OperationName: "GetReports",
		Variables: map[string]interface{}{
			"input": map[string]interface{}{
				"chains":         []interface{}{},
				"scamCategories": []interface{}{},
				"orderBy": map[string]interface{}{
					"field":     "CREATED_AT",
					"direction": "DESC",
				},
			},
			"first": 50, // MAX
		},
		Query: graphqlQuery,
	}

	if cursor != nil {
		reqData.Variables["after"] = *cursor
	}

	jsonData, err := json.Marshal(reqData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	for {
		resp, err := http.Post(
			"https://www.chainabuse.com/api/graphql-proxy",
			"application/json",
			bytes.NewBuffer(jsonData),
		)
		if err != nil {
			fmt.Printf("from %v, request failed: %v\n", cursor, err)
			time.Sleep(10 * time.Second)
			continue
		}
		defer resp.Body.Close()

		var result map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			fmt.Printf("from %v, result is not json: %v\n", cursor, err)
			time.Sleep(10 * time.Second)
			continue
		}

		// Convert to our structured response
		resultBytes, _ := json.Marshal(result)
		var graphqlResp GraphQLResponse
		json.Unmarshal(resultBytes, &graphqlResp)
		graphqlResp.Raw = result

		return &graphqlResp, nil
	}
}

// hasNext checks if there are more pages
func hasNext(result *GraphQLResponse) bool {
	if result.Data == nil || result.Data.Reports.PageInfo.HasNextPage == false {
		return false
	}
	return result.Data.Reports.PageInfo.HasNextPage
}

// writeErrorLog writes error data to a log file
func writeErrorLog(data interface{}) error {
	filename := fmt.Sprintf("error_%d.log", time.Now().Unix())
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(data)
}

func main() {
	// Load environment variables
	if err := godotenv.Load(); err != nil {
		log.Printf("Warning: Could not load .env file: %v", err)
	}

	// Connect to MongoDB
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		log.Fatal("MONGO_URI environment variable is required")
	}

	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatal("Failed to connect to MongoDB:", err)
	}
	defer client.Disconnect(context.TODO())

	db := client.Database("labels")
	coll := db.Collection("chainAbuse")

	var cursor *string // CHANGEME if you want to start from a specific cursor
	totalUpserted := int64(0)
	totalModified := int64(0)
	totalMatched := int64(0)

	for {
		chainabuseResult, err := get50ReportsAfterCursor(cursor)
		if err != nil {
			log.Printf("Error fetching reports: %v", err)
			time.Sleep(time.Hour)
			continue
		}

		// Handle case when TOO MANY REQUESTS or no data
		if chainabuseResult.Data == nil {
			fmt.Printf("from %v, chainabuse_result has no data\n", cursor)
			if err := writeErrorLog(chainabuseResult.Raw); err != nil {
				log.Printf("Failed to write error log: %v", err)
			}
			time.Sleep(time.Hour)
			continue
		}

		// Handle case when no reports in response
		if len(chainabuseResult.Data.Reports.Edges) == 0 {
			fmt.Printf("no more reports, cur %v\n", cursor)
			time.Sleep(time.Hour)
			continue
		}

		// Prepare bulk operations
		var bulkOps []mongo.WriteModel
		for _, report := range chainabuseResult.Data.Reports.Edges {
			// Convert report.Node to bson.M for MongoDB operations
			nodeData, _ := json.Marshal(report.Node)
			var nodeMap bson.M
			json.Unmarshal(nodeData, &nodeMap)

			// Create the complete report structure
			reportData := bson.M{
				"cursor": report.Cursor,
				"node":   nodeMap,
			}

			// Get the node ID for the filter
			var nodeID interface{}
			if nodeMap != nil {
				nodeID = nodeMap["id"]
			}

			updateOp := mongo.NewUpdateOneModel().
				SetFilter(bson.M{"node.id": nodeID}).
				SetUpdate(bson.M{"$set": reportData}).
				SetUpsert(true)

			bulkOps = append(bulkOps, updateOp)
		}

		// Execute bulk operations
		if len(bulkOps) > 0 {
			bulkResult, err := coll.BulkWrite(context.TODO(), bulkOps)
			if err != nil {
				log.Printf("Bulk write error: %v", err)
				time.Sleep(time.Minute * 10)
				continue
			}

			totalUpserted += bulkResult.UpsertedCount
			totalModified += bulkResult.ModifiedCount
			totalMatched += bulkResult.MatchedCount

			fmt.Printf("done: upserted %d/%d, modified %d/%d, matched: %d/%d\n",
				bulkResult.UpsertedCount, totalUpserted,
				bulkResult.ModifiedCount, totalModified,
				bulkResult.MatchedCount, totalMatched,
			)
		}

		// Update cursor for next iteration
		if chainabuseResult.Data.Reports.PageInfo.EndCursor != "" {
			cursor = &chainabuseResult.Data.Reports.PageInfo.EndCursor
		}

		// Check if there are more pages
		if hasNext(chainabuseResult) {
			continue
		} else {
			fmt.Printf("updated to latest, cur: %v\n", cursor)
			time.Sleep(time.Hour)
		}
	}
}
