// +build ignore

// This example demonstrates indexing data in bulk.
//
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch"
	"github.com/elastic/go-elasticsearch/esapi"
)

type Article struct {
	ID        int       `json:"id"`
	Title     string    `json:"title"`
	Body      string    `json:"body"`
	Published time.Time `json:"published"`
	Author    Author    `json:"author"`
}

type Author struct {
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
}

func main() {
	log.SetFlags(0)

	var (
		_     = fmt.Print
		count = 1000
		batch = 75

		buf bytes.Buffer
		res *esapi.Response
		ers map[string]interface{}
		err error

		articles   []*Article
		indexName  = "articles"
		numErrors  int
		numIndexed int
		currBatch  int
	)

	type bulkErrorResponse struct{}

	es, err := elasticsearch.NewDefaultClient()
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}

	// Generate the articles collection
	//
	for i := 1; i < count+1; i++ {
		articles = append(articles, &Article{
			ID:        i,
			Title:     strings.Join([]string{"Title", strconv.Itoa(i)}, " "),
			Body:      "Lorem ipsum...",
			Published: time.Now().AddDate(0, 0, i),
			Author: Author{
				FirstName: "John",
				LastName:  "Smith",
			},
		})
	}
	log.Printf("Generated %d articles", len(articles))

	// Re-create the index
	//
	if _, err = es.Indices.Delete([]string{indexName}); err != nil {
		log.Fatalf("Cannot delete index: %s", err)
	}
	res, err = es.Indices.Delete([]string{indexName})
	if err != nil {
		log.Fatalf("Cannot delete index: %s", err)
	}
	res, err = es.Indices.Create(indexName)
	if err != nil {
		log.Fatalf("Cannot create index: %s", err)
	}
	if res.IsError() {
		log.Fatalf("Cannot create index: %s", res)
	}

	// Start looping over collection
	//
	for i, a := range articles {
		currBatch = i / batch
		if i == count-1 {
			currBatch++
		}

		// Prepare meta data
		//
		meta := []byte(fmt.Sprintf(`{ "index" : { "_index" : "%s", "_id" : "%d" } }%s`, indexName, a.ID, "\n"))
		// fmt.Printf("%s", meta) // <-- Uncomment to see the payload

		// Encode article to JSON
		//
		data, err := json.Marshal(a)
		if err != nil {
			log.Fatalf("Cannot encode article %d: %s", a.ID, err)
		}

		// Append newline to JSON payload
		data = append(data, "\n"...) // <-- Comment out to trigger failure for batch
		// fmt.Printf("%s", data) // <-- Uncomment to see the payload

		// // Uncomment next block to trigger indexing errors -->
		if a.ID == 11 || a.ID == 101 {
			data = []byte(`{"published" : "INCORRECT"}` + "\n")
		}

		// Append meta data and payload to the buffer (ignoring write errors)
		//
		buf.Grow(len(meta) + len(data))
		buf.Write(meta)
		buf.Write(data)

		// When a threshold is reached, execute the Bulk() request with body from buffer
		//
		if i > 0 && i%batch == 0 || i == count-1 {
			log.Printf("Batch %-2d of %d", currBatch, (count/batch)+1)

			res, err := es.Bulk(bytes.NewReader(buf.Bytes()), es.Bulk.WithIndex(indexName))
			if err != nil {
				log.Fatalf("Failure indexing batch %d: %s", currBatch, err)
			}
			// If the whole request failed, print error
			if res.IsError() {
				numErrors += batch
				if err := json.NewDecoder(res.Body).Decode(&ers); err != nil {
					log.Fatalf("Failure to to parse response body: %s", err)
				} else {
					log.Printf("Error: [%d] %s: %s",
						res.StatusCode,
						ers["error"].(map[string]interface{})["type"],
						ers["error"].(map[string]interface{})["reason"],
					)
				}
				// A successful response might still contain errors for particular documents.
			} else {
				if err := json.NewDecoder(res.Body).Decode(&ers); err != nil {
					log.Fatalf("Failure to to parse response body: %s", err)
				} else {
					// Print the response status and error information.
					if _, ok := ers["errors"].(bool); ok {
						for _, d := range ers["items"].([]interface{}) {
							docID := d.(map[string]interface{})["index"].(map[string]interface{})["_id"]
							status := d.(map[string]interface{})["index"].(map[string]interface{})["status"].(float64)

							if status > 201 {
								numErrors++

								reason := d.(map[string]interface{})["index"].(map[string]interface{})["error"].(map[string]interface{})["caused_by"].(map[string]interface{})["caused_by"].(map[string]interface{})["reason"]

								log.Printf("Error: documentID=%-3s [%.0f]: %s",
									docID,
									status,
									reason,
								)
							} else {
								numIndexed++
							}
						}
					}
				}
			}
			buf.Reset()
		}
	}

	log.Println(strings.Repeat("=", 80))

	// Report results: number of indexed docs, number of errors, duration
	if numErrors > 0 {
		log.Fatalf("Indexed [%d] documents, failed [%d]\n\n", numIndexed, numErrors)
	} else {
		log.Printf("Sucessfuly indexed [%d] documents", numIndexed)
	}
}
