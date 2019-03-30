// +build ignore

// This example demonstrates indexing how to index documents using the
// Bulk API [https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html].
//
//
package main

import (
	"bytes"
	"encoding/json"
	"flag"
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

var (
	_     = fmt.Print
	count int
	batch int
)

func init() {
	flag.IntVar(&count, "count", 1000, "Number of documents to generate")
	flag.IntVar(&batch, "batch", 75, "Number of documents to send in one batch")
	flag.Parse()
}

func main() {
	log.SetFlags(0)

	type bulkResponse struct {
		Errors bool `json:"errors"`
		Items  []struct {
			Index struct {
				ID     string `json:"_id"`
				Result string `json:"result"`
				Status int    `json:"status"`
				Error  struct {
					Type   string `json:"type"`
					Reason string `json:"reason"`
					Cause  struct {
						Type   string `json:"type"`
						Reason string `json:"reason"`
					} `json:"caused_by"`
				} `json:"error"`
			} `json:"index"`
		} `json:"items"`
	}

	var (
		buf bytes.Buffer
		res *esapi.Response
		err error
		raw map[string]interface{}
		blk *bulkResponse

		articles  []*Article
		indexName = "articles"

		numItems   int
		numErrors  int
		numIndexed int
		currBatch  int
		numBatches int
	)

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
	log.Printf("> Generated %d articles", len(articles))

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

	if count%batch == 0 {
		numBatches = (count / batch)
	} else {
		numBatches = (count / batch) + 1
	}

	start := time.Now().UTC()

	// Start looping over collection
	//
	for i, a := range articles {
		numItems++

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
		// if a.ID == 11 || a.ID == 101 {
		// 	data = []byte(`{"published" : "INCORRECT"}` + "\n")
		// }

		// Append meta data and payload to the buffer (ignoring write errors)
		//
		buf.Grow(len(meta) + len(data))
		buf.Write(meta)
		buf.Write(data)

		// When a threshold is reached, execute the Bulk() request with body from buffer
		//
		if i > 0 && i%batch == 0 || i == count-1 {
			log.Printf("> Batch %-2d of %d", currBatch, numBatches)

			res, err = es.Bulk(bytes.NewReader(buf.Bytes()), es.Bulk.WithIndex(indexName))
			if err != nil {
				log.Fatalf("Failure indexing batch %d: %s", currBatch, err)
			}
			// If the whole request failed, print error
			if res.IsError() {
				numErrors += numItems
				if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
					log.Fatalf("Failure to to parse response body: %s", err)
				} else {
					log.Printf("  Error: [%d] %s: %s",
						res.StatusCode,
						raw["error"].(map[string]interface{})["type"],
						raw["error"].(map[string]interface{})["reason"],
					)
				}
				// A successful response might still contain errors for particular documents.
			} else {
				if err := json.NewDecoder(res.Body).Decode(&blk); err != nil {
					log.Fatalf("Failure to to parse response body: %s", err)
				} else {
					for _, d := range blk.Items {
						if d.Index.Status > 201 {
							numErrors++

							// Print the response status and error information.
							log.Printf("  Error: [%d]: %s: %s: %s: %s",
								d.Index.Status,
								d.Index.Error.Type,
								d.Index.Error.Reason,
								d.Index.Error.Cause.Type,
								d.Index.Error.Cause.Reason,
							)
						} else {
							numIndexed++
						}
					}
				}
			}
			buf.Reset()
			numItems = 0
		}
	}

	log.Println(strings.Repeat("=", 80))

	dur := time.Since(start)

	// Report results: number of indexed docs, number of errors, duration
	if numErrors > 0 {
		log.Fatalf(
			"Indexed [%d] documents with [%d] errors in %s (%.0f docs/sec)",
			numIndexed,
			numErrors,
			dur.Truncate(time.Millisecond),
			1000.0/float64(int64(dur)/int64(time.Millisecond))*float64(numIndexed),
		)
	} else {
		log.Printf(
			"Sucessfuly indexed [%d] documents in %s (%.0f docs/sec)",
			numIndexed,
			dur.Truncate(time.Millisecond),
			1000.0/float64(int64(dur)/int64(time.Millisecond))*float64(numIndexed),
		)
	}
}
