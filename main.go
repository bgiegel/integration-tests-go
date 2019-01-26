package main

import (
	"context"
	"encoding/json"
	"fmt"
	"gopkg.in/olivere/elastic.v6"
	"log"
	"os"
)

const ElasticURL = "ELASTIC_URL"

// Tweet is a structure used for serializing/deserializing data in Elasticsearch.
type Tweet struct {
	User    string `json:"user"`
	Message string `json:"message"`
}

const mapping = `
{
	"settings":{
		"number_of_shards": 1,
		"number_of_replicas": 0
	},
	"mappings":{
		"tweet":{
			"properties":{
				"user":{
					"type":"keyword"
				},
				"message":{
					"type":"text",
					"store": true,
					"fielddata": true
				}
			}
		}
	}
}`

type App struct {
	client *elastic.Client
}

func main() {
	ctx := context.Background()
	app := NewApp(connectToEs())

	app.create(ctx)

	app.index(ctx)

	app.get(ctx)

	app.deleteIndex(ctx)
}

func NewApp(es *elastic.Client) App {
	return App{client: es}
}

func (a App) deleteIndex(ctx context.Context) {
	// Delete an index.
	deleteIndex, err := a.client.DeleteIndex("twitter").Do(ctx)
	if err != nil {
		// Handle error
		panic(err)
	}
	if deleteIndex.Acknowledged {
		fmt.Println("index deleted")
	}
}

func (a App) get(ctx context.Context) Tweet {
	// Get tweet with specified ID
	get1, err := a.client.Get().
		Index("twitter").
		Type("tweet").
		Id("1").
		Do(ctx)
	if err != nil {
		// Handle error
		panic(err)
	}
	if get1.Found {
		var t Tweet
		err := json.Unmarshal(*get1.Source, &t)
		if err != nil {
			panic(err)
		}
		return t
	}
	return Tweet{}
}

func (a App) index(ctx context.Context) {
	// Index a tweet (using JSON serialization)
	tweet1 := Tweet{User: "olivere", Message: "Take Five"}
	put1, err := a.client.Index().
		Index("twitter").
		Type("tweet").
		Id("1").
		BodyJson(tweet1).
		Do(ctx)
	if err != nil {
		// Handle error
		panic(err)
	}
	fmt.Printf("Indexed tweet %s to index %s, type %s\n", put1.Id, put1.Index, put1.Type)
}

func (a App) create(ctx context.Context) {
	// Use the IndexExists service to check if a specified index exists.
	exists, err := a.client.IndexExists("twitter").Do(ctx)
	if err != nil {
		// Handle error
		panic(err)
	}
	if !exists {
		// Create a new index.
		createIndex, err := a.client.CreateIndex("twitter").BodyString(mapping).Do(ctx)
		if err != nil {
			// Handle error
			panic(err)
		}
		if createIndex.Acknowledged {
			fmt.Println("Index twitter created")
		}
	}
}

func connectToEs() *elastic.Client {
	esURL := os.Getenv(ElasticURL)
	log.Print("connecting to ES @ ", esURL)
	client, err := elastic.NewClient(elastic.SetURL(esURL))
	if err != nil {
		panic(err)
	}
	return client
}
