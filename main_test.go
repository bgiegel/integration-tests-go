package main

import (
	"context"
	"fmt"
	"gopkg.in/olivere/elastic.v6"
	"gopkg.in/ory-am/dockertest.v2"
	"log"
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	defer connectToElastic().KillRemove()

	m.Run()
}

func connectToElastic() dockertest.ContainerID {
	c, ip, port := elasticContainer()
	elasticUrl := fmt.Sprintf("http://%s:%d", ip, port)
	err := dockertest.ConnectToCustomContainer(fmt.Sprintf("%v:%v", ip, port), 15, time.Millisecond*500, func(url string) bool {
		_, err := elastic.NewClient(elastic.SetURL(elasticUrl))
		if err != nil {
			fmt.Println("Trying to connect to ES. Please wait...")
			return false
		}
		return true
	})
	if err != nil {
		log.Fatalf("Could not connect to database: %s", err)
	}
	err = os.Setenv(ElasticURL, elasticUrl)
	if err != nil {
		log.Println("unable to set env var: ",err)
	}
	return c
}

func elasticContainer() (dockertest.ContainerID, string, int) {
	dockerImage := "docker.elastic.co/elasticsearch/elasticsearch:6.5.4"
	exposedPort := 9200
	envArgs := `-e "discovery.type=single-node"`
	c, ip, port, err := dockertest.SetupCustomContainer(
		dockerImage,
		exposedPort,
		time.Second,
		envArgs)
	if err != nil {
		log.Fatalf("Could not setup container: %s", err)
	}
	return c, ip, port
}

func TestGetTweet(t *testing.T) {
	// given
	ctx := context.Background()
	app := NewApp(connectToEs())
	app.create(ctx)
	app.index(ctx)
	expected := Tweet{User: "olivere", Message: "Take Five"}

	//when
	actual := app.get(ctx)

	//then
	if actual != expected {
		t.Fatalf("Expected %+v got: %+v", expected, actual)
	}
}
