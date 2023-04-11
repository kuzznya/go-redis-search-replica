package main

import (
	"bufio"
	"context"
	"encoding/json"
	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
)

func main() {
	client := redis.NewClient(&redis.Options{})

	err := client.FlushDB(context.TODO()).Err()
	if err != nil {
		log.WithError(err).Panicln("Failed to flush DB")
	}

	f, err := os.Open("News_Category_Dataset_v3.json")

	defer func() { _ = f.Close() }()

	if err != nil {
		log.WithError(err).Panicf("Failed to open file")
	}
	r := bufio.NewReader(f)

	i := 0
	for {
		line, _, err := r.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.WithError(err).Panicln("Failed to read line")
		}

		doc := make(map[string]string)

		err = json.Unmarshal(line, &doc)
		if err != nil {
			log.WithError(err).Panicf("Failed to deserialize value: %s", line)
		}

		err = client.HSet(context.TODO(), doc["link"], doc).Err()
		if err != nil {
			log.WithError(err).Panicf("Failed to save document %s", doc["link"])
		}

		i++
	}

	log.Infof("Loaded %d documents", i)
}
