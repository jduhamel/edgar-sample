// Copyright 2012-2015 IQ4Health All rights reserved.

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"runtime"
	"strings"

	mgo "gopkg.in/mgo.v2"

	"github.com/nats-io/nats"
)

func usage() {
	log.Fatalf("Usage: edgar_receive [-nats server (%s)] [-db mongo server (%s)] [-t] <subject> \n", "localhost:27017", nats.DefaultURL)
}

var index = 0

func printMsg(m *nats.Msg, i int) {
	index += 1
	log.Printf("[#%d] Received on [%s]: '%s'\n", i, m.Subject, string(m.Data))
}

func main() {
	var urls = flag.String("nats", nats.DefaultURL, "The nats server URLs (separated by comma)")
	var mgoDB = flag.String("db", "localhost:27017", "Mongo databast URL")
	var showTime = flag.Bool("t", false, "Display timestamps")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		usage()
		fmt.Println("mgoDB", mgoDB)
	}

	opts := nats.DefaultOptions
	opts.Servers = strings.Split(*urls, ",")
	for i, s := range opts.Servers {
		opts.Servers[i] = strings.Trim(s, " ")
	}

	nc, err := opts.Connect()
	if err != nil {
		log.Fatalf("Can't connect: %v\n", err)
	}

	subj, i := args[0], 0

	nc.Subscribe(subj, func(msg *nats.Msg) {
		// ok we need to write this to a mgo database and assume that it's mostly valid ;)
		// we will use the last part of the subj as the collection and the first part as the db.
		// First we convert the Msg into a json obj.
		// then we dump the json into Bson and into mongo.

		var jsonObj map[string]interface{}

		err := json.Unmarshal([]byte(msg.Data), &jsonObj)
		if err != nil {
			// Bad json.
			log.Fatalf("Bad json passed: (%s)\n", msg.Data)
		}

		// ok let's split the msg subject and use part 1 for db and part 2 for collection. throwing away the rest
		foo := strings.Split(subj, ".")

		// Create mgo session
		session, err := mgo.Dial(*mgoDB)
		c := session.Copy().DB(foo[0]).C(foo[1]) // Add the mongo db stuff
		c.Insert(jsonObj)

		printMsg(msg, i)
	})

	log.Printf("Listening on [%s]\n", subj)
	if *showTime {
		log.SetFlags(log.LstdFlags)
	}

	runtime.Goexit()
}
