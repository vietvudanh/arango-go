package main

import (
	"arango"
	"bufio"
	"github.com/arangodb/go-driver"
	"log"
	"os"
	"strings"

	"github.com/gammazero/workerpool"
)

func main() {
	file, err := os.Open("/Users/vietvu/workspace/data/facebook_clean_data/artist_edges.csv")
	if err != nil {
	    log.Fatal(err)
	}
	defer func(){
	    err := file.Close()
	    if err != nil {
	        log.Fatalln("cannot close file", file.Name())
	    }
	}()

	c := arango.GetClient()
	db, err := c.Database(nil, "facebook")

	//
	var collection driver.Collection
	if ok, _ := db.CollectionExists(nil, "artist") ; ok {
		collection, _ = db.Collection(nil, "artist")
	} else {
		collection, _ = db.CreateCollection(nil, "artist", nil)
	}
	collection.Truncate(nil)

	scan := bufio.NewScanner(file)
	vertices := make(map[string]bool)
	edges := make([][]string, 0, 100)
	for scan.Scan() {
	  	line := scan.Text()
		row := strings.Split(line, ",")
		from, to := row[0], row[1]
		vertices[from] = true
		vertices[to] = true

		edges = append(edges, []string{from, to})
	}

	log.Println("vertices", len(vertices))
	log.Println("edges", len(edges))

	if err := scan.Err(); err != nil {
		log.Fatal(err)
	}

	// vertices
	wp := workerpool.New(4)
	buffer := make([]map[string]string, 0, 10000)
	log.Println(len(buffer))
	i := 0
	for v, _ := range vertices {
		buffer = append(buffer, map[string]string{
			"_key": v,
		})

		if len(buffer) == 10000 {
			insert := make([]map[string]string, 0, 10000)
			copy(insert, buffer)
			log.Println(i, "insert vertices in ::", len(insert))
			i := i
			wp.Submit(func() {
				log.Println("w vertices", i)
				collection.CreateDocuments(nil, insert)
			})
			buffer = []map[string]string{}
		}
		i += 1
	}
	log.Println("insert vertices out::", len(buffer))
	collection.CreateDocuments(nil, buffer)
	wp.StopWait()

	// edges
	var graph driver.Graph
	if ok, _ := db.GraphExists(nil, "graph_artist") ; ok {
		graph, _ = db.Graph(nil, "graph_artist")
	} else {
		graph, _ = db.CreateGraph(nil, "graph_artist", nil)
	}


	var edgeCollection driver.Collection
	if ok, _ := graph.EdgeCollectionExists(nil, "artist_edge") ; ok {
		edgeCollection, _, _ = graph.EdgeCollection(nil, "artist_edge")
	} else {
		edgeCollection, _ = graph.CreateEdgeCollection(nil, "artist_edge", driver.VertexConstraints{
			From: []string{"artist"},
			To:   []string{"artist"},
		})
	}
	edgeCollection.Truncate(nil)

	wp = workerpool.New(4)
	bufferEdges := make([]map[string]string, 0, 10000)
	for i, edge := range edges {
		fIdNode, tIdNode := "artist/" + edge[0], "artist/" + edge[1]
		bufferEdges = append(bufferEdges, map[string]string{
			"_from": fIdNode,
			"_to": tIdNode,
		})

		if len(bufferEdges) == 10000 {
			insert := make([]map[string]string, 0, 10000)
			copy(insert, bufferEdges)
			log.Println(i, "insert edges in::", len(insert))
			wp.Submit(func() {
				log.Println("w edges", i)
				edgeCollection.CreateDocuments(nil, insert)
			})
			bufferEdges = []map[string]string{}
		}
	}
	log.Println("insert edges out::", len(bufferEdges))
	edgeCollection.CreateDocuments(nil, bufferEdges)
	wp.StopWait()

	log.Println("done")
}
