package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/gridfs"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var client *mongo.Client
var dbname = "bama"
var dburl = "mongodb://bama:1234567890@cluster1-shard-00-00-xnpza.mongodb.net:27017,cluster1-shard-00-01-xnpza.mongodb.net:27017,cluster1-shard-00-02-xnpza.mongodb.net:27017/test?ssl=true&replicaSet=Cluster1-shard-0&authSource=admin&retryWrites=true&w=majority&ssl=true"

// Status struct
type Status struct {
	Key         string `json:"key"`
	ErrorStatus int    `json:"errorStatus"`
}

// FileMetadata struct
type FileMetadata struct {
	Key        string
	Name       string
	Size       int
	Type       string
	TotalChunk int
}

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var seededRand *rand.Rand = rand.New(
	rand.NewSource(time.Now().UnixNano()))

func stringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func generateKey(length int) string {
	return stringWithCharset(length, charset)
}

func main() {
	// initiate mongodb connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clientOptions := options.Client().ApplyURI(dburl)
	client, _ = mongo.Connect(ctx, clientOptions)
	log.Println("Connected to mongodb")

	router := mux.NewRouter()
	router.HandleFunc("/upload/{key}/{chunk_number}", uploadFile).Methods("POST")
	router.HandleFunc("/abort/{key}", abortUpload).Methods("POST")
	router.HandleFunc("/finish/{key}", finishUpload).Methods("POST")
	router.HandleFunc("/download/{key}", downloadFile).Methods("GET")

	http.ListenAndServe(":8000", router)
}

func downloadFile(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	key := params["key"]

	var file FileMetadata

	collection := client.Database(dbname).Collection("fileMetadata")

	log.Println(key)

	filter := bson.D{{"key", key}}
	err := collection.FindOne(context.TODO(), filter).Decode(&file)

	if err != nil {
		http.NotFound(w, r)
		return
	}

	bucket, err := gridfs.NewBucket(
		client.Database(dbname),
	)

	if err != nil {
		log.Fatal(err)
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		panic("expected http.ResponseWriter to be an http.Flusher")
	}

	fileSize := fmt.Sprintf("%d", file.Size)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Type", file.Type)
	w.Header().Set("Content-Length", fileSize)
	w.Header().Set("Content-Disposition", "attachment; filename="+file.Name)
	for i := 0; i <= file.TotalChunk; i++ {
		// fmt.Fprintf(w, "Chunk #%d\n", i)
		var buf bytes.Buffer
		datakey := fmt.Sprintf("%s.%d", file.Key, i)
		dStream, err := bucket.DownloadToStreamByName(datakey, &buf)
		if err != nil {
			return
		}
		log.Printf("File size to download: %v\n", dStream)
		buf.WriteTo(w)
		flusher.Flush() // Trigger "chunked" encoding and send a chunk...
		time.Sleep(500 * time.Millisecond)
	}

}

func finishUpload(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	var status Status
	status.ErrorStatus = int(0)

	params := mux.Vars(r)
	key := params["key"]

	status.Key = key
	json.NewEncoder(w).Encode(status)
}

func abortUpload(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	var status Status
	status.ErrorStatus = int(0)

	params := mux.Vars(r)
	key := params["key"]

	status.Key = key
	json.NewEncoder(w).Encode(status)
}

func uploadFile(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	var status Status
	status.ErrorStatus = int(900)

	params := mux.Vars(r)
	key := params["key"]
	chunkNumber := params["chunk_number"]

	fileName := r.URL.Query().Get("filename")
	fileSize, _ := strconv.Atoi(r.URL.Query().Get("filesize"))
	totalChunk, _ := strconv.Atoi(r.URL.Query().Get("totalchunk"))
	mimeType := r.URL.Query().Get("mimetype")

	if key == "0" {
		key = fmt.Sprintf("%s%s", generateKey(10), fileName)
		collection := client.Database(dbname).Collection("fileMetadata")
		file := FileMetadata{key, fileName, fileSize, mimeType, totalChunk}
		result, err := collection.InsertOne(context.TODO(), file)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		log.Println(result)
	}

	status.Key = key

	log.Printf("key id: %s\n", key)
	log.Printf("chunk number: %s\n", chunkNumber)

	byts, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Fatal(err)
		json.NewEncoder(w).Encode(status)
	}

	if byts == nil {
		log.Fatal("Bytes data is null")
		json.NewEncoder(w).Encode(status)
	}

	bucket, err := gridfs.NewBucket(
		client.Database(dbname),
	)

	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
	uploadStream, err := bucket.OpenUploadStream(
		status.Key + "." + chunkNumber,
	)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer uploadStream.Close()

	dataSize, err := uploadStream.Write(byts)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
	log.Printf("Write file to DB was successful. File size: %d M\n", dataSize)

	status.ErrorStatus = int(0)
	json.NewEncoder(w).Encode(status)
}
