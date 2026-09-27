package main

import (
	"fmt"
	"log"
	"net/http"
	"pubsub/pubsub"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

var ps = &pubsub.PubSub{}

func generateId() string {
	return uuid.New().String()
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("upgrade error:", err)
		return
	}

	client := &pubsub.Client{
		Id:         generateId(),
		Connection: conn,
	}

	ps.AddClient(client)

	defer func() {
		ps.RemoveClient(client)
		conn.Close()
	}()

	fmt.Println("new client:", client.Id)

	for {
		messageType, payload, err := conn.ReadMessage()
		if err != nil {
			log.Println("client disconnected:", err)
			return
		}

		ps.HandleReceiveMessage(
			client,
			messageType,
			payload,
		)
	}
}

func main() {
	fmt.Println("server running on :3000")

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "static/index.html")
	})

	http.HandleFunc("/ws", wsHandler)

	err := http.ListenAndServe(":3000", nil)
	if err != nil {
		log.Fatal(err)
	}
}
