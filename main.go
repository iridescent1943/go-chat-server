package main

import (
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

type ChatServer struct {
	mu      sync.Mutex
	clients map[*websocket.Conn]bool
}

func newChatServer() *ChatServer {
	return &ChatServer{clients: make(map[*websocket.Conn]bool)}
}

func (s *ChatServer) broadcast(msg []byte, sender *websocket.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for conn := range s.clients {
		if conn != sender {
			if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				log.Println("write error:", err)
			}
		}
	}
}

func (s *ChatServer) addClient(conn *websocket.Conn) {
	s.mu.Lock()
	s.clients[conn] = true
	s.mu.Unlock()
}

func (s *ChatServer) removeClient(conn *websocket.Conn) {
	s.mu.Lock()
	delete(s.clients, conn)
	s.mu.Unlock()
	conn.Close()
}

func (s *ChatServer) handleConn(conn *websocket.Conn) {
	defer s.removeClient(conn)
	s.addClient(conn)

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			break
		}
		s.broadcast(msg, conn)
	}
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func main() {
	server := newChatServer()

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("upgrade error:", err)
			return
		}
		server.handleConn(conn)
	})

	http.Handle("/", http.FileServer(http.Dir("static")))

	log.Println("chat server listening on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}
