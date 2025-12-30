package sync

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// MessageType represents the type of sync message
type MessageType string

const (
	// Host -> Viewers
	MsgPlay          MessageType = "play"
	MsgPause         MessageType = "pause"
	MsgSeek          MessageType = "seek"
	MsgSyncState     MessageType = "sync_state"
	MsgBuffering     MessageType = "buffering"
	MsgReady         MessageType = "ready"
	MsgAudioChange   MessageType = "audio_change"
	MsgSubChange     MessageType = "subtitle_change"
	MsgStreamInfo    MessageType = "stream_info"
	MsgViewerList    MessageType = "viewer_list"
	MsgChat          MessageType = "chat"
	MsgError         MessageType = "error"

	// Viewer -> Host
	MsgRequestSync   MessageType = "request_sync"
	MsgViewerReady   MessageType = "viewer_ready"
	MsgViewerBuffer  MessageType = "viewer_buffering"
)

// Message represents a sync message
type Message struct {
	Type      MessageType     `json:"type"`
	Timestamp int64           `json:"timestamp"`
	Data      json.RawMessage `json:"data,omitempty"`
}

// SyncState contains playback state for synchronization
type SyncState struct {
	Playing   bool    `json:"playing"`
	Position  float64 `json:"position"`
	Timestamp int64   `json:"timestamp"`
}

// Viewer represents a connected viewer
type Viewer struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	IsHost    bool   `json:"is_host"`
	IsReady   bool   `json:"is_ready"`
	Buffering bool   `json:"buffering"`
}

// Client represents a WebSocket client
type Client struct {
	ID       string
	Name     string
	IsHost   bool
	conn     *websocket.Conn
	send     chan []byte
	room     *Room
	mu       sync.Mutex
	isReady  bool
	buffering bool
}

// Room represents a viewing room with host and viewers
type Room struct {
	ID      string
	host    *Client
	viewers map[string]*Client
	mu      sync.RWMutex

	// Playback state
	playing   bool
	position  float64
	lastSync  time.Time

	// Stream info
	streamInfo json.RawMessage

	// Broadcast channel
	broadcast chan *Message

	// Close signal
	done chan struct{}
}

// NewRoom creates a new viewing room
func NewRoom(id string) *Room {
	r := &Room{
		ID:        id,
		viewers:   make(map[string]*Client),
		broadcast: make(chan *Message, 256),
		done:      make(chan struct{}),
	}

	go r.run()
	return r
}

// run handles room message broadcasting
func (r *Room) run() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case msg := <-r.broadcast:
			r.broadcastMessage(msg)

		case <-ticker.C:
			// Periodic sync state broadcast
			if r.host != nil && r.playing {
				r.mu.RLock()
				state := SyncState{
					Playing:   r.playing,
					Position:  r.position,
					Timestamp: time.Now().UnixMilli(),
				}
				r.mu.RUnlock()

				data, _ := json.Marshal(state)
				r.broadcastToViewers(&Message{
					Type:      MsgSyncState,
					Timestamp: time.Now().UnixMilli(),
					Data:      data,
				})
			}

		case <-r.done:
			return
		}
	}
}

// broadcastMessage sends a message to all clients
func (r *Room) broadcastMessage(msg *Message) {
	data, err := json.Marshal(msg)
	if err != nil {
		return
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.host != nil {
		r.host.send <- data
	}

	for _, client := range r.viewers {
		select {
		case client.send <- data:
		default:
			// Client buffer full, skip
		}
	}
}

// broadcastToViewers sends a message to viewers only
func (r *Room) broadcastToViewers(msg *Message) {
	data, err := json.Marshal(msg)
	if err != nil {
		return
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, client := range r.viewers {
		select {
		case client.send <- data:
		default:
		}
	}
}

// JoinAsHost joins the room as the host
func (r *Room) JoinAsHost(conn *websocket.Conn, name string) *Client {
	client := &Client{
		ID:     uuid.New().String(),
		Name:   name,
		IsHost: true,
		conn:   conn,
		send:   make(chan []byte, 256),
		room:   r,
	}

	r.mu.Lock()
	r.host = client
	r.mu.Unlock()

	r.broadcastViewerList()

	return client
}

// JoinAsViewer joins the room as a viewer
func (r *Room) JoinAsViewer(conn *websocket.Conn, name string) *Client {
	client := &Client{
		ID:     uuid.New().String(),
		Name:   name,
		IsHost: false,
		conn:   conn,
		send:   make(chan []byte, 256),
		room:   r,
	}

	r.mu.Lock()
	r.viewers[client.ID] = client
	r.mu.Unlock()

	r.broadcastViewerList()

	// Send current state to new viewer
	r.sendCurrentState(client)

	return client
}

// Leave removes a client from the room
func (r *Room) Leave(client *Client) {
	r.mu.Lock()
	if client.IsHost {
		r.host = nil
		// Pause playback when host leaves
		r.playing = false
	} else {
		delete(r.viewers, client.ID)
	}
	r.mu.Unlock()

	close(client.send)
	r.broadcastViewerList()
}

// broadcastViewerList sends updated viewer list to all clients
func (r *Room) broadcastViewerList() {
	r.mu.RLock()
	viewers := make([]Viewer, 0, len(r.viewers)+1)

	if r.host != nil {
		viewers = append(viewers, Viewer{
			ID:     r.host.ID,
			Name:   r.host.Name,
			IsHost: true,
			IsReady: r.host.isReady,
		})
	}

	for _, v := range r.viewers {
		viewers = append(viewers, Viewer{
			ID:        v.ID,
			Name:      v.Name,
			IsHost:    false,
			IsReady:   v.isReady,
			Buffering: v.buffering,
		})
	}
	r.mu.RUnlock()

	data, _ := json.Marshal(viewers)
	r.broadcast <- &Message{
		Type:      MsgViewerList,
		Timestamp: time.Now().UnixMilli(),
		Data:      data,
	}
}

// sendCurrentState sends current playback state to a client
func (r *Room) sendCurrentState(client *Client) {
	r.mu.RLock()
	state := SyncState{
		Playing:   r.playing,
		Position:  r.position,
		Timestamp: time.Now().UnixMilli(),
	}
	streamInfo := r.streamInfo
	r.mu.RUnlock()

	stateData, _ := json.Marshal(state)
	msg := &Message{
		Type:      MsgSyncState,
		Timestamp: time.Now().UnixMilli(),
		Data:      stateData,
	}

	data, _ := json.Marshal(msg)
	client.send <- data

	// Also send stream info if available
	if len(streamInfo) > 0 {
		infoMsg := &Message{
			Type:      MsgStreamInfo,
			Timestamp: time.Now().UnixMilli(),
			Data:      streamInfo,
		}
		infoData, _ := json.Marshal(infoMsg)
		client.send <- infoData
	}
}

// UpdateStreamInfo updates and broadcasts stream info
func (r *Room) UpdateStreamInfo(info json.RawMessage) {
	r.mu.Lock()
	r.streamInfo = info
	r.mu.Unlock()

	r.broadcast <- &Message{
		Type:      MsgStreamInfo,
		Timestamp: time.Now().UnixMilli(),
		Data:      info,
	}
}

// Play broadcasts play command
func (r *Room) Play(position float64) {
	r.mu.Lock()
	r.playing = true
	r.position = position
	r.lastSync = time.Now()
	r.mu.Unlock()

	data, _ := json.Marshal(map[string]float64{"position": position})
	r.broadcast <- &Message{
		Type:      MsgPlay,
		Timestamp: time.Now().UnixMilli(),
		Data:      data,
	}
}

// Pause broadcasts pause command
func (r *Room) Pause(position float64) {
	r.mu.Lock()
	r.playing = false
	r.position = position
	r.mu.Unlock()

	data, _ := json.Marshal(map[string]float64{"position": position})
	r.broadcast <- &Message{
		Type:      MsgPause,
		Timestamp: time.Now().UnixMilli(),
		Data:      data,
	}
}

// Seek broadcasts seek command
func (r *Room) Seek(position float64) {
	r.mu.Lock()
	r.position = position
	r.lastSync = time.Now()
	r.mu.Unlock()

	data, _ := json.Marshal(map[string]float64{"position": position})
	r.broadcast <- &Message{
		Type:      MsgSeek,
		Timestamp: time.Now().UnixMilli(),
		Data:      data,
	}
}

// UpdatePosition updates current position (from host)
func (r *Room) UpdatePosition(position float64) {
	r.mu.Lock()
	r.position = position
	r.mu.Unlock()
}

// GetViewerCount returns the number of connected viewers
func (r *Room) GetViewerCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.viewers)
}

// HasHost returns true if room has a host
func (r *Room) HasHost() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.host != nil
}

// Close closes the room
func (r *Room) Close() {
	close(r.done)

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.host != nil {
		r.host.conn.Close()
	}

	for _, v := range r.viewers {
		v.conn.Close()
	}
}

// HandleMessages handles incoming messages from a client
func (c *Client) HandleMessages() {
	defer func() {
		c.room.Leave(c)
		c.conn.Close()
	}()

	c.conn.SetReadLimit(65536)
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket error: %v", err)
			}
			break
		}

		var msg Message
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}

		c.handleMessage(&msg)
	}
}

// handleMessage processes a single message
func (c *Client) handleMessage(msg *Message) {
	switch msg.Type {
	case MsgRequestSync:
		c.room.sendCurrentState(c)

	case MsgViewerReady:
		c.mu.Lock()
		c.isReady = true
		c.mu.Unlock()
		c.room.broadcastViewerList()

	case MsgViewerBuffer:
		var buffering struct {
			Buffering bool `json:"buffering"`
		}
		json.Unmarshal(msg.Data, &buffering)
		c.mu.Lock()
		c.buffering = buffering.Buffering
		c.mu.Unlock()
		c.room.broadcastViewerList()

	case MsgChat:
		if len(msg.Data) > 0 {
			// Add sender info and broadcast
			var chatData map[string]interface{}
			json.Unmarshal(msg.Data, &chatData)
			chatData["sender_id"] = c.ID
			chatData["sender_name"] = c.Name
			chatData["is_host"] = c.IsHost

			newData, _ := json.Marshal(chatData)
			c.room.broadcast <- &Message{
				Type:      MsgChat,
				Timestamp: time.Now().UnixMilli(),
				Data:      newData,
			}
		}
	}
}

// WritePump pumps messages from the send channel to the WebSocket
func (c *Client) WritePump() {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)
			w.Close()

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// RoomManager manages multiple rooms
type RoomManager struct {
	rooms map[string]*Room
	mu    sync.RWMutex
}

// NewRoomManager creates a new room manager
func NewRoomManager() *RoomManager {
	return &RoomManager{
		rooms: make(map[string]*Room),
	}
}

// GetOrCreateRoom gets or creates a room by ID
func (rm *RoomManager) GetOrCreateRoom(id string) *Room {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if room, ok := rm.rooms[id]; ok {
		return room
	}

	room := NewRoom(id)
	rm.rooms[id] = room
	return room
}

// GetRoom gets a room by ID
func (rm *RoomManager) GetRoom(id string) *Room {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.rooms[id]
}

// DeleteRoom removes a room
func (rm *RoomManager) DeleteRoom(id string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if room, ok := rm.rooms[id]; ok {
		room.Close()
		delete(rm.rooms, id)
	}
}
