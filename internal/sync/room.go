package sync

import (
	"encoding/json"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// MessageType represents the type of sync message
type MessageType string

const (
	// Host -> Viewers
	MsgPlay        MessageType = "play"
	MsgPause       MessageType = "pause"
	MsgSeek        MessageType = "seek"
	MsgSyncState   MessageType = "sync_state"
	MsgBuffering   MessageType = "buffering"
	MsgReady       MessageType = "ready"
	MsgAudioChange MessageType = "audio_change"
	MsgSubChange   MessageType = "subtitle_change"
	MsgStreamInfo  MessageType = "stream_info"
	MsgViewerList  MessageType = "viewer_list"
	MsgChat        MessageType = "chat"
	MsgError       MessageType = "error"

	// Viewer -> Host
	MsgRequestSync  MessageType = "request_sync"
	MsgViewerReady  MessageType = "viewer_ready"
	MsgViewerBuffer MessageType = "viewer_buffering"
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
	ID        string
	Name      string
	IsHost    bool
	conn      *websocket.Conn
	send      chan []byte
	room      *Room
	mu        sync.RWMutex
	isReady   bool
	buffering bool
	closed    atomic.Bool
}

// safeSend sends data to client channel without panicking if closed
func (c *Client) safeSend(data []byte) bool {
	if c.closed.Load() {
		return false
	}

	select {
	case c.send <- data:
		return true
	default:
		// Buffer full
		return false
	}
}

// getState returns client state safely
func (c *Client) getState() (isReady, buffering bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.isReady, c.buffering
}

// close safely closes the client
func (c *Client) close() {
	if c.closed.CompareAndSwap(false, true) {
		close(c.send)
	}
}

// Room represents a viewing room with host and viewers
type Room struct {
	ID      string
	host    *Client
	viewers map[string]*Client
	mu      sync.RWMutex

	// Playback state
	playing  bool
	position float64
	lastSync time.Time

	// Stream info
	streamInfo json.RawMessage

	// Broadcast channel
	broadcast chan *Message

	// Close signal
	done   chan struct{}
	closed atomic.Bool
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
			r.mu.RLock()
			hasHost := r.host != nil
			playing := r.playing
			position := r.position
			r.mu.RUnlock()

			if hasHost && playing {
				state := SyncState{
					Playing:   playing,
					Position:  position,
					Timestamp: time.Now().UnixMilli(),
				}

				data, err := json.Marshal(state)
				if err != nil {
					log.Printf("Failed to marshal sync state: %v", err)
					continue
				}

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
		log.Printf("Failed to marshal broadcast message: %v", err)
		return
	}

	r.mu.RLock()
	host := r.host
	viewers := make([]*Client, 0, len(r.viewers))
	for _, c := range r.viewers {
		viewers = append(viewers, c)
	}
	r.mu.RUnlock()

	if host != nil {
		host.safeSend(data)
	}

	for _, client := range viewers {
		client.safeSend(data)
	}
}

// broadcastToViewers sends a message to viewers only
func (r *Room) broadcastToViewers(msg *Message) {
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("Failed to marshal viewer message: %v", err)
		return
	}

	r.mu.RLock()
	viewers := make([]*Client, 0, len(r.viewers))
	for _, c := range r.viewers {
		viewers = append(viewers, c)
	}
	r.mu.RUnlock()

	for _, client := range viewers {
		client.safeSend(data)
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
	r.sendCurrentState(client)

	return client
}

// Leave removes a client from the room
func (r *Room) Leave(client *Client) {
	r.mu.Lock()
	if client.IsHost {
		if r.host == client {
			r.host = nil
		}
		r.playing = false
	} else {
		delete(r.viewers, client.ID)
	}
	r.mu.Unlock()

	client.close()
	r.broadcastViewerList()
}

// broadcastViewerList sends updated viewer list to all clients
func (r *Room) broadcastViewerList() {
	r.mu.RLock()
	viewers := make([]Viewer, 0, len(r.viewers)+1)

	if r.host != nil {
		isReady, _ := r.host.getState()
		viewers = append(viewers, Viewer{
			ID:      r.host.ID,
			Name:    r.host.Name,
			IsHost:  true,
			IsReady: isReady,
		})
	}

	for _, v := range r.viewers {
		isReady, buffering := v.getState()
		viewers = append(viewers, Viewer{
			ID:        v.ID,
			Name:      v.Name,
			IsHost:    false,
			IsReady:   isReady,
			Buffering: buffering,
		})
	}
	r.mu.RUnlock()

	data, err := json.Marshal(viewers)
	if err != nil {
		log.Printf("Failed to marshal viewer list: %v", err)
		return
	}

	select {
	case r.broadcast <- &Message{
		Type:      MsgViewerList,
		Timestamp: time.Now().UnixMilli(),
		Data:      data,
	}:
	default:
		log.Printf("Broadcast channel full, dropping viewer list update")
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

	stateData, err := json.Marshal(state)
	if err != nil {
		log.Printf("Failed to marshal state: %v", err)
		return
	}

	msg := &Message{
		Type:      MsgSyncState,
		Timestamp: time.Now().UnixMilli(),
		Data:      stateData,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("Failed to marshal message: %v", err)
		return
	}

	client.safeSend(data)

	// Also send stream info if available
	if len(streamInfo) > 0 {
		infoMsg := &Message{
			Type:      MsgStreamInfo,
			Timestamp: time.Now().UnixMilli(),
			Data:      streamInfo,
		}
		infoData, err := json.Marshal(infoMsg)
		if err != nil {
			log.Printf("Failed to marshal stream info: %v", err)
			return
		}
		client.safeSend(infoData)
	}
}

// UpdateStreamInfo updates and broadcasts stream info
func (r *Room) UpdateStreamInfo(info json.RawMessage) {
	r.mu.Lock()
	r.streamInfo = info
	r.mu.Unlock()

	select {
	case r.broadcast <- &Message{
		Type:      MsgStreamInfo,
		Timestamp: time.Now().UnixMilli(),
		Data:      info,
	}:
	default:
		log.Printf("Broadcast channel full, dropping stream info update")
	}
}

// Play broadcasts play command
func (r *Room) Play(position float64) {
	r.mu.Lock()
	r.playing = true
	r.position = position
	r.lastSync = time.Now()
	r.mu.Unlock()

	data, err := json.Marshal(map[string]float64{"position": position})
	if err != nil {
		log.Printf("Failed to marshal play data: %v", err)
		return
	}

	select {
	case r.broadcast <- &Message{
		Type:      MsgPlay,
		Timestamp: time.Now().UnixMilli(),
		Data:      data,
	}:
	default:
		log.Printf("Broadcast channel full, dropping play command")
	}
}

// Pause broadcasts pause command
func (r *Room) Pause(position float64) {
	r.mu.Lock()
	r.playing = false
	r.position = position
	r.mu.Unlock()

	data, err := json.Marshal(map[string]float64{"position": position})
	if err != nil {
		log.Printf("Failed to marshal pause data: %v", err)
		return
	}

	select {
	case r.broadcast <- &Message{
		Type:      MsgPause,
		Timestamp: time.Now().UnixMilli(),
		Data:      data,
	}:
	default:
		log.Printf("Broadcast channel full, dropping pause command")
	}
}

// Seek broadcasts seek command
func (r *Room) Seek(position float64) {
	r.mu.Lock()
	r.position = position
	r.lastSync = time.Now()
	r.mu.Unlock()

	data, err := json.Marshal(map[string]float64{"position": position})
	if err != nil {
		log.Printf("Failed to marshal seek data: %v", err)
		return
	}

	select {
	case r.broadcast <- &Message{
		Type:      MsgSeek,
		Timestamp: time.Now().UnixMilli(),
		Data:      data,
	}:
	default:
		log.Printf("Broadcast channel full, dropping seek command")
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
	if !r.closed.CompareAndSwap(false, true) {
		return // Already closed
	}

	close(r.done)

	r.mu.Lock()
	host := r.host
	viewers := make([]*Client, 0, len(r.viewers))
	for _, v := range r.viewers {
		viewers = append(viewers, v)
	}
	r.host = nil
	r.viewers = make(map[string]*Client)
	r.mu.Unlock()

	if host != nil {
		host.conn.Close()
	}
	for _, v := range viewers {
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
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure, websocket.CloseNormalClosure) {
				log.Printf("WebSocket error: %v", err)
			}
			break
		}

		var msg Message
		if err := json.Unmarshal(data, &msg); err != nil {
			log.Printf("Failed to unmarshal message: %v", err)
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
		if err := json.Unmarshal(msg.Data, &buffering); err != nil {
			log.Printf("Failed to unmarshal buffering data: %v", err)
			return
		}
		c.mu.Lock()
		c.buffering = buffering.Buffering
		c.mu.Unlock()
		c.room.broadcastViewerList()

	case MsgChat:
		if len(msg.Data) == 0 {
			return
		}

		var chatData map[string]interface{}
		if err := json.Unmarshal(msg.Data, &chatData); err != nil {
			log.Printf("Failed to unmarshal chat data: %v", err)
			return
		}

		chatData["sender_id"] = c.ID
		chatData["sender_name"] = c.Name
		chatData["is_host"] = c.IsHost

		newData, err := json.Marshal(chatData)
		if err != nil {
			log.Printf("Failed to marshal chat data: %v", err)
			return
		}

		select {
		case c.room.broadcast <- &Message{
			Type:      MsgChat,
			Timestamp: time.Now().UnixMilli(),
			Data:      newData,
		}:
		default:
			log.Printf("Broadcast channel full, dropping chat message")
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
			if _, err := w.Write(message); err != nil {
				w.Close()
				return
			}
			if err := w.Close(); err != nil {
				return
			}

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
