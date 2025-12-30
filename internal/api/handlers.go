package api

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/user/torrent-view/internal/stream"
	syncpkg "github.com/user/torrent-view/internal/sync"
)

// Server represents the HTTP API server
type Server struct {
	streamMgr   *stream.Manager
	roomMgr     *syncpkg.RoomManager
	room        *syncpkg.Room
	upgrader    websocket.Upgrader
	router      *mux.Router
}

// NewServer creates a new API server
func NewServer(streamMgr *stream.Manager) *Server {
	s := &Server{
		streamMgr: streamMgr,
		roomMgr:   syncpkg.NewRoomManager(),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow all origins for development
			},
		},
	}

	// Create default room
	s.room = s.roomMgr.GetOrCreateRoom("default")

	s.setupRoutes()
	return s
}

// setupRoutes configures HTTP routes
func (s *Server) setupRoutes() {
	s.router = mux.NewRouter()

	// API routes
	api := s.router.PathPrefix("/api").Subrouter()
	api.HandleFunc("/torrent/magnet", s.handleAddMagnet).Methods("POST")
	api.HandleFunc("/torrent/file", s.handleAddTorrentFile).Methods("POST")
	api.HandleFunc("/torrent/files", s.handleListFiles).Methods("GET")
	api.HandleFunc("/torrent/select/{index}", s.handleSelectVideo).Methods("POST")
	api.HandleFunc("/stream/info", s.handleStreamInfo).Methods("GET")
	api.HandleFunc("/stream/play", s.handlePlay).Methods("POST")
	api.HandleFunc("/stream/pause", s.handlePause).Methods("POST")
	api.HandleFunc("/stream/seek", s.handleSeek).Methods("POST")
	api.HandleFunc("/stream/position", s.handleUpdatePosition).Methods("POST")
	api.HandleFunc("/stream/audio/{index}", s.handleSwitchAudio).Methods("POST")
	api.HandleFunc("/stream/subtitle/{index}", s.handleSwitchSubtitle).Methods("POST")

	// WebSocket route
	s.router.HandleFunc("/ws", s.handleWebSocket)

	// HLS routes
	s.router.PathPrefix("/hls/").Handler(http.StripPrefix("/hls/", http.HandlerFunc(s.handleHLS)))

	// Static files
	s.router.PathPrefix("/").Handler(http.FileServer(http.Dir("web")))
}

// Router returns the HTTP router
func (s *Server) Router() *mux.Router {
	return s.router
}

// handleAddMagnet handles adding a magnet link
func (s *Server) handleAddMagnet(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Magnet string `json:"magnet"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Minute)
	defer cancel()

	if err := s.streamMgr.LoadMagnet(ctx, req.Magnet); err != nil {
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	files := s.streamMgr.GetVideoFiles()
	jsonResponse(w, map[string]interface{}{
		"success": true,
		"files":   files,
	})
}

// handleAddTorrentFile handles uploading a torrent file
func (s *Server) handleAddTorrentFile(w http.ResponseWriter, r *http.Request) {
	// Parse multipart form (max 10MB)
	if err := r.ParseMultipartForm(10 << 20); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}

	file, _, err := r.FormFile("torrent")
	if err != nil {
		http.Error(w, "No torrent file provided", http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Save to temp file
	tmpFile, err := os.CreateTemp("", "torrent-*.torrent")
	if err != nil {
		http.Error(w, "Failed to create temp file", http.StatusInternalServerError)
		return
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := io.Copy(tmpFile, file); err != nil {
		http.Error(w, "Failed to save torrent file", http.StatusInternalServerError)
		return
	}
	tmpFile.Close()

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Minute)
	defer cancel()

	if err := s.streamMgr.LoadTorrentFile(ctx, tmpFile.Name()); err != nil {
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	files := s.streamMgr.GetVideoFiles()
	jsonResponse(w, map[string]interface{}{
		"success": true,
		"files":   files,
	})
}

// handleListFiles returns list of video files in torrent
func (s *Server) handleListFiles(w http.ResponseWriter, r *http.Request) {
	files := s.streamMgr.GetVideoFiles()
	jsonResponse(w, map[string]interface{}{
		"files": files,
	})
}

// handleSelectVideo selects a video file and starts transcoding
func (s *Server) handleSelectVideo(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	index, err := strconv.Atoi(vars["index"])
	if err != nil {
		http.Error(w, "Invalid index", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()

	if err := s.streamMgr.SelectVideo(ctx, index); err != nil {
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	info := s.streamMgr.GetInfo()

	// Update room with stream info
	infoData, _ := json.Marshal(info)
	s.room.UpdateStreamInfo(infoData)

	jsonResponse(w, info)
}

// handleStreamInfo returns current stream information
func (s *Server) handleStreamInfo(w http.ResponseWriter, r *http.Request) {
	info := s.streamMgr.GetInfo()
	jsonResponse(w, info)
}

// handlePlay handles play command
func (s *Server) handlePlay(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Position float64 `json:"position"`
	}
	json.NewDecoder(r.Body).Decode(&req)

	s.streamMgr.Play()
	s.room.Play(req.Position)

	jsonResponse(w, map[string]bool{"success": true})
}

// handlePause handles pause command
func (s *Server) handlePause(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Position float64 `json:"position"`
	}
	json.NewDecoder(r.Body).Decode(&req)

	s.streamMgr.Pause()
	s.room.Pause(req.Position)

	jsonResponse(w, map[string]bool{"success": true})
}

// handleSeek handles seek command
func (s *Server) handleSeek(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Position float64 `json:"position"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	if err := s.streamMgr.Seek(ctx, req.Position); err != nil {
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.room.Seek(req.Position)

	jsonResponse(w, map[string]bool{"success": true})
}

// handleUpdatePosition updates current playback position
func (s *Server) handleUpdatePosition(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Position float64 `json:"position"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	s.streamMgr.SetPosition(req.Position)
	s.room.UpdatePosition(req.Position)

	w.WriteHeader(http.StatusNoContent)
}

// handleSwitchAudio switches audio track
func (s *Server) handleSwitchAudio(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	index, err := strconv.Atoi(vars["index"])
	if err != nil {
		http.Error(w, "Invalid index", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	if err := s.streamMgr.SwitchAudio(ctx, index); err != nil {
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Notify viewers about audio change
	data, _ := json.Marshal(map[string]int{"index": index})
	s.room.UpdateStreamInfo(data)

	jsonResponse(w, map[string]bool{"success": true})
}

// handleSwitchSubtitle switches subtitle track
func (s *Server) handleSwitchSubtitle(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	index, err := strconv.Atoi(vars["index"])
	if err != nil {
		http.Error(w, "Invalid index", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	if err := s.streamMgr.SwitchSubtitle(ctx, index); err != nil {
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	jsonResponse(w, map[string]bool{"success": true})
}

// handleWebSocket handles WebSocket connections
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade error: %v", err)
		return
	}

	// Get query params
	name := r.URL.Query().Get("name")
	if name == "" {
		name = "Anonymous"
	}

	isHost := r.URL.Query().Get("host") == "true"

	var client *syncpkg.Client
	if isHost {
		if s.room.HasHost() {
			conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","data":{"message":"Room already has a host"}}`))
			conn.Close()
			return
		}
		client = s.room.JoinAsHost(conn, name)
	} else {
		if s.room.GetViewerCount() >= 4 {
			conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","data":{"message":"Room is full"}}`))
			conn.Close()
			return
		}
		client = s.room.JoinAsViewer(conn, name)
	}

	log.Printf("Client connected: %s (host: %v)", name, isHost)

	go client.WritePump()
	client.HandleMessages()

	log.Printf("Client disconnected: %s", name)
}

// handleHLS serves HLS segments and playlists
func (s *Server) handleHLS(w http.ResponseWriter, r *http.Request) {
	hlsDir := s.streamMgr.GetHLSDir()
	filePath := filepath.Join(hlsDir, filepath.Clean(r.URL.Path))

	// Security check
	if !filepath.HasPrefix(filePath, hlsDir) {
		http.NotFound(w, r)
		return
	}

	// Set appropriate content type
	ext := filepath.Ext(filePath)
	switch ext {
	case ".m3u8":
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		w.Header().Set("Cache-Control", "no-cache")
	case ".ts":
		w.Header().Set("Content-Type", "video/mp2t")
		w.Header().Set("Cache-Control", "max-age=3600")
	}

	// Allow CORS
	w.Header().Set("Access-Control-Allow-Origin", "*")

	http.ServeFile(w, r, filePath)
}

// jsonResponse sends a JSON response
func jsonResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

// jsonError sends a JSON error response
func jsonError(w http.ResponseWriter, message string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}
