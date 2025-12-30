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
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/user/torrent-view/internal/stream"
	syncpkg "github.com/user/torrent-view/internal/sync"
)

// Config holds server configuration
type Config struct {
	MaxViewers    int
	AllowedOrigin string
}

// Server represents the HTTP API server
type Server struct {
	streamMgr *stream.Manager
	roomMgr   *syncpkg.RoomManager
	room      *syncpkg.Room
	upgrader  websocket.Upgrader
	router    *mux.Router
	config    Config
}

// NewServer creates a new API server
func NewServer(streamMgr *stream.Manager) *Server {
	return NewServerWithConfig(streamMgr, Config{
		MaxViewers:    4,
		AllowedOrigin: "", // Empty means allow all (for development)
	})
}

// NewServerWithConfig creates a new API server with custom config
func NewServerWithConfig(streamMgr *stream.Manager, config Config) *Server {
	s := &Server{
		streamMgr: streamMgr,
		roomMgr:   syncpkg.NewRoomManager(),
		config:    config,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				if config.AllowedOrigin == "" {
					return true // Development mode
				}
				origin := r.Header.Get("Origin")
				return origin == config.AllowedOrigin
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
		jsonError(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.Magnet == "" {
		jsonError(w, "Magnet link is required", http.StatusBadRequest)
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
		jsonError(w, "Failed to parse form", http.StatusBadRequest)
		return
	}

	file, _, err := r.FormFile("torrent")
	if err != nil {
		jsonError(w, "No torrent file provided", http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Save to temp file
	tmpFile, err := os.CreateTemp("", "torrent-*.torrent")
	if err != nil {
		log.Printf("Failed to create temp file: %v", err)
		jsonError(w, "Failed to create temp file", http.StatusInternalServerError)
		return
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := io.Copy(tmpFile, file); err != nil {
		log.Printf("Failed to save torrent file: %v", err)
		jsonError(w, "Failed to save torrent file", http.StatusInternalServerError)
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
		jsonError(w, "Invalid index", http.StatusBadRequest)
		return
	}

	if index < 0 {
		jsonError(w, "Index must be non-negative", http.StatusBadRequest)
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
	infoData, err := json.Marshal(info)
	if err != nil {
		log.Printf("Failed to marshal stream info: %v", err)
	} else {
		s.room.UpdateStreamInfo(infoData)
	}

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

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		// Position is optional, default to 0
		req.Position = 0
	}

	s.streamMgr.Play()
	s.room.Play(req.Position)

	jsonResponse(w, map[string]bool{"success": true})
}

// handlePause handles pause command
func (s *Server) handlePause(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Position float64 `json:"position"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		// Position is optional, default to 0
		req.Position = 0
	}

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
		jsonError(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.Position < 0 {
		jsonError(w, "Position must be non-negative", http.StatusBadRequest)
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
		jsonError(w, "Invalid request body", http.StatusBadRequest)
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
		jsonError(w, "Invalid index", http.StatusBadRequest)
		return
	}

	if index < 0 {
		jsonError(w, "Index must be non-negative", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	if err := s.streamMgr.SwitchAudio(ctx, index); err != nil {
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Notify viewers about audio change
	data, err := json.Marshal(map[string]int{"index": index})
	if err != nil {
		log.Printf("Failed to marshal audio change: %v", err)
	} else {
		s.room.UpdateStreamInfo(data)
	}

	jsonResponse(w, map[string]bool{"success": true})
}

// handleSwitchSubtitle switches subtitle track
func (s *Server) handleSwitchSubtitle(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	index, err := strconv.Atoi(vars["index"])
	if err != nil {
		jsonError(w, "Invalid index", http.StatusBadRequest)
		return
	}

	// -1 is valid (no subtitles)
	if index < -1 {
		jsonError(w, "Index must be >= -1", http.StatusBadRequest)
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

	// Sanitize name
	if len(name) > 20 {
		name = name[:20]
	}

	isHost := r.URL.Query().Get("host") == "true"

	var client *syncpkg.Client
	if isHost {
		if s.room.HasHost() {
			if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","data":{"message":"Room already has a host"}}`)); err != nil {
				log.Printf("Failed to send error message: %v", err)
			}
			conn.Close()
			return
		}
		client = s.room.JoinAsHost(conn, name)
	} else {
		if s.room.GetViewerCount() >= s.config.MaxViewers {
			if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","data":{"message":"Room is full"}}`)); err != nil {
				log.Printf("Failed to send error message: %v", err)
			}
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

	// Clean and validate the path
	requestedPath := filepath.Clean(r.URL.Path)

	// Prevent path traversal
	if strings.Contains(requestedPath, "..") {
		http.NotFound(w, r)
		return
	}

	filePath := filepath.Join(hlsDir, requestedPath)

	// Ensure the resolved path is within hlsDir
	absHlsDir, err := filepath.Abs(hlsDir)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	absFilePath, err := filepath.Abs(filePath)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	if !strings.HasPrefix(absFilePath, absHlsDir) {
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
	default:
		http.NotFound(w, r)
		return
	}

	// Allow CORS
	w.Header().Set("Access-Control-Allow-Origin", "*")

	http.ServeFile(w, r, filePath)
}

// jsonResponse sends a JSON response
func jsonResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("Failed to encode JSON response: %v", err)
	}
}

// jsonError sends a JSON error response
func jsonError(w http.ResponseWriter, message string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(map[string]string{"error": message}); err != nil {
		log.Printf("Failed to encode JSON error response: %v", err)
	}
}
