package stream

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/user/torrent-view/internal/torrent"
	"github.com/user/torrent-view/internal/transcoder"
)

// State represents the current streaming state
type State int

const (
	StateIdle State = iota
	StateLoading
	StateReady
	StatePlaying
	StatePaused
	StateError
)

func (s State) String() string {
	switch s {
	case StateIdle:
		return "idle"
	case StateLoading:
		return "loading"
	case StateReady:
		return "ready"
	case StatePlaying:
		return "playing"
	case StatePaused:
		return "paused"
	case StateError:
		return "error"
	default:
		return "unknown"
	}
}

// StreamInfo contains current stream information
type StreamInfo struct {
	State           State                      `json:"state"`
	TorrentName     string                     `json:"torrent_name"`
	VideoFile       string                     `json:"video_file"`
	Duration        float64                    `json:"duration"`
	CurrentPosition float64                    `json:"current_position"`
	BufferProgress  float64                    `json:"buffer_progress"`
	AudioTracks     []transcoder.AudioTrack    `json:"audio_tracks"`
	SubtitleTracks  []transcoder.SubtitleTrack `json:"subtitle_tracks"`
	SelectedAudio   int                        `json:"selected_audio"`
	SelectedSub     int                        `json:"selected_subtitle"`
	PlaylistURL     string                     `json:"playlist_url"`
	Error           string                     `json:"error,omitempty"`
}

// Manager manages the streaming pipeline
type Manager struct {
	torrentMgr *torrent.Manager
	transcoder *transcoder.Transcoder
	hlsConfig  transcoder.HLSConfig
	dataDir    string
	hlsDir     string

	mu              sync.RWMutex
	state           State
	currentPosition float64
	selectedAudio   int
	selectedSub     int
	lastError       string
	selectedVideo   int

	// Segment cleanup
	maxSegments   int
	cleanupMu     sync.Mutex
	cleanupTicker *time.Ticker
	cleanupStop   chan struct{}
	cleanupActive atomic.Bool
}

// NewManager creates a new stream manager
func NewManager(dataDir string, maxSegments int, bufferAheadMB int64) (*Manager, error) {
	hlsDir := filepath.Join(dataDir, "hls")

	torrentMgr, err := torrent.NewManager(filepath.Join(dataDir, "torrents"), bufferAheadMB)
	if err != nil {
		return nil, fmt.Errorf("failed to create torrent manager: %w", err)
	}

	m := &Manager{
		torrentMgr:    torrentMgr,
		transcoder:    transcoder.NewTranscoder(hlsDir),
		dataDir:       dataDir,
		hlsDir:        hlsDir,
		maxSegments:   maxSegments,
		selectedSub:   -1,
		selectedVideo: -1,
		hlsConfig: transcoder.HLSConfig{
			SegmentDuration: 4,
			OutputDir:       hlsDir,
			AudioTrackIndex: 0,
			SubtitleIndex:   -1,
		},
	}

	return m, nil
}

// LoadMagnet loads a torrent from magnet link
func (m *Manager) LoadMagnet(ctx context.Context, magnetURI string) error {
	m.mu.Lock()
	m.state = StateLoading
	m.lastError = ""
	m.mu.Unlock()

	if err := m.torrentMgr.AddMagnet(ctx, magnetURI); err != nil {
		m.mu.Lock()
		m.state = StateError
		m.lastError = err.Error()
		m.mu.Unlock()
		return err
	}

	return nil
}

// LoadTorrentFile loads a torrent from file
func (m *Manager) LoadTorrentFile(ctx context.Context, path string) error {
	m.mu.Lock()
	m.state = StateLoading
	m.lastError = ""
	m.mu.Unlock()

	if err := m.torrentMgr.AddTorrentFile(ctx, path); err != nil {
		m.mu.Lock()
		m.state = StateError
		m.lastError = err.Error()
		m.mu.Unlock()
		return err
	}

	return nil
}

// GetVideoFiles returns list of video files in torrent
func (m *Manager) GetVideoFiles() []torrent.VideoFile {
	return m.torrentMgr.ListVideoFiles()
}

// SelectVideo selects a video file and starts transcoding
func (m *Manager) SelectVideo(ctx context.Context, index int) error {
	m.mu.Lock()
	m.state = StateLoading
	m.mu.Unlock()

	// Select video in torrent manager
	if err := m.torrentMgr.SelectVideo(index); err != nil {
		m.mu.Lock()
		m.state = StateError
		m.lastError = err.Error()
		m.mu.Unlock()
		return err
	}

	// Wait for initial pieces
	if err := m.torrentMgr.WaitForPieces(ctx, 3); err != nil {
		m.mu.Lock()
		m.state = StateError
		m.lastError = err.Error()
		m.mu.Unlock()
		return err
	}

	// Get video path
	videoPath, err := m.torrentMgr.GetVideoPath()
	if err != nil {
		m.mu.Lock()
		m.state = StateError
		m.lastError = err.Error()
		m.mu.Unlock()
		return err
	}

	// Probe media info
	mediaInfo, err := m.transcoder.ProbeMedia(videoPath)
	if err != nil {
		m.mu.Lock()
		m.state = StateError
		m.lastError = err.Error()
		m.mu.Unlock()
		return err
	}

	// Set default audio track
	m.mu.Lock()
	m.selectedVideo = index
	m.selectedAudio = 0
	m.selectedSub = -1
	for i, track := range mediaInfo.AudioTracks {
		if track.Default {
			m.selectedAudio = i
			break
		}
	}
	m.hlsConfig.AudioTrackIndex = m.selectedAudio
	m.hlsConfig.SubtitleIndex = m.selectedSub
	m.mu.Unlock()

	// Start transcoding
	if err := m.transcoder.StartHLS(ctx, videoPath, m.hlsConfig); err != nil {
		m.mu.Lock()
		m.state = StateError
		m.lastError = err.Error()
		m.mu.Unlock()
		return err
	}

	// Start segment cleanup
	m.startCleanup()

	m.mu.Lock()
	m.state = StateReady
	m.currentPosition = 0
	m.mu.Unlock()

	return nil
}

// Play starts playback
func (m *Manager) Play() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state == StateReady || m.state == StatePaused {
		m.state = StatePlaying
	}
}

// Pause pauses playback
func (m *Manager) Pause() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state == StatePlaying {
		m.state = StatePaused
	}
}

// Seek seeks to a position in seconds
func (m *Manager) Seek(ctx context.Context, position float64) error {
	m.mu.Lock()
	prevState := m.state
	m.state = StateLoading
	selectedVideo := m.selectedVideo
	m.mu.Unlock()

	// Calculate byte offset for torrent
	mediaInfo := m.transcoder.GetMediaInfo()
	if mediaInfo != nil && mediaInfo.Duration > 0 && selectedVideo >= 0 {
		videos := m.torrentMgr.ListVideoFiles()
		if selectedVideo < len(videos) {
			byteOffset := int64(float64(videos[selectedVideo].Size) * (position / mediaInfo.Duration))
			m.torrentMgr.SetPosition(byteOffset)
		}
	}

	// Seek in transcoder
	m.mu.RLock()
	config := m.hlsConfig
	m.mu.RUnlock()

	if err := m.transcoder.SeekTo(ctx, position, config); err != nil {
		m.mu.Lock()
		m.state = StateError
		m.lastError = err.Error()
		m.mu.Unlock()
		return err
	}

	m.mu.Lock()
	m.currentPosition = position
	// Restore previous state properly
	switch prevState {
	case StatePlaying:
		m.state = StatePlaying
	case StatePaused:
		m.state = StatePaused
	case StateLoading:
		m.state = StateReady
	default:
		m.state = StateReady
	}
	m.mu.Unlock()

	return nil
}

// SetPosition updates current playback position (called by player)
func (m *Manager) SetPosition(position float64) {
	m.mu.Lock()
	m.currentPosition = position
	selectedVideo := m.selectedVideo
	m.mu.Unlock()

	// Update torrent position for prioritization
	mediaInfo := m.transcoder.GetMediaInfo()
	if mediaInfo == nil || mediaInfo.Duration <= 0 || selectedVideo < 0 {
		return
	}

	videos := m.torrentMgr.ListVideoFiles()
	if selectedVideo >= len(videos) {
		return
	}

	byteOffset := int64(float64(videos[selectedVideo].Size) * (position / mediaInfo.Duration))
	m.torrentMgr.SetPosition(byteOffset)
}

// SwitchAudio switches to a different audio track
func (m *Manager) SwitchAudio(ctx context.Context, trackIndex int) error {
	m.mu.Lock()
	m.selectedAudio = trackIndex
	m.hlsConfig.AudioTrackIndex = trackIndex
	config := m.hlsConfig
	m.mu.Unlock()

	return m.transcoder.SwitchAudio(ctx, trackIndex, config)
}

// SwitchSubtitle switches to a different subtitle track
func (m *Manager) SwitchSubtitle(ctx context.Context, trackIndex int) error {
	m.mu.Lock()
	m.selectedSub = trackIndex
	m.hlsConfig.SubtitleIndex = trackIndex
	config := m.hlsConfig
	m.mu.Unlock()

	return m.transcoder.SwitchSubtitle(ctx, trackIndex, config)
}

// GetInfo returns current stream information
func (m *Manager) GetInfo() StreamInfo {
	m.mu.RLock()
	info := StreamInfo{
		State:           m.state,
		TorrentName:     m.torrentMgr.GetTorrentName(),
		CurrentPosition: m.currentPosition,
		SelectedAudio:   m.selectedAudio,
		SelectedSub:     m.selectedSub,
		Error:           m.lastError,
	}
	selectedVideo := m.selectedVideo
	m.mu.RUnlock()

	// Get video file info
	videos := m.torrentMgr.ListVideoFiles()
	if selectedVideo >= 0 && selectedVideo < len(videos) {
		info.VideoFile = videos[selectedVideo].Path
	}

	// Get media info
	mediaInfo := m.transcoder.GetMediaInfo()
	if mediaInfo != nil {
		info.Duration = mediaInfo.Duration
		info.AudioTracks = mediaInfo.AudioTracks
		info.SubtitleTracks = mediaInfo.SubtitleTracks
	}

	// Get buffer progress
	completed, total := m.torrentMgr.GetDownloadProgress()
	if total > 0 {
		info.BufferProgress = float64(completed) / float64(total)
	}

	// Set playlist URL if ready
	if info.State == StateReady || info.State == StatePlaying || info.State == StatePaused {
		info.PlaylistURL = "/hls/playlist.m3u8"
	}

	return info
}

// GetHLSDir returns the HLS output directory
func (m *Manager) GetHLSDir() string {
	return m.hlsDir
}

// startCleanup starts the segment cleanup goroutine
func (m *Manager) startCleanup() {
	m.cleanupMu.Lock()
	defer m.cleanupMu.Unlock()

	if m.cleanupActive.Load() {
		return
	}

	m.cleanupStop = make(chan struct{})
	m.cleanupTicker = time.NewTicker(5 * time.Second)
	m.cleanupActive.Store(true)

	go func() {
		defer m.cleanupActive.Store(false)

		for {
			select {
			case <-m.cleanupTicker.C:
				m.cleanupOldSegments()
			case <-m.cleanupStop:
				return
			}
		}
	}()
}

// cleanupOldSegments removes old HLS segments to save disk space
func (m *Manager) cleanupOldSegments() {
	files, err := filepath.Glob(filepath.Join(m.hlsDir, "segment_*.ts"))
	if err != nil {
		log.Printf("Failed to glob HLS segments: %v", err)
		return
	}

	if len(files) <= m.maxSegments {
		return
	}

	// Sort by segment number
	sort.Slice(files, func(i, j int) bool {
		numI := extractSegmentNumber(files[i])
		numJ := extractSegmentNumber(files[j])
		return numI < numJ
	})

	// Remove oldest segments
	toRemove := len(files) - m.maxSegments
	for i := 0; i < toRemove; i++ {
		if err := os.Remove(files[i]); err != nil {
			log.Printf("Failed to remove old segment %s: %v", files[i], err)
		}
	}
}

// extractSegmentNumber extracts the segment number from filename
func extractSegmentNumber(filename string) int {
	base := filepath.Base(filename)
	base = strings.TrimPrefix(base, "segment_")
	base = strings.TrimSuffix(base, ".ts")
	num, err := strconv.Atoi(base)
	if err != nil {
		return 0
	}
	return num
}

// Stop stops all streaming
func (m *Manager) Stop() {
	m.mu.Lock()
	m.state = StateIdle
	m.mu.Unlock()

	m.stopCleanup()
	m.transcoder.Stop()
}

// stopCleanup safely stops the cleanup goroutine
func (m *Manager) stopCleanup() {
	m.cleanupMu.Lock()
	defer m.cleanupMu.Unlock()

	if !m.cleanupActive.Load() {
		return
	}

	if m.cleanupTicker != nil {
		m.cleanupTicker.Stop()
		m.cleanupTicker = nil
	}

	if m.cleanupStop != nil {
		close(m.cleanupStop)
		m.cleanupStop = nil
	}
}

// Close shuts down the manager
func (m *Manager) Close() error {
	m.Stop()
	m.transcoder.Cleanup()
	return m.torrentMgr.Close()
}
