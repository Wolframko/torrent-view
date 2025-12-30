package torrent

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
)

// VideoFile represents a video file in the torrent
type VideoFile struct {
	Path   string
	Size   int64
	Index  int
	Offset int64
}

// Manager handles torrent downloading with chunk prioritization
type Manager struct {
	client     *torrent.Client
	activeTorr *torrent.Torrent
	videoFile  *torrent.File
	dataDir    string

	mu              sync.RWMutex
	currentPosition int64 // current playback position in bytes
	bufferAhead     int64 // how many bytes to buffer ahead
	pieceLength     int64

	// Cleanup tracking
	cleanedPieces map[int]bool
}

// NewManager creates a new torrent manager
func NewManager(dataDir string, bufferAheadMB int64) (*Manager, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data dir: %w", err)
	}

	cfg := torrent.NewDefaultClientConfig()
	cfg.DataDir = dataDir
	cfg.DefaultStorage = storage.NewFileByInfoHash(dataDir)
	cfg.Seed = false // We don't need to seed
	cfg.NoUpload = true
	cfg.DisableIPv6 = true

	client, err := torrent.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create torrent client: %w", err)
	}

	return &Manager{
		client:        client,
		dataDir:       dataDir,
		bufferAhead:   bufferAheadMB * 1024 * 1024,
		cleanedPieces: make(map[int]bool),
	}, nil
}

// AddMagnet adds a magnet link and waits for metadata
func (m *Manager) AddMagnet(ctx context.Context, magnetURI string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Close previous torrent if exists
	if m.activeTorr != nil {
		m.activeTorr.Drop()
		m.activeTorr = nil
		m.videoFile = nil
		m.cleanedPieces = make(map[int]bool)
	}

	t, err := m.client.AddMagnet(magnetURI)
	if err != nil {
		return fmt.Errorf("failed to add magnet: %w", err)
	}

	// Wait for metadata with timeout
	select {
	case <-t.GotInfo():
	case <-ctx.Done():
		t.Drop()
		return ctx.Err()
	case <-time.After(2 * time.Minute):
		t.Drop()
		return fmt.Errorf("timeout waiting for torrent metadata")
	}

	m.activeTorr = t
	m.pieceLength = t.Info().PieceLength

	return nil
}

// AddTorrentFile adds a torrent from file
func (m *Manager) AddTorrentFile(ctx context.Context, torrentPath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.activeTorr != nil {
		m.activeTorr.Drop()
		m.activeTorr = nil
		m.videoFile = nil
		m.cleanedPieces = make(map[int]bool)
	}

	mi, err := metainfo.LoadFromFile(torrentPath)
	if err != nil {
		return fmt.Errorf("failed to load torrent file: %w", err)
	}

	t, err := m.client.AddTorrent(mi)
	if err != nil {
		return fmt.Errorf("failed to add torrent: %w", err)
	}

	select {
	case <-t.GotInfo():
	case <-ctx.Done():
		t.Drop()
		return ctx.Err()
	}

	m.activeTorr = t
	m.pieceLength = t.Info().PieceLength

	return nil
}

// ListVideoFiles returns all video files in the torrent
func (m *Manager) ListVideoFiles() []VideoFile {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.activeTorr == nil {
		return nil
	}

	var videos []VideoFile
	videoExts := []string{".mkv", ".mp4", ".avi", ".webm", ".mov", ".wmv", ".flv", ".m4v"}

	for i, f := range m.activeTorr.Files() {
		ext := strings.ToLower(filepath.Ext(f.Path()))
		for _, ve := range videoExts {
			if ext == ve {
				videos = append(videos, VideoFile{
					Path:   f.Path(),
					Size:   f.Length(),
					Index:  i,
					Offset: f.Offset(),
				})
				break
			}
		}
	}

	// Sort by size descending (main video usually largest)
	sort.Slice(videos, func(i, j int) bool {
		return videos[i].Size > videos[j].Size
	})

	return videos
}

// SelectVideo selects a video file for streaming
func (m *Manager) SelectVideo(index int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.activeTorr == nil {
		return fmt.Errorf("no active torrent")
	}

	files := m.activeTorr.Files()
	if index < 0 || index >= len(files) {
		return fmt.Errorf("invalid file index")
	}

	// Cancel all files first
	for _, f := range files {
		f.SetPriority(torrent.PiecePriorityNone)
	}

	m.videoFile = files[index]
	m.currentPosition = 0

	// Prioritize initial pieces
	m.prioritizePiecesLocked()

	return nil
}

// GetVideoReader returns a reader for the video file at the given offset
func (m *Manager) GetVideoReader(offset int64) (io.ReadSeeker, int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.videoFile == nil {
		return nil, 0, fmt.Errorf("no video selected")
	}

	reader := m.videoFile.NewReader()
	reader.SetReadahead(m.bufferAhead)
	reader.SetResponsive()

	if offset > 0 {
		if _, err := reader.Seek(offset, io.SeekStart); err != nil {
			return nil, 0, err
		}
	}

	return reader, m.videoFile.Length(), nil
}

// GetVideoPath returns the path to the video file on disk
func (m *Manager) GetVideoPath() (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.videoFile == nil {
		return "", fmt.Errorf("no video selected")
	}

	return filepath.Join(m.dataDir, m.activeTorr.InfoHash().HexString(), m.videoFile.Path()), nil
}

// SetPosition updates current playback position and adjusts priorities
func (m *Manager) SetPosition(byteOffset int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.currentPosition = byteOffset
	m.prioritizePiecesLocked()
	m.cleanupOldPiecesLocked()
}

// prioritizePiecesLocked sets piece priorities based on current position
func (m *Manager) prioritizePiecesLocked() {
	if m.videoFile == nil || m.activeTorr == nil {
		return
	}

	fileOffset := m.videoFile.Offset()
	fileEnd := fileOffset + m.videoFile.Length()

	currentPiece := int((fileOffset + m.currentPosition) / m.pieceLength)
	bufferEndByte := fileOffset + m.currentPosition + m.bufferAhead
	if bufferEndByte > fileEnd {
		bufferEndByte = fileEnd
	}
	bufferEndPiece := int(bufferEndByte / m.pieceLength)

	// Set priorities
	for i := currentPiece; i <= bufferEndPiece; i++ {
		priority := torrent.PiecePriorityNormal
		if i == currentPiece {
			priority = torrent.PiecePriorityNow
		} else if i <= currentPiece+3 {
			priority = torrent.PiecePriorityHigh
		}
		m.activeTorr.Piece(i).SetPriority(priority)
	}
}

// cleanupOldPiecesLocked removes pieces that are behind current position
func (m *Manager) cleanupOldPiecesLocked() {
	if m.videoFile == nil || m.activeTorr == nil {
		return
	}

	fileOffset := m.videoFile.Offset()
	currentPiece := int((fileOffset + m.currentPosition) / m.pieceLength)

	// Keep some pieces behind for rewind capability
	keepBehind := 5

	for i := 0; i < currentPiece-keepBehind; i++ {
		if !m.cleanedPieces[i] {
			// Mark as not needed - this will allow the piece to be cleaned
			m.activeTorr.Piece(i).SetPriority(torrent.PiecePriorityNone)
			m.cleanedPieces[i] = true
		}
	}
}

// GetDownloadProgress returns download progress for buffered area
func (m *Manager) GetDownloadProgress() (completed, total int64) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.videoFile == nil {
		return 0, 0
	}

	fileOffset := m.videoFile.Offset()
	fileEnd := fileOffset + m.videoFile.Length()

	currentPiece := int((fileOffset + m.currentPosition) / m.pieceLength)
	bufferEndByte := fileOffset + m.currentPosition + m.bufferAhead
	if bufferEndByte > fileEnd {
		bufferEndByte = fileEnd
	}
	bufferEndPiece := int(bufferEndByte / m.pieceLength)

	for i := currentPiece; i <= bufferEndPiece; i++ {
		total++
		if m.activeTorr.Piece(i).State().Complete {
			completed++
		}
	}

	return completed, total
}

// WaitForPieces waits until enough pieces are downloaded to start playing
func (m *Manager) WaitForPieces(ctx context.Context, minPieces int) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			completed, _ := m.GetDownloadProgress()
			if completed >= int64(minPieces) {
				return nil
			}
		}
	}
}

// Close shuts down the manager
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.activeTorr != nil {
		m.activeTorr.Drop()
	}

	errs := m.client.Close()
	if len(errs) > 0 {
		return errs[0]
	}

	// Cleanup data directory
	os.RemoveAll(m.dataDir)

	return nil
}

// GetTorrentName returns the name of the active torrent
func (m *Manager) GetTorrentName() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.activeTorr == nil {
		return ""
	}

	return m.activeTorr.Name()
}

// IsReady returns true if torrent is ready for streaming
func (m *Manager) IsReady() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.activeTorr != nil && m.videoFile != nil
}
