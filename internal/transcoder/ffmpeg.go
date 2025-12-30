package transcoder

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// AudioTrack represents an audio track in the video
type AudioTrack struct {
	Index    int    `json:"index"`
	Language string `json:"language"`
	Title    string `json:"title"`
	Codec    string `json:"codec"`
	Channels int    `json:"channels"`
	Default  bool   `json:"default"`
}

// SubtitleTrack represents a subtitle track
type SubtitleTrack struct {
	Index    int    `json:"index"`
	Language string `json:"language"`
	Title    string `json:"title"`
	Codec    string `json:"codec"`
	Default  bool   `json:"default"`
}

// MediaInfo contains information about the video file
type MediaInfo struct {
	Duration      float64         `json:"duration"`
	VideoCodec    string          `json:"video_codec"`
	Width         int             `json:"width"`
	Height        int             `json:"height"`
	AudioTracks   []AudioTrack    `json:"audio_tracks"`
	SubtitleTracks []SubtitleTrack `json:"subtitle_tracks"`
}

// HLSConfig contains HLS transcoding settings
type HLSConfig struct {
	SegmentDuration int    // seconds per segment
	OutputDir       string // directory for HLS output
	AudioTrackIndex int    // which audio track to use
	SubtitleIndex   int    // which subtitle to burn in (-1 for none)
}

// Transcoder handles FFmpeg transcoding
type Transcoder struct {
	mu         sync.RWMutex
	cmd        *exec.Cmd
	cancel     context.CancelFunc
	outputDir  string
	inputFile  string
	mediaInfo  *MediaInfo
	isRunning  bool
	currentPos float64
}

// NewTranscoder creates a new transcoder instance
func NewTranscoder(outputDir string) *Transcoder {
	return &Transcoder{
		outputDir: outputDir,
	}
}

// ProbeMedia extracts media information using ffprobe
func (t *Transcoder) ProbeMedia(inputPath string) (*MediaInfo, error) {
	cmd := exec.Command("ffprobe",
		"-v", "quiet",
		"-print_format", "json",
		"-show_format",
		"-show_streams",
		inputPath,
	)

	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("ffprobe failed: %w", err)
	}

	var probe struct {
		Streams []struct {
			Index        int    `json:"index"`
			CodecType    string `json:"codec_type"`
			CodecName    string `json:"codec_name"`
			Width        int    `json:"width,omitempty"`
			Height       int    `json:"height,omitempty"`
			Channels     int    `json:"channels,omitempty"`
			Disposition  struct {
				Default int `json:"default"`
			} `json:"disposition"`
			Tags struct {
				Language string `json:"language"`
				Title    string `json:"title"`
			} `json:"tags"`
		} `json:"streams"`
		Format struct {
			Duration string `json:"duration"`
		} `json:"format"`
	}

	if err := json.Unmarshal(output, &probe); err != nil {
		return nil, fmt.Errorf("failed to parse ffprobe output: %w", err)
	}

	info := &MediaInfo{}

	// Parse duration
	if probe.Format.Duration != "" {
		info.Duration, _ = strconv.ParseFloat(probe.Format.Duration, 64)
	}

	audioIdx := 0
	subIdx := 0

	for _, stream := range probe.Streams {
		switch stream.CodecType {
		case "video":
			info.VideoCodec = stream.CodecName
			info.Width = stream.Width
			info.Height = stream.Height

		case "audio":
			lang := stream.Tags.Language
			if lang == "" {
				lang = "und"
			}
			title := stream.Tags.Title
			if title == "" {
				title = fmt.Sprintf("Audio %d", audioIdx+1)
			}

			info.AudioTracks = append(info.AudioTracks, AudioTrack{
				Index:    audioIdx,
				Language: lang,
				Title:    title,
				Codec:    stream.CodecName,
				Channels: stream.Channels,
				Default:  stream.Disposition.Default == 1,
			})
			audioIdx++

		case "subtitle":
			lang := stream.Tags.Language
			if lang == "" {
				lang = "und"
			}
			title := stream.Tags.Title
			if title == "" {
				title = fmt.Sprintf("Subtitle %d", subIdx+1)
			}

			info.SubtitleTracks = append(info.SubtitleTracks, SubtitleTrack{
				Index:    subIdx,
				Language: lang,
				Title:    title,
				Codec:    stream.CodecName,
				Default:  stream.Disposition.Default == 1,
			})
			subIdx++
		}
	}

	t.mu.Lock()
	t.mediaInfo = info
	t.inputFile = inputPath
	t.mu.Unlock()

	return info, nil
}

// StartHLS starts HLS transcoding
func (t *Transcoder) StartHLS(ctx context.Context, inputPath string, config HLSConfig) error {
	t.mu.Lock()
	if t.isRunning {
		t.mu.Unlock()
		return fmt.Errorf("transcoder already running")
	}
	t.mu.Unlock()

	// Create output directory
	if err := os.MkdirAll(config.OutputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output dir: %w", err)
	}

	// Clean up old segments
	files, _ := filepath.Glob(filepath.Join(config.OutputDir, "*.ts"))
	for _, f := range files {
		os.Remove(f)
	}
	os.Remove(filepath.Join(config.OutputDir, "playlist.m3u8"))

	ctx, cancel := context.WithCancel(ctx)
	t.cancel = cancel

	// Build FFmpeg command
	args := t.buildFFmpegArgs(inputPath, config)

	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	cmd.Dir = config.OutputDir

	// Capture stderr for progress
	stderr, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		return fmt.Errorf("failed to get stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		cancel()
		return fmt.Errorf("failed to start ffmpeg: %w", err)
	}

	t.mu.Lock()
	t.cmd = cmd
	t.isRunning = true
	t.inputFile = inputPath
	t.outputDir = config.OutputDir
	t.mu.Unlock()

	// Monitor progress in background
	go t.monitorProgress(stderr)

	// Wait for completion in background
	go func() {
		cmd.Wait()
		t.mu.Lock()
		t.isRunning = false
		t.mu.Unlock()
	}()

	// Wait for initial segments
	return t.waitForSegments(ctx, config.OutputDir, 2)
}

// buildFFmpegArgs builds FFmpeg command arguments
func (t *Transcoder) buildFFmpegArgs(inputPath string, config HLSConfig) []string {
	args := []string{
		"-y",
		"-i", inputPath,
		"-progress", "pipe:2",
	}

	// Video encoding - use hardware acceleration if available, fallback to software
	args = append(args,
		"-c:v", "libx264",
		"-preset", "veryfast",
		"-tune", "zerolatency",
		"-crf", "23",
		"-maxrate", "4M",
		"-bufsize", "8M",
		"-pix_fmt", "yuv420p",
	)

	// Audio - select specific track
	args = append(args,
		"-map", "0:v:0",
		"-map", fmt.Sprintf("0:a:%d", config.AudioTrackIndex),
		"-c:a", "aac",
		"-b:a", "192k",
		"-ac", "2",
	)

	// Subtitles - burn in if selected
	if config.SubtitleIndex >= 0 {
		// Try to use subtitle filter
		args = append(args,
			"-vf", fmt.Sprintf("subtitles='%s':si=%d", escapeFFmpegPath(inputPath), config.SubtitleIndex),
		)
	}

	// HLS settings
	segDuration := config.SegmentDuration
	if segDuration <= 0 {
		segDuration = 4
	}

	args = append(args,
		"-f", "hls",
		"-hls_time", strconv.Itoa(segDuration),
		"-hls_list_size", "0", // Keep all segments in playlist
		"-hls_flags", "delete_segments+append_list+independent_segments",
		"-hls_segment_type", "mpegts",
		"-hls_segment_filename", "segment_%05d.ts",
		"-hls_allow_cache", "1",
		"-start_number", "0",
		"playlist.m3u8",
	)

	return args
}

// escapeFFmpegPath escapes a path for use in FFmpeg filters
func escapeFFmpegPath(path string) string {
	// Escape special characters for FFmpeg filter
	path = strings.ReplaceAll(path, "\\", "\\\\")
	path = strings.ReplaceAll(path, "'", "\\'")
	path = strings.ReplaceAll(path, ":", "\\:")
	return path
}

// monitorProgress monitors FFmpeg progress output
func (t *Transcoder) monitorProgress(stderr io.ReadCloser) {
	scanner := bufio.NewScanner(stderr)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "out_time_ms=") {
			if ms, err := strconv.ParseInt(strings.TrimPrefix(line, "out_time_ms="), 10, 64); err == nil {
				t.mu.Lock()
				t.currentPos = float64(ms) / 1000000.0
				t.mu.Unlock()
			}
		}
	}
}

// waitForSegments waits for initial HLS segments to be ready
func (t *Transcoder) waitForSegments(ctx context.Context, outputDir string, count int) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	timeout := time.After(30 * time.Second)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout:
			return fmt.Errorf("timeout waiting for HLS segments")
		case <-ticker.C:
			files, _ := filepath.Glob(filepath.Join(outputDir, "*.ts"))
			if len(files) >= count {
				// Also check if playlist exists
				if _, err := os.Stat(filepath.Join(outputDir, "playlist.m3u8")); err == nil {
					return nil
				}
			}
		}
	}
}

// SeekTo seeks to a specific position and restarts transcoding
func (t *Transcoder) SeekTo(ctx context.Context, position float64, config HLSConfig) error {
	t.Stop()

	// Clean up old segments
	if err := os.MkdirAll(config.OutputDir, 0755); err != nil {
		return err
	}

	files, _ := filepath.Glob(filepath.Join(config.OutputDir, "*.ts"))
	for _, f := range files {
		os.Remove(f)
	}
	os.Remove(filepath.Join(config.OutputDir, "playlist.m3u8"))

	t.mu.RLock()
	inputPath := t.inputFile
	t.mu.RUnlock()

	if inputPath == "" {
		return fmt.Errorf("no input file set")
	}

	ctx, cancel := context.WithCancel(ctx)
	t.cancel = cancel

	// Build args with seek
	args := []string{
		"-y",
		"-ss", fmt.Sprintf("%.3f", position),
		"-i", inputPath,
		"-progress", "pipe:2",
		"-c:v", "libx264",
		"-preset", "veryfast",
		"-tune", "zerolatency",
		"-crf", "23",
		"-maxrate", "4M",
		"-bufsize", "8M",
		"-pix_fmt", "yuv420p",
		"-map", "0:v:0",
		"-map", fmt.Sprintf("0:a:%d", config.AudioTrackIndex),
		"-c:a", "aac",
		"-b:a", "192k",
		"-ac", "2",
	}

	if config.SubtitleIndex >= 0 {
		args = append(args,
			"-vf", fmt.Sprintf("subtitles='%s':si=%d", escapeFFmpegPath(inputPath), config.SubtitleIndex),
		)
	}

	segDuration := config.SegmentDuration
	if segDuration <= 0 {
		segDuration = 4
	}

	args = append(args,
		"-f", "hls",
		"-hls_time", strconv.Itoa(segDuration),
		"-hls_list_size", "0",
		"-hls_flags", "delete_segments+append_list+independent_segments",
		"-hls_segment_type", "mpegts",
		"-hls_segment_filename", "segment_%05d.ts",
		"-hls_allow_cache", "1",
		"-start_number", "0",
		"playlist.m3u8",
	)

	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	cmd.Dir = config.OutputDir

	stderr, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		return err
	}

	if err := cmd.Start(); err != nil {
		cancel()
		return err
	}

	t.mu.Lock()
	t.cmd = cmd
	t.isRunning = true
	t.currentPos = position
	t.mu.Unlock()

	go t.monitorProgress(stderr)

	go func() {
		cmd.Wait()
		t.mu.Lock()
		t.isRunning = false
		t.mu.Unlock()
	}()

	return t.waitForSegments(ctx, config.OutputDir, 1)
}

// SwitchAudio switches to a different audio track
func (t *Transcoder) SwitchAudio(ctx context.Context, audioIndex int, config HLSConfig) error {
	t.mu.RLock()
	currentPos := t.currentPos
	t.mu.RUnlock()

	config.AudioTrackIndex = audioIndex
	return t.SeekTo(ctx, currentPos, config)
}

// SwitchSubtitle switches to a different subtitle track
func (t *Transcoder) SwitchSubtitle(ctx context.Context, subtitleIndex int, config HLSConfig) error {
	t.mu.RLock()
	currentPos := t.currentPos
	t.mu.RUnlock()

	config.SubtitleIndex = subtitleIndex
	return t.SeekTo(ctx, currentPos, config)
}

// Stop stops the current transcoding process
func (t *Transcoder) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.cancel != nil {
		t.cancel()
		t.cancel = nil
	}

	if t.cmd != nil && t.cmd.Process != nil {
		t.cmd.Process.Kill()
		t.cmd.Wait()
		t.cmd = nil
	}

	t.isRunning = false
}

// IsRunning returns whether transcoding is in progress
func (t *Transcoder) IsRunning() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.isRunning
}

// GetProgress returns current transcoding position
func (t *Transcoder) GetProgress() float64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.currentPos
}

// GetMediaInfo returns cached media info
func (t *Transcoder) GetMediaInfo() *MediaInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.mediaInfo
}

// GetPlaylistPath returns the path to the HLS playlist
func (t *Transcoder) GetPlaylistPath() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return filepath.Join(t.outputDir, "playlist.m3u8")
}

// Cleanup removes all transcoded files
func (t *Transcoder) Cleanup() {
	t.Stop()

	t.mu.RLock()
	outputDir := t.outputDir
	t.mu.RUnlock()

	if outputDir != "" {
		os.RemoveAll(outputDir)
	}
}

// ExtractSubtitles extracts subtitles to WebVTT format
func (t *Transcoder) ExtractSubtitles(inputPath string, subIndex int, outputPath string) error {
	cmd := exec.Command("ffmpeg",
		"-y",
		"-i", inputPath,
		"-map", fmt.Sprintf("0:s:%d", subIndex),
		"-c:s", "webvtt",
		outputPath,
	)

	return cmd.Run()
}
