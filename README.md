# Torrent View

Watch torrents together with friends in real-time.

## Features

- **Real-time sync**: Host controls playback, viewers stay synchronized
- **Streaming**: Video is transcoded on-the-fly, no full download required
- **Multiple audio tracks**: Switch between different audio tracks (dubs)
- **Subtitles**: Burn-in subtitles support
- **Minimal storage**: Only keeps necessary chunks, cleans up as you watch
- **Chat**: Built-in chat for viewers

## Architecture

```
┌─────────────┐     ┌─────────────────────────────────────┐     ┌──────────────┐
│    Host     │────▶│              Server                 │◀────│   Viewers    │
│  (browser)  │     │                                     │     │  (browsers)  │
└─────────────┘     │  ┌─────────┐  ┌────────┐  ┌──────┐  │     └──────────────┘
                    │  │ Torrent │─▶│ FFmpeg │─▶│ HLS  │  │
                    │  │ Engine  │  │(stream)│  │chunks│  │
                    │  └─────────┘  └────────┘  └──────┘  │
                    │       ▲                       │     │
                    │       │ prioritize            ▼     │
                    │  ┌─────────┐           ┌─────────┐  │
                    │  │  Sync   │◀─────────▶│  HTTP   │  │
                    │  │  (WS)   │           │ Stream  │  │
                    │  └─────────┘           └─────────┘  │
                    └─────────────────────────────────────┘
```

## Requirements

- Go 1.21+
- FFmpeg (with libx264 and aac support)

### Install FFmpeg

**Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install ffmpeg
```

**macOS:**
```bash
brew install ffmpeg
```

**Windows:**
Download from https://ffmpeg.org/download.html

## Installation

```bash
# Clone the repository
git clone https://github.com/user/torrent-view.git
cd torrent-view

# Download dependencies
go mod tidy

# Build
go build -o torrent-view ./cmd/server
```

## Usage

```bash
# Start server with default settings
./torrent-view

# Custom settings
./torrent-view \
  -addr :8080 \
  -data ./data \
  -max-segments 30 \
  -buffer 50
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-addr` | `:8080` | HTTP server address |
| `-data` | `./data` | Directory for torrent and HLS files |
| `-max-segments` | `30` | Max HLS segments to keep (saves disk space) |
| `-buffer` | `50` | Buffer ahead size in MB for torrent downloading |

## How to Use

1. **Start the server**
   ```bash
   ./torrent-view
   ```

2. **Open browser**: Go to `http://localhost:8080`

3. **Host creates room**:
   - Enter your name
   - Click "Create Room (Host)"
   - Paste magnet link or upload .torrent file
   - Select video file from the list

4. **Viewers join**:
   - Open `http://YOUR_SERVER_IP:8080`
   - Enter name
   - Click "Join as Viewer"
   - Wait for host to start the stream

5. **Host controls playback**:
   - Play/Pause
   - Seek
   - Switch audio tracks
   - Switch subtitles

Viewers are automatically synced with the host.

## API Endpoints

### Torrent Management
- `POST /api/torrent/magnet` - Add magnet link
- `POST /api/torrent/file` - Upload .torrent file
- `GET /api/torrent/files` - List video files

### Stream Control
- `POST /api/torrent/select/{index}` - Select video and start transcoding
- `GET /api/stream/info` - Get current stream info
- `POST /api/stream/play` - Play
- `POST /api/stream/pause` - Pause
- `POST /api/stream/seek` - Seek to position
- `POST /api/stream/audio/{index}` - Switch audio track
- `POST /api/stream/subtitle/{index}` - Switch subtitle track

### WebSocket
- `GET /ws?name=NAME&host=true|false` - Connect for real-time sync

### HLS
- `GET /hls/playlist.m3u8` - HLS playlist
- `GET /hls/segment_*.ts` - HLS segments

## How It Works

1. **Torrent Engine**: Uses anacrolix/torrent for downloading. Only downloads pieces needed for current playback position + buffer ahead.

2. **Transcoding**: FFmpeg transcodes video to HLS on-the-fly. Supports any input format, outputs H.264 + AAC in MPEG-TS segments.

3. **Chunk Management**: Old segments are automatically deleted to save disk space. Only keeps `max-segments` HLS segments.

4. **Sync Protocol**: WebSocket-based. Host sends play/pause/seek commands, server broadcasts to viewers. Periodic sync state updates ensure viewers stay in sync.

## Project Structure

```
torrent-view/
├── cmd/server/          # Main entry point
├── internal/
│   ├── api/            # HTTP handlers
│   ├── stream/         # Stream manager
│   ├── sync/           # WebSocket sync
│   ├── torrent/        # Torrent management
│   └── transcoder/     # FFmpeg transcoding
├── web/                # Frontend files
├── go.mod
└── README.md
```

## License

MIT
