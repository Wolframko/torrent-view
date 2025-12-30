// Torrent View - Player and Sync Logic

class TorrentView {
    constructor() {
        this.isHost = false;
        this.username = '';
        this.ws = null;
        this.hls = null;
        this.video = null;
        this.streamInfo = null;
        this.isSeeking = false;
        this.lastSyncTime = 0;
        this.syncThreshold = 2; // seconds

        this.init();
    }

    init() {
        // Join screen handlers
        document.getElementById('btn-host').addEventListener('click', () => this.joinAsHost());
        document.getElementById('btn-join').addEventListener('click', () => this.joinAsViewer());
        document.getElementById('username').addEventListener('keypress', (e) => {
            if (e.key === 'Enter') this.joinAsHost();
        });

        // Host controls
        document.getElementById('btn-add-magnet').addEventListener('click', () => this.addMagnet());
        document.getElementById('magnet-input').addEventListener('keypress', (e) => {
            if (e.key === 'Enter') this.addMagnet();
        });
        document.getElementById('btn-upload-torrent').addEventListener('click', () => {
            document.getElementById('torrent-file').click();
        });
        document.getElementById('torrent-file').addEventListener('change', (e) => this.uploadTorrent(e));

        // Chat handlers
        document.getElementById('btn-send-chat').addEventListener('click', () => this.sendChat('chat-input'));
        document.getElementById('chat-input').addEventListener('keypress', (e) => {
            if (e.key === 'Enter') this.sendChat('chat-input');
        });
        document.getElementById('btn-viewer-send-chat').addEventListener('click', () => this.sendChat('viewer-chat-input'));
        document.getElementById('viewer-chat-input').addEventListener('keypress', (e) => {
            if (e.key === 'Enter') this.sendChat('viewer-chat-input');
        });
    }

    joinAsHost() {
        this.username = document.getElementById('username').value.trim() || 'Host';
        this.isHost = true;
        this.showScreen('host-screen');
        this.connectWebSocket();
        this.initHostPlayer();
    }

    joinAsViewer() {
        this.username = document.getElementById('username').value.trim() || 'Viewer';
        this.isHost = false;
        this.showScreen('viewer-screen');
        this.connectWebSocket();
        this.initViewerPlayer();
    }

    showScreen(screenId) {
        document.querySelectorAll('.screen').forEach(s => s.classList.remove('active'));
        document.getElementById(screenId).classList.add('active');
    }

    // WebSocket Connection
    connectWebSocket() {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}/ws?name=${encodeURIComponent(this.username)}&host=${this.isHost}`;

        this.ws = new WebSocket(wsUrl);

        this.ws.onopen = () => {
            console.log('WebSocket connected');
            this.ws.send(JSON.stringify({ type: 'request_sync', timestamp: Date.now() }));
        };

        this.ws.onmessage = (event) => {
            const msg = JSON.parse(event.data);
            this.handleMessage(msg);
        };

        this.ws.onclose = () => {
            console.log('WebSocket disconnected');
            setTimeout(() => this.connectWebSocket(), 3000);
        };

        this.ws.onerror = (error) => {
            console.error('WebSocket error:', error);
        };
    }

    handleMessage(msg) {
        switch (msg.type) {
            case 'sync_state':
                if (!this.isHost) {
                    this.handleSyncState(JSON.parse(msg.data));
                }
                break;
            case 'play':
                if (!this.isHost) {
                    const data = JSON.parse(msg.data);
                    this.viewerPlay(data.position);
                }
                break;
            case 'pause':
                if (!this.isHost) {
                    const data = JSON.parse(msg.data);
                    this.viewerPause(data.position);
                }
                break;
            case 'seek':
                if (!this.isHost) {
                    const data = JSON.parse(msg.data);
                    this.viewerSeek(data.position);
                }
                break;
            case 'stream_info':
                this.updateStreamInfo(JSON.parse(msg.data));
                break;
            case 'viewer_list':
                this.updateViewerList(JSON.parse(msg.data));
                break;
            case 'chat':
                this.displayChatMessage(JSON.parse(msg.data));
                break;
            case 'error':
                const errData = JSON.parse(msg.data);
                alert(errData.message);
                break;
        }
    }

    // Host Player
    initHostPlayer() {
        this.video = document.getElementById('video-player');

        // Play/Pause button
        document.getElementById('btn-play-pause').addEventListener('click', () => {
            if (this.video.paused) {
                this.hostPlay();
            } else {
                this.hostPause();
            }
        });

        // Progress bar
        const progressBar = document.getElementById('progress-bar');
        progressBar.addEventListener('click', (e) => this.handleProgressClick(e));

        // Dragging support
        let isDragging = false;
        progressBar.addEventListener('mousedown', () => { isDragging = true; this.isSeeking = true; });
        document.addEventListener('mouseup', () => {
            if (isDragging) {
                isDragging = false;
                this.isSeeking = false;
            }
        });
        document.addEventListener('mousemove', (e) => {
            if (isDragging) this.handleProgressDrag(e, progressBar);
        });

        // Video events
        this.video.addEventListener('timeupdate', () => this.updateProgress());
        this.video.addEventListener('play', () => this.updatePlayPauseButton(true));
        this.video.addEventListener('pause', () => this.updatePlayPauseButton(false));
        this.video.addEventListener('waiting', () => this.showLoading('Buffering...'));
        this.video.addEventListener('canplay', () => this.hideLoading());

        // Audio/Subtitle selects
        document.getElementById('audio-select').addEventListener('change', (e) => {
            this.switchAudio(parseInt(e.target.value));
        });
        document.getElementById('subtitle-select').addEventListener('change', (e) => {
            this.switchSubtitle(parseInt(e.target.value));
        });

        // Fullscreen
        document.getElementById('btn-fullscreen').addEventListener('click', () => {
            this.toggleFullscreen(document.querySelector('#host-screen .video-area'));
        });
    }

    // Viewer Player
    initViewerPlayer() {
        this.video = document.getElementById('viewer-video-player');

        this.video.addEventListener('timeupdate', () => this.updateViewerProgress());
        this.video.addEventListener('waiting', () => {
            this.showViewerLoading('Buffering...');
            this.sendBufferingStatus(true);
        });
        this.video.addEventListener('canplay', () => {
            this.hideViewerLoading();
            this.sendBufferingStatus(false);
        });

        // Fullscreen
        document.getElementById('btn-viewer-fullscreen').addEventListener('click', () => {
            this.toggleFullscreen(document.querySelector('#viewer-screen .video-area'));
        });
    }

    // Torrent Management
    async addMagnet() {
        const magnet = document.getElementById('magnet-input').value.trim();
        if (!magnet) return;

        this.showLoading('Loading torrent...');

        try {
            const response = await fetch('/api/torrent/magnet', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ magnet })
            });

            const data = await response.json();
            if (data.error) throw new Error(data.error);

            this.showFileList(data.files);
            document.getElementById('magnet-input').value = '';
        } catch (error) {
            alert('Failed to add torrent: ' + error.message);
        } finally {
            this.hideLoading();
        }
    }

    async uploadTorrent(event) {
        const file = event.target.files[0];
        if (!file) return;

        this.showLoading('Uploading torrent...');

        try {
            const formData = new FormData();
            formData.append('torrent', file);

            const response = await fetch('/api/torrent/file', {
                method: 'POST',
                body: formData
            });

            const data = await response.json();
            if (data.error) throw new Error(data.error);

            this.showFileList(data.files);
        } catch (error) {
            alert('Failed to upload torrent: ' + error.message);
        } finally {
            this.hideLoading();
            event.target.value = '';
        }
    }

    showFileList(files) {
        const panel = document.getElementById('files-panel');
        const list = document.getElementById('file-list');

        panel.style.display = 'block';
        list.innerHTML = '';

        files.forEach((file, index) => {
            const item = document.createElement('div');
            item.className = 'file-item';
            item.innerHTML = `
                <div class="file-name" title="${file.Path}">${file.Path}</div>
                <div class="file-size">${this.formatBytes(file.Size)}</div>
            `;
            item.addEventListener('click', () => this.selectVideo(file.Index, item));
            list.appendChild(item);
        });
    }

    async selectVideo(index, element) {
        document.querySelectorAll('.file-item').forEach(el => el.classList.remove('selected'));
        element.classList.add('selected');

        this.showLoading('Preparing stream...');

        try {
            const response = await fetch(`/api/torrent/select/${index}`, {
                method: 'POST'
            });

            const data = await response.json();
            if (data.error) throw new Error(data.error);

            this.streamInfo = data;
            this.updateTrackSelects(data);
            this.loadHLS(data.playlist_url);
        } catch (error) {
            alert('Failed to start stream: ' + error.message);
            this.hideLoading();
        }
    }

    loadHLS(url) {
        if (this.hls) {
            this.hls.destroy();
        }

        if (Hls.isSupported()) {
            this.hls = new Hls({
                enableWorker: true,
                lowLatencyMode: true,
                backBufferLength: 30
            });

            this.hls.loadSource(url);
            this.hls.attachMedia(this.video);

            this.hls.on(Hls.Events.MANIFEST_PARSED, () => {
                this.hideLoading();
            });

            this.hls.on(Hls.Events.ERROR, (event, data) => {
                if (data.fatal) {
                    console.error('HLS error:', data);
                    if (data.type === Hls.ErrorTypes.NETWORK_ERROR) {
                        this.hls.startLoad();
                    } else if (data.type === Hls.ErrorTypes.MEDIA_ERROR) {
                        this.hls.recoverMediaError();
                    }
                }
            });
        } else if (this.video.canPlayType('application/vnd.apple.mpegurl')) {
            this.video.src = url;
            this.video.addEventListener('loadedmetadata', () => {
                this.hideLoading();
            });
        }
    }

    updateTrackSelects(info) {
        const audioSelect = document.getElementById('audio-select');
        const subSelect = document.getElementById('subtitle-select');

        audioSelect.innerHTML = '';
        info.audio_tracks?.forEach((track, i) => {
            const option = document.createElement('option');
            option.value = i;
            option.textContent = `${track.title} (${track.language})`;
            if (i === info.selected_audio) option.selected = true;
            audioSelect.appendChild(option);
        });

        subSelect.innerHTML = '<option value="-1">No Subtitles</option>';
        info.subtitle_tracks?.forEach((track, i) => {
            const option = document.createElement('option');
            option.value = i;
            option.textContent = `${track.title} (${track.language})`;
            if (i === info.selected_subtitle) option.selected = true;
            subSelect.appendChild(option);
        });
    }

    // Playback Control (Host)
    async hostPlay() {
        this.video.play();
        await fetch('/api/stream/play', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ position: this.video.currentTime })
        });
    }

    async hostPause() {
        this.video.pause();
        await fetch('/api/stream/pause', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ position: this.video.currentTime })
        });
    }

    async hostSeek(position) {
        this.video.currentTime = position;
        await fetch('/api/stream/seek', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ position })
        });
    }

    async switchAudio(index) {
        this.showLoading('Switching audio...');
        try {
            await fetch(`/api/stream/audio/${index}`, { method: 'POST' });
            // Reload HLS
            if (this.streamInfo?.playlist_url) {
                const currentTime = this.video.currentTime;
                this.loadHLS(this.streamInfo.playlist_url + '?t=' + Date.now());
                this.video.currentTime = currentTime;
            }
        } catch (error) {
            console.error('Failed to switch audio:', error);
        }
    }

    async switchSubtitle(index) {
        this.showLoading('Switching subtitles...');
        try {
            await fetch(`/api/stream/subtitle/${index}`, { method: 'POST' });
            if (this.streamInfo?.playlist_url) {
                const currentTime = this.video.currentTime;
                this.loadHLS(this.streamInfo.playlist_url + '?t=' + Date.now());
                this.video.currentTime = currentTime;
            }
        } catch (error) {
            console.error('Failed to switch subtitle:', error);
        }
    }

    // Playback Control (Viewer)
    viewerPlay(position) {
        if (Math.abs(this.video.currentTime - position) > this.syncThreshold) {
            this.video.currentTime = position;
        }
        this.video.play();
        this.updateViewerStatus(true);
    }

    viewerPause(position) {
        this.video.currentTime = position;
        this.video.pause();
        this.updateViewerStatus(false);
    }

    viewerSeek(position) {
        this.video.currentTime = position;
        this.showSyncIndicator();
    }

    handleSyncState(state) {
        if (!this.video || !this.video.src) return;

        const timeDiff = Math.abs(this.video.currentTime - state.position);

        if (timeDiff > this.syncThreshold) {
            this.video.currentTime = state.position;
            this.showSyncIndicator('Syncing...');
        }

        if (state.playing && this.video.paused) {
            this.video.play();
        } else if (!state.playing && !this.video.paused) {
            this.video.pause();
        }

        this.updateViewerStatus(state.playing);
    }

    // Progress Bar
    handleProgressClick(e) {
        const rect = e.currentTarget.getBoundingClientRect();
        const percent = (e.clientX - rect.left) / rect.width;
        const time = percent * this.video.duration;

        if (this.isHost) {
            this.hostSeek(time);
        }
    }

    handleProgressDrag(e, progressBar) {
        const rect = progressBar.getBoundingClientRect();
        let percent = (e.clientX - rect.left) / rect.width;
        percent = Math.max(0, Math.min(1, percent));

        document.getElementById('progress-played').style.width = `${percent * 100}%`;
        document.getElementById('progress-handle').style.left = `${percent * 100}%`;

        if (this.isHost && this.video.duration) {
            this.video.currentTime = percent * this.video.duration;
        }
    }

    updateProgress() {
        if (this.isSeeking) return;

        const duration = this.video.duration || 0;
        const current = this.video.currentTime || 0;
        const percent = duration ? (current / duration) * 100 : 0;

        document.getElementById('progress-played').style.width = `${percent}%`;
        document.getElementById('progress-handle').style.left = `${percent}%`;
        document.getElementById('time-display').textContent =
            `${this.formatTime(current)} / ${this.formatTime(duration)}`;

        // Report position periodically
        if (this.isHost && Date.now() - this.lastSyncTime > 1000) {
            this.lastSyncTime = Date.now();
            fetch('/api/stream/position', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ position: current })
            }).catch(() => {});
        }
    }

    updateViewerProgress() {
        const duration = this.video.duration || 0;
        const current = this.video.currentTime || 0;
        const percent = duration ? (current / duration) * 100 : 0;

        document.getElementById('viewer-progress-played').style.width = `${percent}%`;
        document.getElementById('viewer-time-display').textContent =
            `${this.formatTime(current)} / ${this.formatTime(duration)}`;
    }

    updatePlayPauseButton(playing) {
        document.querySelector('.icon-play').style.display = playing ? 'none' : 'inline';
        document.querySelector('.icon-pause').style.display = playing ? 'inline' : 'none';
    }

    updateViewerStatus(playing) {
        const status = document.getElementById('viewer-status');
        status.textContent = playing ? 'Playing' : 'Paused';
        status.className = 'viewer-status' + (playing ? ' playing' : '');
    }

    // Stream Info
    updateStreamInfo(info) {
        this.streamInfo = info;

        if (!this.isHost && info.playlist_url && !this.video.src) {
            this.loadHLS(info.playlist_url);
            this.hideViewerLoading();
        }

        document.getElementById('viewer-stream-info').textContent =
            info.video_file || 'No stream active';
    }

    // Viewer List
    updateViewerList(viewers) {
        const listId = this.isHost ? 'viewer-list' : 'viewer-viewer-list';
        const countId = this.isHost ? 'viewer-count' : 'viewer-viewer-count';

        const list = document.getElementById(listId);
        const count = document.getElementById(countId);

        count.textContent = `(${viewers.length})`;
        list.innerHTML = '';

        viewers.forEach(viewer => {
            const item = document.createElement('div');
            item.className = 'viewer-item';
            item.innerHTML = `
                <div class="viewer-avatar">${viewer.name.charAt(0).toUpperCase()}</div>
                <div class="viewer-name">${viewer.name}</div>
                ${viewer.is_host ? '<span class="viewer-badge">Host</span>' : ''}
                <div class="viewer-status-dot ${viewer.buffering ? 'buffering' : ''}"></div>
            `;
            list.appendChild(item);
        });
    }

    // Chat
    sendChat(inputId) {
        const input = document.getElementById(inputId);
        const message = input.value.trim();
        if (!message || !this.ws) return;

        this.ws.send(JSON.stringify({
            type: 'chat',
            timestamp: Date.now(),
            data: JSON.stringify({ message })
        }));

        input.value = '';
    }

    displayChatMessage(data) {
        const containerId = this.isHost ? 'chat-messages' : 'viewer-chat-messages';
        const container = document.getElementById(containerId);

        const msgEl = document.createElement('div');
        msgEl.className = 'chat-message';
        msgEl.innerHTML = `
            <div class="sender ${data.is_host ? 'host' : ''}">${data.sender_name}</div>
            <div class="text">${this.escapeHtml(data.message)}</div>
        `;

        container.appendChild(msgEl);
        container.scrollTop = container.scrollHeight;
    }

    sendBufferingStatus(buffering) {
        if (this.ws) {
            this.ws.send(JSON.stringify({
                type: 'viewer_buffering',
                timestamp: Date.now(),
                data: JSON.stringify({ buffering })
            }));
        }
    }

    // UI Helpers
    showLoading(text) {
        const overlay = document.getElementById('loading-overlay');
        document.getElementById('loading-text').textContent = text;
        overlay.classList.remove('hidden');
    }

    hideLoading() {
        document.getElementById('loading-overlay').classList.add('hidden');
    }

    showViewerLoading(text) {
        const overlay = document.getElementById('viewer-loading-overlay');
        document.getElementById('viewer-loading-text').textContent = text;
        overlay.classList.remove('hidden');
    }

    hideViewerLoading() {
        document.getElementById('viewer-loading-overlay').classList.add('hidden');
    }

    showSyncIndicator(text = 'Synced') {
        const indicator = document.getElementById('sync-indicator');
        indicator.textContent = text;
        indicator.classList.add('visible');
        indicator.classList.toggle('warning', text !== 'Synced');

        setTimeout(() => {
            indicator.classList.remove('visible');
        }, 2000);
    }

    toggleFullscreen(element) {
        if (document.fullscreenElement) {
            document.exitFullscreen();
        } else {
            element.requestFullscreen();
        }
    }

    // Utilities
    formatTime(seconds) {
        if (!seconds || !isFinite(seconds)) return '0:00';
        const h = Math.floor(seconds / 3600);
        const m = Math.floor((seconds % 3600) / 60);
        const s = Math.floor(seconds % 60);

        if (h > 0) {
            return `${h}:${m.toString().padStart(2, '0')}:${s.toString().padStart(2, '0')}`;
        }
        return `${m}:${s.toString().padStart(2, '0')}`;
    }

    formatBytes(bytes) {
        if (bytes === 0) return '0 B';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
}

// Initialize app
document.addEventListener('DOMContentLoaded', () => {
    window.app = new TorrentView();
});
