const messagesDiv = document.getElementById('messages');
const messageInput = document.getElementById('message-input');
const sendButton = document.getElementById('send-button');
const statusDiv = document.getElementById('status');
const uploadButton = document.getElementById('upload-button');
const fileInput = document.getElementById('file-input');
const stagedFilesDisplay = document.getElementById('staged-files-display');

const API_SERVER_URL = 'http://localhost:3000'; // Local server for testing
const PEERJS_CONFIG = {
    // Use the default PeerJS cloud server for signaling negotiation
};

// File Transfer Constants
const MAX_FILES = 100;
const MAX_FILE_SIZE_MB = 500;
const MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024;
const CHUNK_SIZE = 64 * 1024; // 64 KB chunks

let peer = null;
let myPeerId = null;
const connections = {}; // Store connections { peerId: DataConnection }
let socket = null;

// Track ongoing file transfers
const outgoingFiles = {}; // { fileId: { meta, file, progressElement, statusElement, messageElement, previewContainerElement, previewObjectURL?, peers, sentChunks } }
const incomingFiles = {}; // { fileId: { meta, chunks: [], progressElement, statusElement, downloadLink, messageElement, previewContainerElement, senderId, receivedChunks } }

// Staging area for files before sending
let stagedFiles = []; // Array to hold File objects
const stagedObjectURLs = new Map(); // Map to store Object URLs for staged file previews { fileObject: url }

// Configure DOMPurify
DOMPurify.setConfig({ ADD_ATTR: ['target'] });

// --- Helper Functions ---

// NEW: Format file size adaptively
function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    // Show 1 decimal place for KB, MB, GB, etc. No decimals for Bytes.
    const num = parseFloat((bytes / Math.pow(k, i)).toFixed(i === 0 ? 0 : 1));
    return `${num} ${sizes[i]}`;
}

function shortId(id) {
    if (!id) return 'Anonymous';
    const idStr = String(id);
    return idStr.length > 8 ? `${idStr.substring(0, 4)}...${idStr.substring(idStr.length - 4)}` : idStr;
}

function scrollToBottom() {
    setTimeout(() => {
        messagesDiv.scrollTop = messagesDiv.scrollHeight;
    }, 50);
}

// --- Initialization --- (No changes from previous version)
function initializePeer() {
    peer = new Peer(undefined, PEERJS_CONFIG);

    peer.on('open', (id) => {
        myPeerId = id;
        console.log('My PeerJS ID is:', myPeerId);
        updateStatus(`Connected as ${shortId(myPeerId)}. Waiting for signaling server...`);
        initializeSignaling();
    });

    peer.on('connection', (conn) => {
        console.log(`Incoming connection from ${conn.peer}`);
        setupConnection(conn);
    });

    peer.on('disconnected', () => {
        updateStatus('PeerJS connection lost. Attempting to reconnect...');
        console.error('PeerJS disconnected. Attempting reconnect.');
    });

    peer.on('close', () => {
        updateStatus('PeerJS connection closed.');
        console.warn('PeerJS connection closed.');
    });

    peer.on('error', (err) => {
        updateStatus(`PeerJS Error: ${err.type}`);
        console.error('PeerJS Error:', err);
        handlePeerError(err);
    });
}

function initializeSignaling() {
    socket = io(`${API_SERVER_URL}/msg`, { path: '/socket.io' });

    socket.on('connect', () => {
        console.log('Connected to signaling server (Socket.IO)');
        updateStatus(`Connected as ${shortId(myPeerId)}. Joining chat...`);
        socket.emit('join-room', myPeerId);
    });

    socket.on('connect_error', (err) => {
        console.error('Signaling connection error:', err);
        let errorReason = err.message;
        if (err.cause) errorReason += ` (cause: ${err.cause.status || err.cause.message || 'unknown'})`;
        updateStatus(`Error connecting to signaling server: ${errorReason}`);
    });

    socket.on('disconnect', (reason) => {
        console.warn('Disconnected from signaling server:', reason);
        updateStatus('Disconnected from signaling server.');
    });

    socket.on('existing-users', (peerIds) => {
        console.log('Existing users:', peerIds);
        updateStatus(`Connected as ${shortId(myPeerId)}. ${peerIds.length} other user(s) online.`);
        peerIds.forEach(peerId => {
            if (peerId !== myPeerId && !connections[peerId]) {
                connectToPeer(peerId);
            }
        });
    });

    socket.on('user-joined', (peerId) => {
        console.log('User joined:', peerId);
        if (peerId !== myPeerId && !connections[peerId]) {
            displaySystemMessage(`${shortId(peerId)} joined the chat.`);
            connectToPeer(peerId);
        }
    });

    socket.on('user-left', (peerId) => {
        console.log('User left:', peerId);
        handlePeerLeft(peerId);
    });
}

// --- Peer Connection Handling --- (No changes from previous version)
function connectToPeer(peerId) {
    if (connections[peerId]?._temp || (connections[peerId] && connections[peerId].open)) {
        console.log(`Already connected or attempting connection to ${peerId}`);
        return;
    }
    console.log(`Attempting to connect to ${peerId}`);
    const conn = peer.connect(peerId, { reliable: true, label: myPeerId });
    connections[peerId] = { open: false, _temp: true }; // Placeholder
    setupConnection(conn);
}

function setupConnection(conn) {
    conn.on('open', () => {
        console.log(`Connection established with ${conn.peer}`);
        connections[conn.peer] = conn; // Replace placeholder
        conn.label = conn.label || conn.peer;
        updateStatus(`${Object.values(connections).filter(c => c && c.open).length} user(s) online.`);
    });

    conn.on('data', (data) => {
        console.log(`Data received from ${conn.peer}:`, data?.type);
        if (typeof data !== 'object' || data === null) {
             console.warn(`Received non-object data from ${conn.peer}:`, data);
             return;
        }
        switch (data.type) {
            case 'chat':
                handleChatMessage(conn, data);
                break;
            case 'file-meta':
                handleFileMetadata(conn.peer, data);
                break;
            case 'file-chunk':
                handleFileChunk(conn.peer, data);
                break;
            default:
                console.warn(`Received unknown data type from ${conn.peer}:`, data.type);
        }
    });

    conn.on('close', () => {
        console.log(`Connection closed with ${conn.peer}`);
        handlePeerLeft(conn.peer);
    });

    conn.on('error', (err) => {
        console.error(`Connection error with ${conn.peer}:`, err);
        if (connections[conn.peer]) {
            displaySystemMessage(`Error with ${shortId(connections[conn.peer].label || conn.peer)} connection: ${err.type}`);
            delete connections[conn.peer];
            updateStatus(`${Object.values(connections).filter(c => c && c.open).length} user(s) online.`);
            abortTransfersForPeer(conn.peer);
        }
    });
}

function handleChatMessage(conn, data) {
    if (typeof data.message === 'string') {
        const sender = data.sender || conn.label || conn.peer;
        displayMessage(sender, data.message, false);
    } else {
        console.warn(`Received malformed chat data from ${conn.peer}:`, data);
    }
}


function handlePeerLeft(peerId) {
    if (connections[peerId]) {
        const displayName = shortId(connections[peerId].label || peerId);
        displaySystemMessage(`${displayName} left or disconnected.`);
        if (connections[peerId].close) connections[peerId].close();
        delete connections[peerId];
        updateStatus(`${Object.values(connections).filter(c => c && c.open).length} user(s) online.`);
        abortTransfersForPeer(peerId);
    } else {
        displaySystemMessage(`${shortId(peerId)} left the chat.`);
        updateStatus(`${Object.values(connections).filter(c => c && c.open).length} user(s) online.`);
    }
}

function handlePeerError(err) {
     if (err.type === 'unavailable-id') {
        peer = null; // Force re-creation
        setTimeout(initializePeer, 3000);
    } else if (['network', 'server-error', 'socket-error', 'socket-closed'].includes(err.type)) {
        updateStatus('Connection issues. Check network or try refreshing.');
    } else if (err.type === 'disconnected') {
         // Handled by 'disconnected' event
    } else {
         updateStatus(`PeerJS Error: ${err.type}. May need to refresh.`);
    }
}

// --- UI and Messaging ---

function handleSendAction() {
    const messageText = messageInput.value.trim();
    const filesToSend = [...stagedFiles]; // Copy staged files

    if (!messageText && filesToSend.length === 0) return;

    const openConnections = Object.values(connections).filter(conn => conn?.open).length;
    const noReceivers = openConnections === 0;

    // 1. Send Text Message
    if (messageText) {
        sendTextMessage(messageText, noReceivers);
    }

    // 2. Send Files
    if (filesToSend.length > 0) {
        if (noReceivers) {
            displaySystemMessage(`Cannot send ${filesToSend.length} file(s): no peers connected.`);
            // Clear staged files if cannot send
            clearStagedFiles();
        } else {
            console.log(`Sending ${filesToSend.length} staged file(s)...`);
            filesToSend.forEach(file => {
                sendFile(file); // Handles its own display
            });
            clearStagedFiles(); // Clear after initiating send
        }
    }

    // 3. Clear text input
    messageInput.value = '';
}

function sendTextMessage(messageText, receivedByNone = false) {
    if (!messageText || !peer || !myPeerId) return;
    displayMessage(myPeerId, messageText, true, receivedByNone);
    if (!receivedByNone) {
        broadcastData({ type: 'chat', sender: myPeerId, message: messageText });
    }
}

function displayMessage(senderId, message, isSent, receivedByNone = false) {
    const messageElement = document.createElement('div');
    messageElement.classList.add('message');
    messageElement.classList.add(isSent ? 'sent' : 'received');

    const senderElement = document.createElement('span');
    senderElement.classList.add('sender');
    senderElement.textContent = isSent ? 'You' : shortId(senderId);
    messageElement.appendChild(senderElement);

    const rawHtml = marked.parse(message, { gfm: true, breaks: true });
    const sanitizedHtml = DOMPurify.sanitize(rawHtml, { USE_PROFILES: { html: true }, ADD_ATTR: ['target'] });

    const contentElement = document.createElement('div');
    contentElement.classList.add('message-content');
    contentElement.innerHTML = sanitizedHtml;
    contentElement.querySelectorAll('a').forEach(link => {
        if ((link.protocol === 'http:' || link.protocol === 'https:') && link.hostname !== window.location.hostname) {
            link.target = '_blank';
            link.rel = 'noopener noreferrer';
        }
    });
    messageElement.appendChild(contentElement);

    if (isSent && receivedByNone) {
        const warningElement = document.createElement('span');
        warningElement.classList.add('warning');
        warningElement.textContent = ' (No one connected)';
        messageElement.appendChild(warningElement);
    }

    messagesDiv.appendChild(messageElement);
    scrollToBottom();
}

function displaySystemMessage(message) {
    const messageElement = document.createElement('div');
    messageElement.classList.add('message', 'system');
    messageElement.textContent = message;
    messagesDiv.appendChild(messageElement);
    scrollToBottom();
}

function updateStatus(text) {
    statusDiv.textContent = text;
}


// --- File Staging Logic ---

function handleFileSelect(event) {
    const files = event.target.files;
    if (!files || files.length === 0) return;

    const currentStagedCount = stagedFiles.length;
    if (currentStagedCount + files.length > MAX_FILES) {
        displaySystemMessage(`Error: Cannot attach more files. Max is ${MAX_FILES}.`);
        fileInput.value = null;
        return;
    }

    let filesToStage = [];
    for (const file of files) {
        if (file.size > MAX_FILE_SIZE_BYTES) {
            displaySystemMessage(`Error: File "${file.name}" (${formatFileSize(file.size)}) exceeds ${MAX_FILE_SIZE_MB} MB limit.`);
            continue;
        }
        if (file.size === 0) {
            displaySystemMessage(`Warning: Skipping empty file "${file.name}".`);
            continue;
        }
        if (stagedFiles.some(staged => staged.name === file.name && staged.size === file.size && staged.lastModified === file.lastModified)) {
             displaySystemMessage(`Warning: File "${file.name}" is already attached.`);
             continue;
        }
        filesToStage.push(file);
    }

    if (filesToStage.length > 0) {
        stagedFiles.push(...filesToStage);
        updateStagedFilesUI();
    }
    fileInput.value = null; // Clear selection
}

function updateStagedFilesUI() {
    stagedFilesDisplay.innerHTML = ''; // Clear previous items
    if (stagedFiles.length === 0) {
        stagedFilesDisplay.style.display = 'none';
        return;
    }

    stagedFilesDisplay.style.display = 'flex';

    stagedFiles.forEach((file, index) => {
        const item = document.createElement('div');
        item.classList.add('staged-file-item');
        item.title = `${file.name} (${formatFileSize(file.size)})`;

        const previewContainer = document.createElement('div');
        previewContainer.classList.add('staged-preview');

        // Generate and store Object URL if needed
        let previewUrl = stagedObjectURLs.get(file);
        const isImage = file.type.startsWith('image/');
        const isVideo = file.type.startsWith('video/');

        if ((isImage || isVideo) && !previewUrl) {
            previewUrl = URL.createObjectURL(file);
            stagedObjectURLs.set(file, previewUrl);
        }

        if (isImage) {
            const img = document.createElement('img');
            img.src = previewUrl;
            img.alt = 'Image preview';
            previewContainer.appendChild(img);
        } else if (isVideo) {
            const vid = document.createElement('video');
            vid.src = previewUrl;
            vid.muted = true; // Important for preview
            vid.preload = 'metadata'; // Just load enough to show first frame potentially
            // vid.controls = false; // No controls for preview
            previewContainer.appendChild(vid);
        } else {
            // Generic file icon
            const icon = document.createElement('span');
            icon.classList.add('file-icon');
            icon.textContent = '📄'; // Simple text icon
            previewContainer.appendChild(icon);
        }
        item.appendChild(previewContainer);

        const infoSpan = document.createElement('div');
        infoSpan.classList.add('staged-file-info');
        infoSpan.textContent = file.name;
        item.appendChild(infoSpan);

        const removeButton = document.createElement('button');
        removeButton.innerHTML = '×';
        removeButton.title = 'Remove file';
        removeButton.classList.add('remove-staged');
        removeButton.dataset.index = index; // Store index based on current rendering
        removeButton.onclick = (event) => {
            // Find the actual file object based on the index *at the time of click*
            const clickedIndex = parseInt(event.target.dataset.index, 10);
             if (!isNaN(clickedIndex) && clickedIndex < stagedFiles.length) {
                 const fileToRemove = stagedFiles[clickedIndex];
                 removeStagedFile(fileToRemove);
             } else {
                 console.error("Could not find file to remove by index:", clickedIndex);
                 // Fallback: try to find by DOM structure (less reliable)
                 const itemToRemove = event.target.closest('.staged-file-item');
                 // Find index based on node list (if needed)
                 // ... but better to rely on finding the file object.
             }
        };
        item.appendChild(removeButton);

        stagedFilesDisplay.appendChild(item);
    });
}

// Modified to accept file object for reliable removal and URL revocation
function removeStagedFile(fileToRemove) {
    const indexToRemove = stagedFiles.findIndex(f => f === fileToRemove);
    if (indexToRemove > -1) {
        stagedFiles.splice(indexToRemove, 1);

        // Revoke Object URL if it exists for this file
        const url = stagedObjectURLs.get(fileToRemove);
        if (url) {
            URL.revokeObjectURL(url);
            stagedObjectURLs.delete(fileToRemove);
            console.log(`Revoked staged preview URL for ${fileToRemove.name}`);
        }

        updateStagedFilesUI(); // Re-render the list
    } else {
         console.warn("Could not find file to remove in stagedFiles array:", fileToRemove?.name);
    }
}

// NEW: Clear all staged files and revoke URLs
function clearStagedFiles() {
    stagedFiles.forEach(file => {
        const url = stagedObjectURLs.get(file);
        if (url) {
            URL.revokeObjectURL(url);
        }
    });
    stagedFiles = [];
    stagedObjectURLs.clear();
    updateStagedFilesUI();
}

// --- File Transfer Logic ---

function sendFile(file) {
    const fileId = uuid.v4();
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);

    const metadata = {
        type: 'file-meta',
        sender: myPeerId,
        fileId: fileId,
        fileName: file.name,
        fileSize: file.size,
        fileType: file.type || 'application/octet-stream',
        totalChunks: totalChunks
    };

    // Display placeholder locally first
    const { messageElement, progressElement, statusElement, previewContainerElement } = displayFileTransfer(
        myPeerId,
        metadata,
        true // isSent = true
    );

    let previewObjectURL = null;
    // Create preview for sent file if image/video
    if (metadata.fileType.startsWith('image/') || metadata.fileType.startsWith('video/')) {
        previewObjectURL = URL.createObjectURL(file); // Use the original file
        const previewElement = metadata.fileType.startsWith('image/') ? document.createElement('img') : document.createElement('video');
        previewElement.src = previewObjectURL;
        if (metadata.fileType.startsWith('video/')) {
            previewElement.muted = true;
            previewElement.preload = 'metadata';
        }
        previewContainerElement.innerHTML = ''; // Clear placeholder
        previewContainerElement.appendChild(previewElement);
    }

    const peersToSendTo = Object.keys(connections).filter(id => connections[id]?.open);
    outgoingFiles[fileId] = {
        meta: metadata,
        file: file,
        progressElement: progressElement,
        statusElement: statusElement,
        messageElement: messageElement, // Store message element
        previewContainerElement: previewContainerElement, // Store preview container
        previewObjectURL: previewObjectURL, // Store URL to revoke later if needed
        sentChunks: 0,
        peers: peersToSendTo
    };

    if (peersToSendTo.length === 0) {
        console.warn(`No peers to send file ${file.name} (ID: ${fileId}) to.`);
        updateFileProgress(fileId, 0, 'Send failed (no connections)');
        messageElement.classList.add('transfer-complete'); // Hide progress bar even on failure
        // Don't delete outgoingFiles[fileId] immediately, keep the message visible
        return;
    }

    updateFileProgress(fileId, 0, 'Starting transfer...');
    console.log(`Sending metadata for ${file.name} (ID: ${fileId}) to ${peersToSendTo.length} peers`);
    broadcastData(metadata);
    sendChunk(fileId, 0);
}

function sendChunk(fileId, chunkIndex) {
    const transfer = outgoingFiles[fileId];
    if (!transfer) return;

    const { file, meta } = transfer;
    if (chunkIndex >= meta.totalChunks) return;

    const start = chunkIndex * CHUNK_SIZE;
    const end = Math.min(start + CHUNK_SIZE, meta.fileSize);
    const chunk = file.slice(start, end);

    const reader = new FileReader();
    reader.onload = (event) => {
        if (!outgoingFiles[fileId]) return; // Check if aborted during read

        const chunkData = event.target.result; // ArrayBuffer
        const chunkPayload = { type: 'file-chunk', fileId: fileId, chunkIndex: chunkIndex, data: chunkData };
        let success = false;

        // Create a copy of peers to iterate over, as connection errors might modify transfer.peers
        const currentPeers = [...transfer.peers];
        currentPeers.forEach(peerId => {
            if (connections[peerId]?.open) {
                try {
                    connections[peerId].send(chunkPayload);
                    success = true;
                } catch (error) {
                    console.error(`Error sending chunk ${chunkIndex} of ${fileId} to ${peerId}:`, error);
                    // Rely on PeerJS connection events ('error', 'close') to handle peer removal
                }
            } else if (transfer.peers.includes(peerId)) {
                 // If peer is in our list but connection is no longer open, remove them
                 console.warn(`Removing non-open peer ${peerId} from ongoing transfer ${fileId}`);
                 transfer.peers.splice(transfer.peers.indexOf(peerId), 1);
            }
        });

        // Check if *any* peer is still targeted *after* attempting send
        if (transfer.peers.length === 0) {
             console.error(`No remaining peers for transfer ${fileId}. Aborting.`);
             updateFileProgress(fileId, transfer.sentChunks / meta.totalChunks, 'Transfer failed (peers disconnected)');
             transfer.messageElement.classList.add('transfer-complete'); // Hide progress bar
             // Optionally revoke preview URL here if desired
             // if (transfer.previewObjectURL) URL.revokeObjectURL(transfer.previewObjectURL);
             // delete outgoingFiles[fileId]; // Keep entry to show status
             return;
        }

        transfer.sentChunks++;
        const progress = transfer.sentChunks / meta.totalChunks;
        updateFileProgress(fileId, progress, `Sending... (${transfer.sentChunks}/${meta.totalChunks})`);

        if (transfer.sentChunks === meta.totalChunks) {
            console.log(`Finished sending all chunks for ${file.name} (ID: ${fileId})`);
            updateFileProgress(fileId, 1, 'Sent');
            transfer.messageElement.classList.add('transfer-complete'); // Add class to hide progress bar
            // Optionally revoke preview URL here after a delay, or on unload
            // if (transfer.previewObjectURL) { ... }
        } else {
            setTimeout(() => sendChunk(fileId, chunkIndex + 1), 0); // Next chunk
        }
    };

    reader.onerror = (event) => {
        console.error(`Error reading chunk ${chunkIndex} for file ${fileId}:`, event.target.error);
        updateFileProgress(fileId, transfer.sentChunks / meta.totalChunks, 'Error reading file chunk.');
        transfer.messageElement.classList.add('transfer-complete'); // Hide progress bar
        // delete outgoingFiles[fileId]; // Keep entry to show status
    };

    reader.readAsArrayBuffer(chunk);
}

function handleFileMetadata(senderId, metadata) {
    const { fileId, fileName, fileSize, fileType, totalChunks } = metadata;
    if (!fileId || !fileName || typeof fileSize !== 'number' || typeof totalChunks !== 'number') {
        console.warn(`Received invalid file metadata from ${senderId}:`, metadata);
        return;
    }
    if (incomingFiles[fileId] || outgoingFiles[fileId]) { // Check both just in case
        console.warn(`Received metadata for existing transfer ID ${fileId}. Ignoring.`);
        return;
    }

    console.log(`Received metadata for ${fileName} (${formatFileSize(fileSize)}) from ${senderId}`);

    const { messageElement, progressElement, statusElement, downloadLink, previewContainerElement } = displayFileTransfer(
        senderId,
        metadata,
        false // isSent = false
    );

    incomingFiles[fileId] = {
        meta: metadata,
        chunks: [],
        receivedChunks: 0,
        progressElement: progressElement,
        statusElement: statusElement,
        downloadLink: downloadLink,
        messageElement: messageElement, // Store message element
        previewContainerElement: previewContainerElement, // Store preview container
        senderId: senderId
    };

    updateFileProgress(fileId, 0, 'Waiting for data...');
}

function handleFileChunk(senderId, chunkData) {
    const { fileId, chunkIndex, data } = chunkData;
    const transfer = incomingFiles[fileId];
    if (!transfer) return;

    // Accept ArrayBuffer or Uint8Array
    if (typeof chunkIndex !== 'number' || !(data instanceof ArrayBuffer || data instanceof Uint8Array)) {
        console.warn(`Received invalid chunk data type from ${senderId} for ${fileId}. Got:`, typeof data);
        return;
    }
    if (transfer.chunks[chunkIndex]) return; // Duplicate chunk

    transfer.chunks[chunkIndex] = data;
    transfer.receivedChunks++;

    const progress = transfer.receivedChunks / transfer.meta.totalChunks;
    updateFileProgress(fileId, progress, `Receiving... (${transfer.receivedChunks}/${transfer.meta.totalChunks})`);

    if (transfer.receivedChunks === transfer.meta.totalChunks) {
        console.log(`Received all chunks for ${transfer.meta.fileName} (ID: ${fileId})`);
        let allChunksPresent = true;
        for(let i = 0; i < transfer.meta.totalChunks; i++) {
            if (!transfer.chunks[i]) {
                allChunksPresent = false;
                console.error(`Missing chunk ${i} for ${fileId}.`);
                updateFileProgress(fileId, progress, `Error: Missing chunk ${i}`);
                transfer.messageElement.classList.add('transfer-complete'); // Hide progress
                if (transfer.downloadLink) {
                    transfer.downloadLink.textContent = 'Download Failed';
                    transfer.downloadLink.setAttribute('disabled', 'true');
                }
                break;
            }
        }

        if (allChunksPresent) {
            updateFileProgress(fileId, 1, 'Assembling file...');
            setTimeout(() => assembleFile(fileId), 0); // Assemble async
        }
    }
}

function assembleFile(fileId) {
    const transfer = incomingFiles[fileId];
    if (!transfer || transfer.receivedChunks !== transfer.meta.totalChunks || transfer.chunks.length !== transfer.meta.totalChunks) {
        console.error(`Cannot assemble file ${fileId}: Incomplete data.`);
        const progress = transfer ? (transfer.receivedChunks / transfer.meta.totalChunks) : 0;
        updateFileProgress(fileId, progress, 'Assembly failed: Incomplete data');
        if(transfer?.messageElement) transfer.messageElement.classList.add('transfer-complete');
        if(transfer?.downloadLink) {
            transfer.downloadLink.textContent = 'Download Failed';
            transfer.downloadLink.setAttribute('disabled', 'true');
        }
        return;
    }

    console.log(`Assembling file ${transfer.meta.fileName} (ID: ${fileId})`);

    try {
        const fileBlob = new Blob(transfer.chunks, { type: transfer.meta.fileType });

        if (fileBlob.size !== transfer.meta.fileSize) {
             console.warn(`Assembled file size (${fileBlob.size}) mismatch metadata (${transfer.meta.fileSize}) for ${fileId}.`);
        }

        const objectURL = URL.createObjectURL(fileBlob);

        // Update download link
        transfer.downloadLink.href = objectURL;
        transfer.downloadLink.download = transfer.meta.fileName;
        transfer.downloadLink.removeAttribute('disabled');
        transfer.downloadLink.textContent = `Download ${transfer.meta.fileName}`; // Keep it concise

        // Update status and hide progress bar
        updateFileProgress(fileId, 1, 'Ready to download');
        transfer.messageElement.classList.add('transfer-complete');

        // Create preview if image/video
        if (transfer.meta.fileType.startsWith('image/') || transfer.meta.fileType.startsWith('video/')) {
            const previewElement = transfer.meta.fileType.startsWith('image/') ? document.createElement('img') : document.createElement('video');
            previewElement.src = objectURL; // Use the same URL as download
             if (transfer.meta.fileType.startsWith('video/')) {
                previewElement.muted = true;
                previewElement.preload = 'metadata';
            }
            transfer.previewContainerElement.innerHTML = ''; // Clear placeholder
            transfer.previewContainerElement.appendChild(previewElement);
        }

        console.log(`File ${transfer.meta.fileName} (ID: ${fileId}) ready.`);

        // Clean up chunk data
        transfer.chunks = [];

        // Revoke URL after download (or on unload)
        transfer.downloadLink.addEventListener('click', () => {
             setTimeout(() => {
                 // Check if URL still exists before revoking
                 if (transfer.downloadLink.href.startsWith('blob:')) {
                    // Note: Revoking here might break the preview if user doesn't navigate away.
                    // It's safer to rely on the beforeunload cleanup.
                    // URL.revokeObjectURL(transfer.downloadLink.href);
                    // console.log(`Revoked object URL for ${fileId} after click`);
                 }
             }, 100);
        }, { once: true });

    } catch (error) {
        console.error(`Error assembling file ${fileId}:`, error);
        updateFileProgress(fileId, transfer.receivedChunks / transfer.meta.totalChunks, `Assembly failed: ${error.message}`);
        transfer.messageElement.classList.add('transfer-complete');
        if (transfer.downloadLink) {
            transfer.downloadLink.textContent = 'Download Failed';
            transfer.downloadLink.removeAttribute('href');
            transfer.downloadLink.setAttribute('disabled', 'true');
        }
        // delete incomingFiles[fileId]; // Keep entry to show status
    }
}

// MODIFIED: Display structure for file transfer including preview area
function displayFileTransfer(senderId, metadata, isSent) {
    const { fileId, fileName, fileSize, fileType } = metadata;
    const messageElement = document.createElement('div');
    messageElement.classList.add('message', 'file-transfer');
    messageElement.classList.add(isSent ? 'sent' : 'received');
    messageElement.dataset.fileId = fileId;

    const senderElement = document.createElement('span');
    senderElement.classList.add('sender');
    senderElement.textContent = isSent ? 'You' : shortId(senderId);
    messageElement.appendChild(senderElement);

    // Main container for preview + details
    const contentContainer = document.createElement('div');
    contentContainer.classList.add('file-transfer-content');

    // Preview container (initially placeholder)
    const previewContainerElement = document.createElement('div');
    previewContainerElement.classList.add('file-preview');
    const icon = document.createElement('span');
    icon.classList.add('file-icon');
    icon.textContent = '📄'; // Default icon
    previewContainerElement.appendChild(icon);
    contentContainer.appendChild(previewContainerElement);

    // Details container
    const detailsContainer = document.createElement('div');
    detailsContainer.classList.add('file-details');

    const fileInfo = document.createElement('div');
    fileInfo.classList.add('file-info');
    fileInfo.textContent = fileName;
    fileInfo.title = fileName; // Tooltip for long names
    detailsContainer.appendChild(fileInfo);

    const fileSizeInfo = document.createElement('div');
    fileSizeInfo.classList.add('file-size');
    fileSizeInfo.textContent = formatFileSize(fileSize); // Use helper
    detailsContainer.appendChild(fileSizeInfo);

    const progressContainer = document.createElement('div');
    progressContainer.classList.add('file-progress-container');
    const progressElement = document.createElement('div');
    progressElement.classList.add('file-progress-bar');
    progressContainer.appendChild(progressElement);
    detailsContainer.appendChild(progressContainer);

    const statusElement = document.createElement('div');
    statusElement.classList.add('file-status');
    statusElement.textContent = 'Initializing...';
    detailsContainer.appendChild(statusElement);

    let downloadLink = null;
    if (!isSent) {
        downloadLink = document.createElement('a');
        downloadLink.classList.add('download-link');
        downloadLink.textContent = 'Preparing...';
        downloadLink.setAttribute('disabled', 'true');
        detailsContainer.appendChild(downloadLink);
    }

    contentContainer.appendChild(detailsContainer);
    messageElement.appendChild(contentContainer);

    messagesDiv.appendChild(messageElement);
    scrollToBottom();

    // Return all necessary elements
    return { messageElement, progressElement, statusElement, downloadLink, previewContainerElement };
}

// updateFileProgress (No changes needed, completion class handles hiding)
function updateFileProgress(fileId, progress, statusText) {
    const transfer = outgoingFiles[fileId] || incomingFiles[fileId];
    if (!transfer) return;

    if (transfer.progressElement) {
        transfer.progressElement.style.width = `${Math.min(100, Math.round(progress * 100))}%`;
    }
    if (transfer.statusElement) {
        transfer.statusElement.textContent = statusText;
    }
}

// abortTransfersForPeer (Added transfer-complete class on abort)
function abortTransfersForPeer(peerId) {
    console.log(`Aborting transfers involving peer ${peerId}`);

    // Abort outgoing transfers TO this peer
    for (const fileId in outgoingFiles) {
        const transfer = outgoingFiles[fileId];
        const peerIndex = transfer.peers.indexOf(peerId);
        if (peerIndex > -1) {
            transfer.peers.splice(peerIndex, 1);
            console.log(`Removed peer ${peerId} from outgoing transfer ${fileId}`);
            if (transfer.peers.length === 0 && transfer.sentChunks < transfer.meta.totalChunks) {
                 updateFileProgress(fileId, transfer.sentChunks / transfer.meta.totalChunks, 'Transfer failed (peer disconnected)');
                 if(transfer.messageElement) transfer.messageElement.classList.add('transfer-complete'); // Hide progress
                 console.log(`Outgoing transfer ${fileId} aborted as last peer disconnected.`);
                 // Optionally revoke preview URL
                 // if (transfer.previewObjectURL) URL.revokeObjectURL(transfer.previewObjectURL);
                 // delete outgoingFiles[fileId]; // Keep entry to show status
            }
        }
    }

    // Abort incoming transfers FROM this peer
    for (const fileId in incomingFiles) {
        const transfer = incomingFiles[fileId];
        // Check if not already completed
        if (transfer.senderId === peerId && !transfer.messageElement.classList.contains('transfer-complete')) {
             console.log(`Aborting incoming transfer ${fileId} from disconnected peer ${peerId}`);
             updateFileProgress(fileId, transfer.receivedChunks / transfer.meta.totalChunks, 'Transfer failed (sender disconnected)');
             if(transfer.messageElement) transfer.messageElement.classList.add('transfer-complete'); // Hide progress
             if (transfer.downloadLink) {
                 transfer.downloadLink.textContent = 'Download Failed';
                 transfer.downloadLink.setAttribute('disabled', 'true');
                 transfer.downloadLink.removeAttribute('href');
                 // Rely on page unload to revoke blob URL if it was created
             }
             // delete incomingFiles[fileId]; // Keep entry to show status
        }
    }
}

// broadcastData (No changes needed)
function broadcastData(data) {
     console.log(`Broadcasting data to ${Object.values(connections).filter(c=>c?.open).length} peers:`, data.type);
     let sentToAny = false;
     for (const peerId in connections) {
        if (connections[peerId]?.open && connections[peerId].send) {
            try {
                connections[peerId].send(data);
                sentToAny = true;
            } catch (error) {
                console.error(`Error broadcasting data to ${peerId}:`, error);
                handlePeerLeft(peerId); // Assume connection is dead
            }
        }
     }
     if (!sentToAny && Object.keys(connections).length > 0) {
         console.warn("Broadcast attempted but no connections were open.");
     }
     return sentToAny;
}

// --- Event Listeners ---

sendButton.addEventListener('click', handleSendAction);
messageInput.addEventListener('keypress', (event) => {
    if (event.key === 'Enter' && !event.shiftKey) {
        event.preventDefault();
        handleSendAction();
    }
});

uploadButton.addEventListener('click', () => fileInput.click());
fileInput.addEventListener('change', handleFileSelect);

document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible') {
        if (peer?.disconnected && !peer.destroyed) {
            console.log('Tab visible, attempting PeerJS reconnect.');
            peer.reconnect();
        }
        if (socket && !socket.connected) {
            console.log('Tab visible, attempting Socket.IO reconnect.');
            socket.connect();
        }
    }
});

// Cleanup object URLs on page unload
window.addEventListener('beforeunload', () => {
    // Revoke staged file preview URLs
    stagedObjectURLs.forEach(url => URL.revokeObjectURL(url));
    stagedObjectURLs.clear();
    console.log("Revoked staged preview URLs on page unload");

    // Revoke completed incoming file URLs (download/preview)
    for (const fileId in incomingFiles) {
        const transfer = incomingFiles[fileId];
        if (transfer.downloadLink && transfer.downloadLink.href.startsWith('blob:')) {
            URL.revokeObjectURL(transfer.downloadLink.href);
            console.log(`Revoked object URL for incoming file ${fileId} on page unload`);
        }
    }
     // Revoke sent file preview URLs (if any were stored and not cleaned up)
    for (const fileId in outgoingFiles) {
        const transfer = outgoingFiles[fileId];
        if (transfer.previewObjectURL) {
             URL.revokeObjectURL(transfer.previewObjectURL);
             console.log(`Revoked object URL for outgoing preview ${fileId} on page unload`);
        }
    }

    if (peer && !peer.destroyed) peer.destroy();
    if (socket?.connected) socket.disconnect();
});


// --- Start the application ---
initializePeer();
updateStagedFilesUI(); // Initial setup for staged display (hidden if empty)