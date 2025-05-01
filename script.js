const messagesDiv = document.getElementById('messages');
const messageInput = document.getElementById('message-input');
const sendButton = document.getElementById('send-button');
const statusDiv = document.getElementById('status');
const uploadButton = document.getElementById('upload-button');
const fileInput = document.getElementById('file-input');
const stagedFilesDisplay = document.getElementById('staged-files-display'); // Added

// const API_SERVER_URL = 'https://api.totob12.com'; // Your API server URL
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
const outgoingFiles = {}; // { fileId: { meta, progressElement, statusElement } }
const incomingFiles = {}; // { fileId: { meta, chunks: [], progressElement, statusElement, downloadLink } }

// Staging area for files before sending
let stagedFiles = []; // Array to hold File objects

// Configure DOMPurify
DOMPurify.setConfig({ ADD_ATTR: ['target'] });

// --- Initialization --- (No changes from previous version)
function initializePeer() {
    peer = new Peer(undefined, PEERJS_CONFIG); // Let PeerJS generate an ID

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
        // PeerJS attempts auto-reconnect
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
    socket = io(`${API_SERVER_URL}/msg`, {
        path: '/socket.io',
        // Consider adding reconnection options if needed
        // reconnectionAttempts: 5,
        // reconnectionDelay: 3000,
    });

    socket.on('connect', () => {
        console.log('Connected to signaling server (Socket.IO)');
        updateStatus(`Connected as ${shortId(myPeerId)}. Joining chat...`);
        socket.emit('join-room', myPeerId);
    });

    socket.on('connect_error', (err) => {
        console.error('Signaling connection error:', err);
        let errorReason = err.message;
        if (err.cause) {
            errorReason += ` (cause: ${err.cause.status || err.cause.message || 'unknown'})`;
        }
        updateStatus(`Error connecting to signaling server: ${errorReason}`);
    });

    socket.on('disconnect', (reason) => {
        console.warn('Disconnected from signaling server:', reason);
        updateStatus('Disconnected from signaling server.');
        // Handle potential need to clear connections if server disconnects unexpectedly
        // clearAllConnections(); // Or implement more robust state handling
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
    if (connections[peerId]) {
        console.log(`Already connected or attempting connection to ${peerId}`);
        return;
    }
    console.log(`Attempting to connect to ${peerId}`);
    const conn = peer.connect(peerId, {
        reliable: true,
        label: myPeerId,
        // Serialization might need adjustment for large binary data if issues arise
        // serialization: 'binary', // 'binary' is often default and good for files
    });
    // Add temporary placeholder to prevent reconnect attempts while opening
    connections[peerId] = { open: false, _temp: true };
    setupConnection(conn);
}

function setupConnection(conn) {
    conn.on('open', () => {
        console.log(`Connection established with ${conn.peer}`);
        // Replace placeholder with the actual connection object
        connections[conn.peer] = conn;
        conn.label = conn.label || conn.peer; // Store the sender's ID
        updateStatus(`${Object.keys(connections).length} user(s) online.`);
        // Optional: Send pending messages/files if any were queued
    });

    conn.on('data', (data) => {
        console.log(`Data received from ${conn.peer}:`, data);
        // Ensure data is an object before accessing type
        if (typeof data !== 'object' || data === null) {
             console.warn(`Received non-object data from ${conn.peer}:`, data);
             return;
        }

        switch (data.type) {
            case 'chat':
                if (typeof data.message === 'string') {
                    const sender = data.sender || conn.label || conn.peer;
                    displayMessage(sender, data.message, false);
                } else {
                    console.warn(`Received malformed chat data from ${conn.peer}:`, data);
                }
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
        handlePeerLeft(conn.peer); // Use the same logic as user-left event
    });

    conn.on('error', (err) => {
        console.error(`Connection error with ${conn.peer}:`, err);
        if (connections[conn.peer]) {
            displaySystemMessage(`Error with ${shortId(connections[conn.peer].label || conn.peer)} connection: ${err.type}`);
            // Clean up connection state on error
            delete connections[conn.peer];
            updateStatus(`${Object.keys(connections).length} user(s) online.`);
            // Abort any ongoing file transfers with this peer
            abortTransfersForPeer(conn.peer);
        }
    });
}

function handlePeerLeft(peerId) {
    if (connections[peerId]) {
        const displayName = shortId(connections[peerId].label || peerId);
        displaySystemMessage(`${displayName} left or disconnected.`);
        if (connections[peerId].close) {
           connections[peerId].close(); // Ensure connection is closed
        }
        delete connections[peerId];
        updateStatus(`${Object.keys(connections).length} user(s) online.`);
        // Abort any ongoing file transfers involving this peer
        abortTransfersForPeer(peerId);
    } else {
        // Might receive user-left before connection fully established
        displaySystemMessage(`${shortId(peerId)} left the chat.`);
        updateStatus(`${Object.keys(connections).length} user(s) online.`);
    }
}

function handlePeerError(err) {
     if (err.type === 'unavailable-id') {
        peer = null; // Force re-creation
        setTimeout(initializePeer, 3000);
    } else if (err.type === 'network' || err.type === 'server-error' || err.type === 'socket-error' || err.type === 'socket-closed') {
        updateStatus('Connection issues. Check network or try refreshing.');
        // Consider more robust reconnection logic here if needed
    } else if (err.type === 'disconnected') {
         // Already handled by 'disconnected' event
    } else {
         // General PeerJS error
         updateStatus(`PeerJS Error: ${err.type}. May need to refresh.`);
    }
}

// --- UI and Messaging ---

// NEW: Handles the send button click for text and/or staged files
function handleSendAction() {
    const messageText = messageInput.value.trim();
    const filesToSend = [...stagedFiles]; // Copy staged files

    if (!messageText && filesToSend.length === 0) {
        return; // Nothing to send
    }

    const openConnections = Object.values(connections).filter(conn => conn && conn.open).length;
    const noReceivers = openConnections === 0;

    // 1. Send Text Message (if any)
    if (messageText) {
        sendTextMessage(messageText, noReceivers);
    }

    // 2. Send Files (if any)
    if (filesToSend.length > 0) {
        if (noReceivers) {
            displaySystemMessage(`Cannot send ${filesToSend.length} file(s): no peers connected.`);
            // Keep files staged in case connection resumes? Or clear them? Let's clear them for simplicity.
            stagedFiles = [];
            updateStagedFilesUI();
        } else {
            console.log(`Sending ${filesToSend.length} staged file(s)...`);
            filesToSend.forEach(file => {
                sendFile(file); // sendFile now displays its own message/progress
            });
            stagedFiles = []; // Clear staged files after initiating send
            updateStagedFilesUI();
        }
    }

    // 3. Clear text input
    messageInput.value = '';
}


// RENAMED: Function specifically for sending text messages
function sendTextMessage(messageText, receivedByNone = false) {
    if (!messageText || !peer || !myPeerId) {
        return;
    }

    // Display message locally immediately
    displayMessage(myPeerId, messageText, true, receivedByNone);

    if (!receivedByNone) {
        const messagePayload = {
            type: 'chat',
            sender: myPeerId,
            message: messageText
        };
        broadcastData(messagePayload);
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
    const sanitizedHtml = DOMPurify.sanitize(rawHtml, {
        USE_PROFILES: { html: true },
        ADD_ATTR: ['target'],
        FORBID_TAGS: ['style'],
        FORBID_ATTR: ['style']
    });

    const contentElement = document.createElement('div');
    contentElement.classList.add('message-content');
    contentElement.innerHTML = sanitizedHtml;

    contentElement.querySelectorAll('a').forEach(link => {
        if ((link.href.startsWith('http://') || link.href.startsWith('https://')) && link.hostname !== window.location.hostname) {
            link.target = '_blank';
            link.rel = 'noopener noreferrer';
        }
    });

    messageElement.appendChild(contentElement);

    if (isSent && receivedByNone) {
        const warningElement = document.createElement('span');
        warningElement.classList.add('warning');
        warningElement.textContent = ' (No one connected to receive)';
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

function shortId(id) {
    if (!id) return 'Anonymous';
    const idStr = String(id);
    return idStr.length > 8 ? `${idStr.substring(0, 4)}...${idStr.substring(idStr.length - 4)}` : idStr;
}

function scrollToBottom() {
    // Add a small delay to allow the DOM to update, especially after adding multiple elements
    setTimeout(() => {
        messagesDiv.scrollTop = messagesDiv.scrollHeight;
    }, 50);
}

// --- File Staging Logic ---

// MODIFIED: Validates files and adds them to the staging array
function handleFileSelect(event) {
    const files = event.target.files;
    if (!files || files.length === 0) {
        return;
    }

    const currentStagedCount = stagedFiles.length;
    if (currentStagedCount + files.length > MAX_FILES) {
        displaySystemMessage(`Error: Cannot attach more files. Maximum is ${MAX_FILES} (currently ${currentStagedCount} attached).`);
        fileInput.value = null; // Clear selection
        return;
    }

    let filesToStage = [];
    let validationFailed = false;
    for (const file of files) {
        if (file.size > MAX_FILE_SIZE_BYTES) {
            displaySystemMessage(`Error: File "${file.name}" (${(file.size / 1024 / 1024).toFixed(1)} MB) exceeds the ${MAX_FILE_SIZE_MB} MB limit. It won't be attached.`);
            validationFailed = true;
            continue; // Skip this file
        }
        if (file.size === 0) {
            displaySystemMessage(`Warning: Skipping empty file "${file.name}".`);
            validationFailed = true;
            continue; // Skip this file
        }
        // Check for duplicates already staged
        if (stagedFiles.some(staged => staged.name === file.name && staged.size === file.size && staged.lastModified === file.lastModified)) {
             displaySystemMessage(`Warning: File "${file.name}" is already attached. Skipping duplicate.`);
             validationFailed = true;
             continue;
        }
        filesToStage.push(file);
    }

    if (filesToStage.length > 0) {
        stagedFiles.push(...filesToStage);
        updateStagedFilesUI();
    }

    // Clear the file input so the same file(s) can be selected again if removed
    fileInput.value = null;
}

// NEW: Renders the list of staged files in the UI
function updateStagedFilesUI() {
    stagedFilesDisplay.innerHTML = ''; // Clear previous items
    if (stagedFiles.length === 0) {
        stagedFilesDisplay.style.display = 'none'; // Hide if empty
        return;
    }

    stagedFilesDisplay.style.display = 'flex'; // Show the container

    stagedFiles.forEach((file, index) => {
        const item = document.createElement('div');
        item.classList.add('staged-file-item');
        item.title = `${file.name} (${(file.size / 1024 / 1024).toFixed(2)} MB)`;

        const nameSpan = document.createElement('span');
        nameSpan.textContent = file.name;
        item.appendChild(nameSpan);

        const removeButton = document.createElement('button');
        removeButton.innerHTML = '×'; // Multiplication sign as 'x'
        removeButton.title = 'Remove file';
        removeButton.dataset.index = index; // Store index to know which file to remove
        removeButton.onclick = (event) => {
            const indexToRemove = parseInt(event.target.dataset.index, 10);
            removeStagedFile(indexToRemove);
        };
        item.appendChild(removeButton);

        stagedFilesDisplay.appendChild(item);
    });
}

// NEW: Removes a file from the staging array and updates UI
function removeStagedFile(index) {
    if (index >= 0 && index < stagedFiles.length) {
        const removedFile = stagedFiles.splice(index, 1);
        console.log(`Removed staged file: ${removedFile[0]?.name}`);
        updateStagedFilesUI(); // Re-render the list
    }
}


// --- File Transfer Logic ---

// MODIFIED: Called by handleSendAction for each staged file
function sendFile(file) {
    // This function now assumes it's okay to send (connection check done in handleSendAction)
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

    // Display placeholder in chat locally WHEN SENDING starts
    const { progressElement, statusElement } = displayFileTransfer(
        myPeerId,
        metadata,
        true // isSent = true
    );

    outgoingFiles[fileId] = {
        meta: metadata,
        file: file,
        progressElement: progressElement,
        statusElement: statusElement,
        sentChunks: 0,
        // Get current list of open connections *at the time of sending*
        peers: Object.keys(connections).filter(id => connections[id] && connections[id].open)
    };

    if (outgoingFiles[fileId].peers.length === 0) {
        // Should ideally not happen due to check in handleSendAction, but as a safeguard:
        console.warn(`No peers to send file ${file.name} (ID: ${fileId}) to.`);
        updateFileProgress(fileId, 0, 'Send failed (no connections)');
        delete outgoingFiles[fileId]; // Clean up immediately
        return; // Don't proceed
    }

    updateFileProgress(fileId, 0, 'Starting transfer...');

    console.log(`Sending metadata for ${file.name} (ID: ${fileId}) to ${outgoingFiles[fileId].peers.length} peers`);
    broadcastData(metadata); // Send metadata first

    // Start sending chunks
    sendChunk(fileId, 0);
}

// sendChunk: Reads file slice, sends chunk payload (No changes needed here from previous version)
function sendChunk(fileId, chunkIndex) {
    const transfer = outgoingFiles[fileId];
    if (!transfer) {
        console.warn(`sendChunk: Transfer ${fileId} not found or aborted.`);
        return;
    }

    const { file, meta } = transfer;
    if (chunkIndex >= meta.totalChunks) {
        console.log(`sendChunk: All chunks seemingly sent for ${fileId}`);
        return;
    }

    const start = chunkIndex * CHUNK_SIZE;
    const end = Math.min(start + CHUNK_SIZE, meta.fileSize);
    const chunk = file.slice(start, end);

    const reader = new FileReader();
    reader.onload = (event) => {
        if (!outgoingFiles[fileId]) {
            console.warn(`sendChunk (onload): Transfer ${fileId} aborted during chunk read.`);
            return;
        }
        const chunkData = event.target.result; // This is an ArrayBuffer
        const chunkPayload = {
            type: 'file-chunk',
            fileId: fileId,
            chunkIndex: chunkIndex,
            data: chunkData // Send ArrayBuffer
        };

        let success = false;
        transfer.peers.forEach(peerId => {
            if (connections[peerId] && connections[peerId].open) {
                try {
                    // PeerJS handles sending the ArrayBuffer efficiently
                    connections[peerId].send(chunkPayload);
                    success = true;
                } catch (error) {
                    console.error(`Error sending chunk ${chunkIndex} of ${fileId} to ${peerId}:`, error);
                    // If send fails, connection might be closing/closed.
                    // handlePeerLeft(peerId) // This might be too aggressive, rely on PeerJS error/close events
                }
            } else {
                 console.warn(`Cannot send chunk ${chunkIndex} of ${fileId} to ${peerId}: connection not open.`);
                 // Remove peer from this transfer if connection is gone?
                 // transfer.peers = transfer.peers.filter(p => p !== peerId);
            }
        });

        if (!success && transfer.peers.length > 0) {
             console.error(`Failed to send chunk ${chunkIndex} of ${fileId} to any target peers.`);
             updateFileProgress(fileId, transfer.sentChunks / meta.totalChunks, 'Error sending chunk.');
             // Consider aborting the transfer here
             // delete outgoingFiles[fileId];
             return;
        }


        transfer.sentChunks++;
        const progress = transfer.sentChunks / meta.totalChunks;
        updateFileProgress(fileId, progress, `Sending... (${transfer.sentChunks}/${meta.totalChunks})`);

        if (transfer.sentChunks === meta.totalChunks) {
            console.log(`Finished sending all chunks for ${file.name} (ID: ${fileId})`);
            updateFileProgress(fileId, 1, 'Sent');
            // Keep the outgoing file entry to show "Sent" status
        } else {
            // Use setTimeout for flow control and prevent blocking
            setTimeout(() => sendChunk(fileId, chunkIndex + 1), 0);
        }
    };

    reader.onerror = (event) => {
        console.error(`Error reading chunk ${chunkIndex} for file ${fileId}:`, event.target.error);
        updateFileProgress(fileId, transfer.sentChunks / meta.totalChunks, 'Error reading file chunk.');
        delete outgoingFiles[fileId]; // Abort on read error
    };

    reader.readAsArrayBuffer(chunk);
}

// handleFileMetadata (No changes needed here from previous version)
function handleFileMetadata(senderId, metadata) {
    const { fileId, fileName, fileSize, fileType, totalChunks } = metadata;

    if (!fileId || !fileName || typeof fileSize !== 'number' || typeof totalChunks !== 'number') {
        console.warn(`Received invalid file metadata from ${senderId}:`, metadata);
        return;
    }
     // Avoid displaying duplicate transfers if metadata is somehow resent
    if (incomingFiles[fileId]) {
        console.warn(`Received duplicate metadata for file ID ${fileId}. Ignoring.`);
        return;
    }


    console.log(`Received metadata for ${fileName} (ID: ${fileId}) from ${senderId}`);

    const { messageElement, progressElement, statusElement, downloadLink } = displayFileTransfer(
        senderId,
        metadata,
        false // isSent = false
    );

    incomingFiles[fileId] = {
        meta: metadata,
        chunks: [], // Store received chunks (expecting ArrayBuffer or Uint8Array)
        receivedChunks: 0,
        progressElement: progressElement,
        statusElement: statusElement,
        downloadLink: downloadLink,
        senderId: senderId
    };

    updateFileProgress(fileId, 0, 'Waiting for data...');
}


// MODIFIED: Handle Chunk Data Type Issue
function handleFileChunk(senderId, chunkData) {
    const { fileId, chunkIndex, data } = chunkData;

    const transfer = incomingFiles[fileId];
    if (!transfer) {
        console.warn(`Received chunk for unknown or completed transfer ${fileId} from ${senderId}. Ignoring.`);
        return;
    }

    // --- FIX: Accept ArrayBuffer OR Uint8Array ---
    // PeerJS sometimes delivers binary data as Uint8Array even if ArrayBuffer was sent.
    // Blob constructor accepts either.
    if (typeof chunkIndex !== 'number' || !(data instanceof ArrayBuffer || data instanceof Uint8Array)) {
        console.warn(`Received invalid chunk data type from ${senderId} for ${fileId}. Expected ArrayBuffer or Uint8Array. Got:`, data);
        return; // Skip invalid chunk
    }
    // --- End FIX ---


    if (transfer.chunks[chunkIndex]) {
        console.log(`Received duplicate chunk ${chunkIndex} for ${fileId}. Ignoring.`);
        return; // Already have this chunk
    }

    // Store the received chunk data (ArrayBuffer or Uint8Array)
    transfer.chunks[chunkIndex] = data;
    transfer.receivedChunks++;

    const progress = transfer.receivedChunks / transfer.meta.totalChunks;
    updateFileProgress(fileId, progress, `Receiving... (${transfer.receivedChunks}/${transfer.meta.totalChunks})`);

    // Check if all chunks are received
    if (transfer.receivedChunks === transfer.meta.totalChunks) {
        console.log(`Received all chunks for ${transfer.meta.fileName} (ID: ${fileId})`);
        // Verify chunk array integrity (ensure no gaps) - important if using sparse array
        let allChunksPresent = true;
        for(let i = 0; i < transfer.meta.totalChunks; i++) {
            if (!transfer.chunks[i]) {
                allChunksPresent = false;
                console.error(`Missing chunk ${i} for ${fileId} despite received count matching total.`);
                updateFileProgress(fileId, progress, `Error: Missing chunk ${i}`);
                // Consider requesting missing chunks or failing the transfer
                break;
            }
        }

        if (allChunksPresent) {
            updateFileProgress(fileId, 1, 'Assembling file...');
            setTimeout(() => assembleFile(fileId), 0); // Assemble async
        } else {
             // Handle missing chunk scenario (e.g., mark as failed)
             updateFileProgress(fileId, progress, 'Assembly failed (missing data)');
             // Clean up? delete incomingFiles[fileId];
        }
    }
}


// assembleFile (No changes needed - Blob constructor handles ArrayBuffer/Uint8Array)
function assembleFile(fileId) {
    const transfer = incomingFiles[fileId];
    // Double check conditions for assembly
    if (!transfer || transfer.receivedChunks !== transfer.meta.totalChunks || transfer.chunks.length !== transfer.meta.totalChunks) {
        console.error(`Cannot assemble file ${fileId}: Transfer data incomplete, missing, or count mismatch.`);
        const progress = transfer ? (transfer.receivedChunks / transfer.meta.totalChunks) : 0;
        updateFileProgress(fileId, progress, 'Assembly failed: Incomplete data');
        // Maybe clean up `incomingFiles[fileId]` here
        return;
    }

    console.log(`Assembling file ${transfer.meta.fileName} (ID: ${fileId})`);

    try {
        // Blob constructor takes an array of BlobParts (ArrayBuffer, TypedArray, Blob, String)
        const fileBlob = new Blob(transfer.chunks, { type: transfer.meta.fileType });

        if (fileBlob.size !== transfer.meta.fileSize) {
             console.warn(`Assembled file size (${fileBlob.size}) does not match metadata (${transfer.meta.fileSize}) for ${fileId}.`);
        }

        const objectURL = URL.createObjectURL(fileBlob);

        transfer.downloadLink.href = objectURL;
        transfer.downloadLink.download = transfer.meta.fileName;
        transfer.downloadLink.removeAttribute('disabled');
        transfer.downloadLink.textContent = `Download ${transfer.meta.fileName}`;
        updateFileProgress(fileId, 1, 'Ready to download');

        console.log(`File ${transfer.meta.fileName} (ID: ${fileId}) assembled and ready for download.`);

        // Clean up the stored chunks array to free memory
        transfer.chunks = []; // Release chunk data

        // Add listener to revoke URL after download click
        transfer.downloadLink.addEventListener('click', () => {
             setTimeout(() => {
                 if (transfer.downloadLink.href.startsWith('blob:')) {
                    URL.revokeObjectURL(transfer.downloadLink.href);
                    console.log(`Revoked object URL for ${fileId}`);
                 }
             }, 100);
        }, { once: true });

    } catch (error) {
        console.error(`Error assembling file ${fileId}:`, error);
        updateFileProgress(fileId, transfer.receivedChunks / transfer.meta.totalChunks, `Assembly failed: ${error.message}`);
        if (transfer.downloadLink) {
            transfer.downloadLink.textContent = 'Download Failed';
            transfer.downloadLink.removeAttribute('href');
            transfer.downloadLink.setAttribute('disabled', 'true');
        }
        // delete incomingFiles[fileId]; // Clean up failed transfer
    }
}


// displayFileTransfer (No changes needed)
function displayFileTransfer(senderId, metadata, isSent) {
    const { fileId, fileName, fileSize, totalChunks } = metadata;
    const messageElement = document.createElement('div');
    messageElement.classList.add('message', 'file-transfer');
    messageElement.classList.add(isSent ? 'sent' : 'received');
    messageElement.dataset.fileId = fileId; // Store fileId for later reference

    const senderElement = document.createElement('span');
    senderElement.classList.add('sender');
    senderElement.textContent = isSent ? 'You' : shortId(senderId);
    messageElement.appendChild(senderElement);

    const fileInfo = document.createElement('div');
    fileInfo.classList.add('file-info');
    fileInfo.textContent = `${fileName} (${(fileSize / 1024 / 1024).toFixed(2)} MB)`;
    messageElement.appendChild(fileInfo);

    const progressContainer = document.createElement('div');
    progressContainer.classList.add('file-progress-container');
    const progressElement = document.createElement('div');
    progressElement.classList.add('file-progress-bar');
    progressContainer.appendChild(progressElement);
    messageElement.appendChild(progressContainer);

    const statusElement = document.createElement('div');
    statusElement.classList.add('file-status');
    statusElement.textContent = 'Initializing...';
    messageElement.appendChild(statusElement);

    let downloadLink = null;
    if (!isSent) {
        downloadLink = document.createElement('a');
        downloadLink.classList.add('download-link');
        downloadLink.textContent = 'Preparing download...';
        downloadLink.setAttribute('disabled', 'true'); // Disabled until transfer is complete
        messageElement.appendChild(downloadLink);
    }

    messagesDiv.appendChild(messageElement);
    scrollToBottom();

    return { messageElement, progressElement, statusElement, downloadLink };
}

// updateFileProgress (No changes needed)
function updateFileProgress(fileId, progress, statusText) {
    const transfer = outgoingFiles[fileId] || incomingFiles[fileId];
    if (!transfer) return;

    if (transfer.progressElement) {
        // Ensure progress doesn't exceed 100% visually
        transfer.progressElement.style.width = `${Math.min(100, Math.round(progress * 100))}%`;
    }
    if (transfer.statusElement) {
        transfer.statusElement.textContent = statusText;
    }
}

// abortTransfersForPeer (No changes needed)
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
                 delete outgoingFiles[fileId];
                 console.log(`Outgoing transfer ${fileId} aborted as last peer disconnected.`);
            }
        }
    }

    // Abort incoming transfers FROM this peer
    for (const fileId in incomingFiles) {
        const transfer = incomingFiles[fileId];
        if (transfer.senderId === peerId && transfer.receivedChunks < transfer.meta.totalChunks) {
             console.log(`Aborting incoming transfer ${fileId} from disconnected peer ${peerId}`);
             updateFileProgress(fileId, transfer.receivedChunks / transfer.meta.totalChunks, 'Transfer failed (sender disconnected)');
             if (transfer.downloadLink) {
                 transfer.downloadLink.textContent = 'Download Failed';
                 transfer.downloadLink.setAttribute('disabled', 'true');
                 transfer.downloadLink.removeAttribute('href');
                 if (transfer.downloadLink.href.startsWith('blob:')) {
                     URL.revokeObjectURL(transfer.downloadLink.href);
                 }
             }
             delete incomingFiles[fileId];
        }
    }
}

// broadcastData (No changes needed)
function broadcastData(data) {
     console.log(`Broadcasting data to ${Object.keys(connections).length} peers:`, data.type);
     let sentToAny = false;
     for (const peerId in connections) {
        // Ensure connection object exists and is open
        if (connections[peerId] && connections[peerId].open && connections[peerId].send) {
            try {
                connections[peerId].send(data);
                sentToAny = true;
            } catch (error) {
                console.error(`Error broadcasting data to ${peerId}:`, error);
                // Assume connection is dead on broadcast error
                handlePeerLeft(peerId);
            }
        } else {
             console.warn(`Connection to ${peerId} not open or doesn't exist, skipping broadcast.`);
        }
     }
     return sentToAny;
}

// --- Event Listeners ---

// MODIFIED: Use handleSendAction for Send button and Enter key
sendButton.addEventListener('click', handleSendAction);
messageInput.addEventListener('keypress', (event) => {
    if (event.key === 'Enter' && !event.shiftKey) {
        event.preventDefault();
        handleSendAction(); // Trigger combined send action
    }
});

// Trigger hidden file input when upload button is clicked
uploadButton.addEventListener('click', () => {
    // Only allow attaching if not already sending too many files? (Validation is in handleFileSelect)
    fileInput.click();
});

// Handle file selection -> Stages files
fileInput.addEventListener('change', handleFileSelect);

// Handle visibility changes (No changes needed)
document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible') {
        if (peer && peer.disconnected && !peer.destroyed) {
            console.log('Tab became visible, attempting PeerJS reconnect.');
            peer.reconnect();
        }
        if (socket && !socket.connected) {
            console.log('Tab became visible, attempting Socket.IO reconnect.');
            socket.connect();
        }
    }
});

// Cleanup object URLs on page unload (No changes needed)
window.addEventListener('beforeunload', () => {
    for (const fileId in incomingFiles) {
        const transfer = incomingFiles[fileId];
        if (transfer.downloadLink && transfer.downloadLink.href.startsWith('blob:')) {
            URL.revokeObjectURL(transfer.downloadLink.href);
            console.log(`Revoked object URL for ${fileId} on page unload`);
        }
    }
    if (peer && !peer.destroyed) {
        peer.destroy();
    }
     if (socket && socket.connected) {
         socket.disconnect();
     }
});


// --- Start the application ---
initializePeer();
updateStagedFilesUI(); // Initialize the staged files display (hidden initially)