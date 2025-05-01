const messagesDiv = document.getElementById('messages');
const messageInput = document.getElementById('message-input');
const sendButton = document.getElementById('send-button');
const statusDiv = document.getElementById('status');
const uploadButton = document.getElementById('upload-button');
const fileInput = document.getElementById('file-input');
const stagedFilesDisplay = document.getElementById('staged-files-display');

const API_SERVER_URL = 'https://api.totob12.com';
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
const incomingFiles = {}; // { fileId: { meta, chunks: [], progressElement, statusElement, messageElement, previewContainerElement, senderId, receivedChunks, objectURL: null } }

// Staging area for files before sending
let stagedFiles = []; // Array to hold File objects
const stagedObjectURLs = new Map(); // Map to store Object URLs for staged file previews { fileObject: url }

// Configure DOMPurify
DOMPurify.setConfig({ ADD_ATTR: ['target'] });

// --- Helper Functions ---

function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
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

// --- Initialization --- (No changes)
function initializePeer() {
    peer = new Peer(undefined, PEERJS_CONFIG);
    peer.on('open', (id) => { myPeerId = id; console.log('My PeerJS ID:', myPeerId); updateStatus(`Connected as ${shortId(myPeerId)}. Waiting...`); initializeSignaling(); });
    peer.on('connection', setupConnection);
    peer.on('disconnected', () => { updateStatus('PeerJS lost. Reconnecting...'); console.error('PeerJS disconnected.'); });
    peer.on('close', () => { updateStatus('PeerJS closed.'); console.warn('PeerJS closed.'); });
    peer.on('error', (err) => { updateStatus(`PeerJS Error: ${err.type}`); console.error('PeerJS Error:', err); handlePeerError(err); });
}

function initializeSignaling() {
    socket = io(`${API_SERVER_URL}/msg`, { path: '/socket.io' });
    socket.on('connect', () => { console.log('Connected to signaling server'); updateStatus(`Connected as ${shortId(myPeerId)}. Joining...`); socket.emit('join-room', myPeerId); });
    socket.on('connect_error', (err) => { console.error('Signaling error:', err); updateStatus(`Signaling Error: ${err.message}`); });
    socket.on('disconnect', (reason) => { console.warn('Signaling disconnected:', reason); updateStatus('Signaling disconnected.'); });
    socket.on('existing-users', (peerIds) => { updateStatus(`Connected as ${shortId(myPeerId)}. ${peerIds.length} other(s) online.`); peerIds.forEach(id => { if (id !== myPeerId && !connections[id]) connectToPeer(id); }); });
    socket.on('user-joined', (peerId) => { if (peerId !== myPeerId && !connections[peerId]) { displaySystemMessage(`${shortId(peerId)} joined.`); connectToPeer(peerId); } });
    socket.on('user-left', handlePeerLeft);
}

// --- Peer Connection Handling --- (No changes)
function connectToPeer(peerId) {
    if (connections[peerId]?._temp || connections[peerId]?.open) return;
    console.log(`Connecting to ${peerId}`);
    const conn = peer.connect(peerId, { reliable: true, label: myPeerId });
    connections[peerId] = { open: false, _temp: true };
    setupConnection(conn);
}

function setupConnection(conn) {
    conn.on('open', () => { console.log(`Connection open with ${conn.peer}`); connections[conn.peer] = conn; conn.label = conn.label || conn.peer; updateStatus(`${Object.values(connections).filter(c => c?.open).length} user(s) online.`); });
    conn.on('data', (data) => { console.log(`Data from ${conn.peer}:`, data?.type); if (typeof data === 'object' && data !== null) { switch (data.type) { case 'chat': handleChatMessage(conn, data); break; case 'file-meta': handleFileMetadata(conn.peer, data); break; case 'file-chunk': handleFileChunk(conn.peer, data); break; default: console.warn(`Unknown data type from ${conn.peer}:`, data.type); } } else { console.warn(`Non-object data from ${conn.peer}:`, data); } });
    conn.on('close', () => { console.log(`Connection closed with ${conn.peer}`); handlePeerLeft(conn.peer); });
    conn.on('error', (err) => { console.error(`Connection error with ${conn.peer}:`, err); if (connections[conn.peer]) { displaySystemMessage(`Error with ${shortId(connections[conn.peer].label || conn.peer)}: ${err.type}`); delete connections[conn.peer]; updateStatus(`${Object.values(connections).filter(c => c?.open).length} user(s) online.`); abortTransfersForPeer(conn.peer); } });
}

function handleChatMessage(conn, data) { if (typeof data.message === 'string') { displayMessage(data.sender || conn.label || conn.peer, data.message, false); } else { console.warn(`Malformed chat from ${conn.peer}:`, data); } }
function handlePeerLeft(peerId) { if (connections[peerId]) { const name = shortId(connections[peerId].label || peerId); displaySystemMessage(`${name} left/disconnected.`); if (connections[peerId].close) connections[peerId].close(); delete connections[peerId]; updateStatus(`${Object.values(connections).filter(c => c?.open).length} user(s) online.`); abortTransfersForPeer(peerId); } else { displaySystemMessage(`${shortId(peerId)} left.`); updateStatus(`${Object.values(connections).filter(c => c?.open).length} user(s) online.`); } }
function handlePeerError(err) { if (err.type === 'unavailable-id') { peer = null; setTimeout(initializePeer, 3000); } else if (['network', 'server-error', 'socket-error', 'socket-closed'].includes(err.type)) { updateStatus('Connection issues.'); } else if (err.type !== 'disconnected') { updateStatus(`PeerJS Error: ${err.type}.`); } }

// --- UI and Messaging --- (No changes to displayMessage, displaySystemMessage, sendTextMessage)
function handleSendAction() {
    const messageText = messageInput.value.trim();
    const filesToSend = [...stagedFiles];
    if (!messageText && filesToSend.length === 0) return;
    const openConnections = Object.values(connections).filter(conn => conn?.open).length;
    const noReceivers = openConnections === 0;
    if (messageText) sendTextMessage(messageText, noReceivers);
    if (filesToSend.length > 0) { if (noReceivers) { displaySystemMessage(`Cannot send ${filesToSend.length} file(s): no peers.`); clearStagedFiles(); } else { filesToSend.forEach(sendFile); clearStagedFiles(); } }
    messageInput.value = '';
}
function sendTextMessage(messageText, receivedByNone = false) { if (!messageText || !peer || !myPeerId) return; displayMessage(myPeerId, messageText, true, receivedByNone); if (!receivedByNone) broadcastData({ type: 'chat', sender: myPeerId, message: messageText }); }
function displayMessage(senderId, message, isSent, receivedByNone = false) { const el = document.createElement('div'); el.classList.add('message', isSent ? 'sent' : 'received'); const senderEl = document.createElement('span'); senderEl.classList.add('sender'); senderEl.textContent = isSent ? 'You' : shortId(senderId); el.appendChild(senderEl); const rawHtml = marked.parse(message, { gfm: true, breaks: true }); const sanitizedHtml = DOMPurify.sanitize(rawHtml, { USE_PROFILES: { html: true }, ADD_ATTR: ['target'] }); const contentEl = document.createElement('div'); contentEl.classList.add('message-content'); contentEl.innerHTML = sanitizedHtml; contentEl.querySelectorAll('a').forEach(link => { if ((link.protocol === 'http:' || link.protocol === 'https:') && link.hostname !== window.location.hostname) { link.target = '_blank'; link.rel = 'noopener noreferrer'; } }); el.appendChild(contentEl); if (isSent && receivedByNone) { const warnEl = document.createElement('span'); warnEl.classList.add('warning'); warnEl.textContent = ' (No one connected)'; el.appendChild(warnEl); } messagesDiv.appendChild(el); scrollToBottom(); }
function displaySystemMessage(message) { const el = document.createElement('div'); el.classList.add('message', 'system'); el.textContent = message; messagesDiv.appendChild(el); scrollToBottom(); }
function updateStatus(text) { statusDiv.textContent = text; }


// --- File Staging Logic --- (No changes)
function handleFileSelect(event) { const files = event.target.files; if (!files || files.length === 0) return; const currentCount = stagedFiles.length; if (currentCount + files.length > MAX_FILES) { displaySystemMessage(`Error: Max ${MAX_FILES} files.`); fileInput.value = null; return; } let filesToStage = []; for (const file of files) { if (file.size > MAX_FILE_SIZE_BYTES) { displaySystemMessage(`Error: "${file.name}" (${formatFileSize(file.size)}) > ${MAX_FILE_SIZE_MB} MB.`); continue; } if (file.size === 0) { displaySystemMessage(`Warning: Skipping empty "${file.name}".`); continue; } if (stagedFiles.some(sf => sf.name === file.name && sf.size === file.size && sf.lastModified === file.lastModified)) { displaySystemMessage(`Warning: "${file.name}" already attached.`); continue; } filesToStage.push(file); } if (filesToStage.length > 0) { stagedFiles.push(...filesToStage); updateStagedFilesUI(); } fileInput.value = null; }
function updateStagedFilesUI() { stagedFilesDisplay.innerHTML = ''; if (stagedFiles.length === 0) { stagedFilesDisplay.style.display = 'none'; return; } stagedFilesDisplay.style.display = 'flex'; stagedFiles.forEach((file, index) => { const item = document.createElement('div'); item.classList.add('staged-file-item'); item.title = `${file.name} (${formatFileSize(file.size)})`; const previewCont = document.createElement('div'); previewCont.classList.add('staged-preview'); let previewUrl = stagedObjectURLs.get(file); const isImage = file.type.startsWith('image/'); const isVideo = file.type.startsWith('video/'); if ((isImage || isVideo) && !previewUrl) { previewUrl = URL.createObjectURL(file); stagedObjectURLs.set(file, previewUrl); } if (isImage) { const img = document.createElement('img'); img.src = previewUrl; img.alt = 'Preview'; previewCont.appendChild(img); } else if (isVideo) { const vid = document.createElement('video'); vid.src = previewUrl; vid.muted = true; vid.preload = 'metadata'; previewCont.appendChild(vid); } else { const icon = document.createElement('span'); icon.classList.add('file-icon'); icon.textContent = '📄'; previewCont.appendChild(icon); } item.appendChild(previewCont); const infoSpan = document.createElement('div'); infoSpan.classList.add('staged-file-info'); infoSpan.textContent = file.name; item.appendChild(infoSpan); const removeBtn = document.createElement('button'); removeBtn.innerHTML = '×'; removeBtn.title = 'Remove'; removeBtn.classList.add('remove-staged'); removeBtn.dataset.index = index; removeBtn.onclick = (e) => { const idx = parseInt(e.target.dataset.index, 10); if (!isNaN(idx) && idx < stagedFiles.length) removeStagedFile(stagedFiles[idx]); }; item.appendChild(removeBtn); stagedFilesDisplay.appendChild(item); }); }
function removeStagedFile(fileToRemove) { const index = stagedFiles.findIndex(f => f === fileToRemove); if (index > -1) { stagedFiles.splice(index, 1); const url = stagedObjectURLs.get(fileToRemove); if (url) { URL.revokeObjectURL(url); stagedObjectURLs.delete(fileToRemove); } updateStagedFilesUI(); } }
function clearStagedFiles() { stagedFiles.forEach(file => { const url = stagedObjectURLs.get(file); if (url) URL.revokeObjectURL(url); }); stagedFiles = []; stagedObjectURLs.clear(); updateStagedFilesUI(); }

// --- File Transfer Logic ---

function sendFile(file) {
    const fileId = uuid.v4();
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
    const metadata = { type: 'file-meta', sender: myPeerId, fileId, fileName: file.name, fileSize: file.size, fileType: file.type || 'application/octet-stream', totalChunks };

    const { messageElement, progressElement, statusElement, previewContainerElement } = displayFileTransfer(myPeerId, metadata, true);

    let previewObjectURL = null;
    if (metadata.fileType.startsWith('image/') || metadata.fileType.startsWith('video/')) {
        previewObjectURL = URL.createObjectURL(file);
        const previewEl = metadata.fileType.startsWith('image/') ? document.createElement('img') : document.createElement('video');
        previewEl.src = previewObjectURL;
        if (metadata.fileType.startsWith('video/')) { previewEl.muted = true; previewEl.preload = 'metadata'; }
        previewContainerElement.innerHTML = '';
        previewContainerElement.appendChild(previewEl);
    }

    const peersToSendTo = Object.keys(connections).filter(id => connections[id]?.open);
    outgoingFiles[fileId] = { meta: metadata, file, progressElement, statusElement, messageElement, previewContainerElement, previewObjectURL, sentChunks: 0, peers: peersToSendTo };

    if (peersToSendTo.length === 0) {
        updateFileProgress(fileId, 0, 'Send failed (no connections)');
        messageElement.classList.add('transfer-complete');
        return;
    }

    updateFileProgress(fileId, 0, 'Starting transfer...');
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
        if (!outgoingFiles[fileId]) return;
        const chunkData = event.target.result;
        const chunkPayload = { type: 'file-chunk', fileId, chunkIndex, data: chunkData };
        const currentPeers = [...transfer.peers]; // Iterate copy
        currentPeers.forEach(peerId => {
            if (connections[peerId]?.open) {
                try { connections[peerId].send(chunkPayload); }
                catch (error) { console.error(`Error sending chunk ${chunkIndex} to ${peerId}:`, error); }
            } else if (transfer.peers.includes(peerId)) {
                console.warn(`Removing non-open peer ${peerId} from transfer ${fileId}`);
                transfer.peers.splice(transfer.peers.indexOf(peerId), 1);
            }
        });

        if (transfer.peers.length === 0) {
            console.error(`No remaining peers for transfer ${fileId}. Aborting.`);
            updateFileProgress(fileId, transfer.sentChunks / meta.totalChunks, 'Transfer failed (peers disconnected)');
            transfer.messageElement.classList.add('transfer-complete');
            return;
        }

        transfer.sentChunks++;
        const progress = transfer.sentChunks / meta.totalChunks;
        updateFileProgress(fileId, progress, `Sending... (${transfer.sentChunks}/${meta.totalChunks})`);

        if (transfer.sentChunks === meta.totalChunks) {
            updateFileProgress(fileId, 1, 'Sent');
            transfer.messageElement.classList.add('transfer-complete');
        } else {
            setTimeout(() => sendChunk(fileId, chunkIndex + 1), 0);
        }
    };
    reader.onerror = () => { console.error(`Error reading chunk ${chunkIndex} for ${fileId}`); updateFileProgress(fileId, transfer.sentChunks / meta.totalChunks, 'Error reading file chunk.'); transfer.messageElement.classList.add('transfer-complete'); };
    reader.readAsArrayBuffer(chunk);
}

function handleFileMetadata(senderId, metadata) {
    const { fileId, fileName, fileSize, fileType, totalChunks } = metadata;
    if (!fileId || !fileName || typeof fileSize !== 'number' || typeof totalChunks !== 'number') { console.warn(`Invalid meta from ${senderId}:`, metadata); return; }
    if (incomingFiles[fileId] || outgoingFiles[fileId]) { console.warn(`Meta for existing transfer ID ${fileId}. Ignoring.`); return; }

    console.log(`Received meta for ${fileName} (${formatFileSize(fileSize)}) from ${senderId}`);
    // Note: displayFileTransfer now returns downloadLink = null for received files
    const { messageElement, progressElement, statusElement, previewContainerElement } = displayFileTransfer(senderId, metadata, false);

    incomingFiles[fileId] = {
        meta: metadata, chunks: [], receivedChunks: 0,
        progressElement, statusElement, messageElement, previewContainerElement, senderId,
        objectURL: null // Initialize objectURL holder
    };
    updateFileProgress(fileId, 0, 'Waiting for data...');
}

function handleFileChunk(senderId, chunkData) {
    const { fileId, chunkIndex, data } = chunkData;
    const transfer = incomingFiles[fileId];
    if (!transfer) return;
    if (typeof chunkIndex !== 'number' || !(data instanceof ArrayBuffer || data instanceof Uint8Array)) { console.warn(`Invalid chunk data type from ${senderId} for ${fileId}.`); return; }
    if (transfer.chunks[chunkIndex]) return; // Duplicate

    transfer.chunks[chunkIndex] = data;
    transfer.receivedChunks++;
    const progress = transfer.receivedChunks / transfer.meta.totalChunks;
    updateFileProgress(fileId, progress, `Receiving... (${transfer.receivedChunks}/${transfer.meta.totalChunks})`);

    if (transfer.receivedChunks === transfer.meta.totalChunks) {
        console.log(`Received all chunks for ${transfer.meta.fileName} (ID: ${fileId})`);
        let allPresent = true;
        for(let i = 0; i < transfer.meta.totalChunks; i++) { if (!transfer.chunks[i]) { allPresent = false; console.error(`Missing chunk ${i} for ${fileId}.`); updateFileProgress(fileId, progress, `Error: Missing chunk ${i}`); transfer.messageElement.classList.add('transfer-complete'); break; } }
        if (allPresent) {
            updateFileProgress(fileId, 1, 'Assembling file...');
            setTimeout(() => assembleFile(fileId), 0);
        }
    }
}

function assembleFile(fileId) {
    const transfer = incomingFiles[fileId];
    if (!transfer || transfer.receivedChunks !== transfer.meta.totalChunks || transfer.chunks.length !== transfer.meta.totalChunks) {
        console.error(`Cannot assemble ${fileId}: Incomplete data.`);
        const progress = transfer ? (transfer.receivedChunks / transfer.meta.totalChunks) : 0;
        updateFileProgress(fileId, progress, 'Assembly failed: Incomplete data');
        if(transfer?.messageElement) transfer.messageElement.classList.add('transfer-complete');
        return;
    }
    console.log(`Assembling ${transfer.meta.fileName} (ID: ${fileId})`);
    try {
        const fileBlob = new Blob(transfer.chunks, { type: transfer.meta.fileType });
        if (fileBlob.size !== transfer.meta.fileSize) console.warn(`Size mismatch for ${fileId}: ${fileBlob.size} vs ${transfer.meta.fileSize}`);

        // Store the Object URL on the transfer object
        transfer.objectURL = URL.createObjectURL(fileBlob);

        // Update status to be clickable
        transfer.statusElement.textContent = 'Download';
        transfer.statusElement.classList.add('clickable-download');
        transfer.statusElement.onclick = () => {
            // Create temporary link and click it
            const tempLink = document.createElement('a');
            tempLink.href = transfer.objectURL;
            tempLink.download = transfer.meta.fileName;
            document.body.appendChild(tempLink); // Required for Firefox
            tempLink.click();
            document.body.removeChild(tempLink);
            console.log(`Download triggered for ${fileId}`);
            // Note: We don't revoke the URL here, rely on beforeunload
        };

        // Update overall message state
        transfer.messageElement.classList.add('transfer-complete'); // Hide progress bar

        // Create preview if applicable
        if (transfer.meta.fileType.startsWith('image/') || transfer.meta.fileType.startsWith('video/')) {
            const previewEl = transfer.meta.fileType.startsWith('image/') ? document.createElement('img') : document.createElement('video');
            previewEl.src = transfer.objectURL; // Use the same URL
             if (transfer.meta.fileType.startsWith('video/')) { previewEl.muted = true; previewEl.preload = 'metadata'; }
            transfer.previewContainerElement.innerHTML = '';
            transfer.previewContainerElement.appendChild(previewEl);
        }
        console.log(`File ${transfer.meta.fileName} (ID: ${fileId}) ready.`);
        transfer.chunks = []; // Clean up memory

    } catch (error) {
        console.error(`Error assembling ${fileId}:`, error);
        updateFileProgress(fileId, transfer.receivedChunks / transfer.meta.totalChunks, `Assembly failed: ${error.message}`);
        transfer.messageElement.classList.add('transfer-complete');
        transfer.statusElement.textContent = 'Assembly Failed'; // Update status text
        transfer.statusElement.classList.remove('clickable-download'); // Ensure not clickable
        transfer.statusElement.onclick = null; // Remove handler
        // delete incomingFiles[fileId]; // Keep entry
    }
}

// MODIFIED: displayFileTransfer no longer creates downloadLink for received files
function displayFileTransfer(senderId, metadata, isSent) {
    const { fileId, fileName, fileSize } = metadata;
    const messageElement = document.createElement('div');
    messageElement.classList.add('message', 'file-transfer', isSent ? 'sent' : 'received');
    messageElement.dataset.fileId = fileId;

    const senderElement = document.createElement('span');
    senderElement.classList.add('sender');
    senderElement.textContent = isSent ? 'You' : shortId(senderId);
    messageElement.appendChild(senderElement);

    const contentContainer = document.createElement('div');
    contentContainer.classList.add('file-transfer-content');

    const previewContainerElement = document.createElement('div');
    previewContainerElement.classList.add('file-preview');
    const icon = document.createElement('span');
    icon.classList.add('file-icon');
    icon.textContent = '📄';
    previewContainerElement.appendChild(icon);
    contentContainer.appendChild(previewContainerElement);

    const detailsContainer = document.createElement('div');
    detailsContainer.classList.add('file-details');

    const fileInfo = document.createElement('div');
    fileInfo.classList.add('file-info');
    fileInfo.textContent = fileName;
    fileInfo.title = fileName;
    detailsContainer.appendChild(fileInfo);

    const fileSizeInfo = document.createElement('div');
    fileSizeInfo.classList.add('file-size');
    fileSizeInfo.textContent = formatFileSize(fileSize);
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

    // REMOVED: No downloadLink element created here for received files
    // let downloadLink = null;
    // if (!isSent) { ... }

    contentContainer.appendChild(detailsContainer);
    messageElement.appendChild(contentContainer);
    messagesDiv.appendChild(messageElement);
    scrollToBottom();

    // Return downloadLink as null when isSent is false
    return { messageElement, progressElement, statusElement, previewContainerElement, downloadLink: null };
}


// updateFileProgress (No changes needed)
function updateFileProgress(fileId, progress, statusText) {
    const transfer = outgoingFiles[fileId] || incomingFiles[fileId];
    if (!transfer) return;
    if (transfer.progressElement) transfer.progressElement.style.width = `${Math.min(100, Math.round(progress * 100))}%`;
    // Only update status text if it's not already clickable (avoid overwriting "Download")
    if (transfer.statusElement && !transfer.statusElement.classList.contains('clickable-download')) {
        transfer.statusElement.textContent = statusText;
    }
}

// abortTransfersForPeer (Updated incoming check)
function abortTransfersForPeer(peerId) {
    console.log(`Aborting transfers involving ${peerId}`);
    // Outgoing
    for (const fileId in outgoingFiles) {
        const transfer = outgoingFiles[fileId];
        const peerIndex = transfer.peers.indexOf(peerId);
        if (peerIndex > -1) {
            transfer.peers.splice(peerIndex, 1);
            if (transfer.peers.length === 0 && !transfer.messageElement.classList.contains('transfer-complete')) {
                 updateFileProgress(fileId, transfer.sentChunks / transfer.meta.totalChunks, 'Transfer failed (peer disconnected)');
                 if(transfer.messageElement) transfer.messageElement.classList.add('transfer-complete');
                 console.log(`Outgoing ${fileId} aborted.`);
            }
        }
    }
    // Incoming
    for (const fileId in incomingFiles) {
        const transfer = incomingFiles[fileId];
        // Check if not completed and sender matches
        if (transfer.senderId === peerId && !transfer.messageElement.classList.contains('transfer-complete')) {
             console.log(`Aborting incoming ${fileId} from ${peerId}`);
             updateFileProgress(fileId, transfer.receivedChunks / transfer.meta.totalChunks, 'Transfer failed (sender disconnected)');
             if(transfer.messageElement) transfer.messageElement.classList.add('transfer-complete');
             if (transfer.statusElement) {
                 transfer.statusElement.textContent = 'Download Failed'; // Update status
                 transfer.statusElement.classList.remove('clickable-download'); // Ensure not clickable
                 transfer.statusElement.onclick = null; // Remove handler
             }
             if (transfer.objectURL) { // Revoke URL if assembly started but failed mid-way due to disconnect
                 URL.revokeObjectURL(transfer.objectURL);
                 transfer.objectURL = null;
             }
        }
    }
}

// broadcastData (No changes)
function broadcastData(data) { console.log(`Broadcasting ${data.type} to ${Object.values(connections).filter(c=>c?.open).length} peers`); let sent = false; for (const peerId in connections) { if (connections[peerId]?.open && connections[peerId].send) { try { connections[peerId].send(data); sent = true; } catch (error) { console.error(`Broadcast error to ${peerId}:`, error); handlePeerLeft(peerId); } } } if (!sent && Object.keys(connections).length > 0) console.warn("Broadcast attempted but no connections open."); return sent; }


// --- Event Listeners --- (No changes)
sendButton.addEventListener('click', handleSendAction);
messageInput.addEventListener('keypress', (e) => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); handleSendAction(); } });
uploadButton.addEventListener('click', () => fileInput.click());
fileInput.addEventListener('change', handleFileSelect);
document.addEventListener('visibilitychange', () => { if (document.visibilityState === 'visible') { if (peer?.disconnected && !peer.destroyed) peer.reconnect(); if (socket && !socket.connected) socket.connect(); } });

// Cleanup object URLs on page unload (Added incoming file objectURL cleanup)
window.addEventListener('beforeunload', () => {
    stagedObjectURLs.forEach(url => URL.revokeObjectURL(url));
    stagedObjectURLs.clear();
    console.log("Revoked staged URLs");

    for (const fileId in incomingFiles) {
        const transfer = incomingFiles[fileId];
        // Revoke the main object URL if it exists
        if (transfer.objectURL) {
            URL.revokeObjectURL(transfer.objectURL);
            console.log(`Revoked incoming object URL for ${fileId}`);
        }
    }
    for (const fileId in outgoingFiles) {
        const transfer = outgoingFiles[fileId];
        if (transfer.previewObjectURL) {
             URL.revokeObjectURL(transfer.previewObjectURL);
             console.log(`Revoked outgoing preview URL for ${fileId}`);
        }
    }

    if (peer && !peer.destroyed) peer.destroy();
    if (socket?.connected) socket.disconnect();
});

// --- Start the application ---
initializePeer();
updateStagedFilesUI();