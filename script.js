const messagesDiv = document.getElementById('messages');
const messageInput = document.getElementById('message-input');
const sendButton = document.getElementById('send-button');
const statusDiv = document.getElementById('status');

// const API_SERVER_URL = 'https://api.totob12.com'; // Your API server URL
const API_SERVER_URL = 'http://localhost:3000'; // Local server for testing
const PEERJS_CONFIG = {
    // Use the default PeerJS cloud server for signaling negotiation
    // For extreme reliability/control, you might host your own PeerServer,
    // but the cloud one is easiest to start.
    // host: 'your-peerjs-server.com', // Optional: if you host your own PeerServer
    // port: 9000,                    // Optional: if you host your own PeerServer
    // path: '/myapp'                 // Optional: if you host your own PeerServer
};

let peer = null;
let myPeerId = null;
const connections = {}; // Store connections { peerId: DataConnection }
let socket = null;

// Configure DOMPurify (optional, defaults are usually good)
// Allow target="_blank" for links, commonly used in Markdown
DOMPurify.setConfig({ ADD_ATTR: ['target'] });

// --- Initialization ---

function initializePeer() {
    peer = new Peer(undefined, PEERJS_CONFIG); // Let PeerJS generate an ID

    peer.on('open', (id) => {
        myPeerId = id;
        console.log('My PeerJS ID is:', myPeerId);
        updateStatus(`Connected as ${shortId(myPeerId)}. Waiting for signaling server...`);
        initializeSignaling(); // Connect to signaling server AFTER getting Peer ID
    });

    peer.on('connection', (conn) => {
        console.log(`Incoming connection from ${conn.peer}`);
        setupConnection(conn);
    });

    peer.on('disconnected', () => {
        updateStatus('PeerJS connection lost. Attempting to reconnect...');
        console.error('PeerJS disconnected. Attempting reconnect.');
        // PeerJS will attempt to reconnect automatically.
    });

    peer.on('close', () => {
        updateStatus('PeerJS connection closed.');
        console.warn('PeerJS connection closed.');
        // Maybe attempt to re-initialize?
    });

    peer.on('error', (err) => {
        updateStatus(`PeerJS Error: ${err.type}`);
        console.error('PeerJS Error:', err);
        if (err.type === 'unavailable-id') {
            peer = null; // Force re-creation
            setTimeout(initializePeer, 3000);
        } else if (err.type === 'network' || err.type === 'server-error') {
            updateStatus('Connection issues. Check network or try refreshing.');
        }
    });
}

function initializeSignaling() {
    socket = io(`${API_SERVER_URL}/msg`, {
        path: '/socket.io',
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
        if (connections[peerId]) {
            displaySystemMessage(`${shortId(connections[peerId].label || peerId)} left the chat.`);
            connections[peerId].close();
            delete connections[peerId];
            updateStatus(`${Object.keys(connections).length} user(s) online.`);
        } else {
            displaySystemMessage(`${shortId(peerId)} left the chat.`);
            updateStatus(`${Object.keys(connections).length} user(s) online.`);
        }
    });
}

// --- Peer Connection Handling ---

function connectToPeer(peerId) {
    console.log(`Attempting to connect to ${peerId}`);
    const conn = peer.connect(peerId, {
        reliable: true,
        label: myPeerId
    });
    setupConnection(conn);
}

function setupConnection(conn) {
    conn.on('open', () => {
        console.log(`Connection established with ${conn.peer}`);
        connections[conn.peer] = conn;
        conn.label = conn.label || conn.peer; // Store the sender's ID if they provided it
        updateStatus(`${Object.keys(connections).length} user(s) online.`);
    });

    conn.on('data', (data) => {
        console.log(`Data received from ${conn.peer}:`, data);
        // Basic validation of received data structure
        if (typeof data === 'object' && data !== null && data.type === 'chat' && typeof data.message === 'string') {
            // Use conn.label (sender's PeerJS ID) if available, otherwise conn.peer
            const sender = data.sender || conn.label || conn.peer;
            displayMessage(sender, data.message, false);
        } else {
            console.warn(`Received malformed data from ${conn.peer}:`, data);
        }
    });

    conn.on('close', () => {
        console.log(`Connection closed with ${conn.peer}`);
        if (connections[conn.peer]) {
            displaySystemMessage(`${shortId(connections[conn.peer].label || conn.peer)} connection closed.`);
            delete connections[conn.peer];
            updateStatus(`${Object.keys(connections).length} user(s) online.`);
        }
    });

    conn.on('error', (err) => {
        console.error(`Connection error with ${conn.peer}:`, err);
        if (connections[conn.peer]) {
            displaySystemMessage(`Error with ${shortId(connections[conn.peer].label || conn.peer)} connection.`);
            delete connections[conn.peer];
            updateStatus(`${Object.keys(connections).length} user(s) online.`);
        }
    });
}

// --- UI and Messaging ---

function sendMessage() {
    const messageText = messageInput.value.trim();
    if (!messageText || peer === null || !myPeerId) {
        return;
    }

    const openConnections = Object.values(connections).filter(conn => conn.open).length;
    const receivedByNone = openConnections === 0;

    // Display message locally immediately
    displayMessage(myPeerId, messageText, true, receivedByNone);

    if (!receivedByNone) {
        const messagePayload = {
            type: 'chat',
            sender: myPeerId, // Include sender ID in the payload
            message: messageText
        };

        console.log(`Sending message to ${openConnections} peers.`);
        for (const peerId in connections) {
            if (connections[peerId] && connections[peerId].open) {
                try {
                    connections[peerId].send(messagePayload);
                } catch (error) {
                    console.error(`Error sending message to ${peerId}:`, error);
                    // Optionally handle the error, e.g., display a specific warning
                    displaySystemMessage(`Failed to send message to ${shortId(peerId)}.`);
                    // Consider closing the connection if sending consistently fails
                    // connections[peerId].close();
                    // delete connections[peerId];
                    // updateStatus(`${Object.keys(connections).length} user(s) online.`);
                }
            } else {
                console.warn(`Connection to ${peerId} not open or doesn't exist, skipping send.`);
            }
        }
    }

    messageInput.value = '';
}

function displayMessage(senderId, message, isSent, receivedByNone = false) {
    const messageElement = document.createElement('div');
    messageElement.classList.add('message');
    messageElement.classList.add(isSent ? 'sent' : 'received');

    const senderElement = document.createElement('span');
    senderElement.classList.add('sender');
    senderElement.textContent = isSent ? 'You' : shortId(senderId);
    messageElement.appendChild(senderElement);

    // --- MARKDOWN PROCESSING ---
    // 1. Parse the Markdown text to HTML using marked
    //    Disable deprecated options and enable GitHub Flavored Markdown (GFM)
    const rawHtml = marked.parse(message, { gfm: true, breaks: true });

    // 2. Sanitize the generated HTML using DOMPurify to prevent XSS
    const sanitizedHtml = DOMPurify.sanitize(rawHtml, {
        USE_PROFILES: { html: true }, // Ensure we are purifying HTML content
        ADD_ATTR: ['target'], // Allow target attribute (for target="_blank" on links)
        FORBID_TAGS: ['style'], // Explicitly forbid style tags
        FORBID_ATTR: ['style'] // Explicitly forbid style attributes
        });

    // 3. Add the sanitized HTML to the message element
    const contentElement = document.createElement('div');
    contentElement.classList.add('message-content');
    contentElement.innerHTML = sanitizedHtml;

    // Make external links open in a new tab
    contentElement.querySelectorAll('a').forEach(link => {
        // Check if the link is external (starts with http or https)
        if (link.href.startsWith('http://') || link.href.startsWith('https://')) {
             // Check if it's not linking to the current host (optional, good practice)
            if (link.hostname !== window.location.hostname) {
                link.target = '_blank';
                link.rel = 'noopener noreferrer'; // Security measure for target="_blank"
            }
        }
    });

    messageElement.appendChild(contentElement);
    // --- END MARKDOWN PROCESSING ---

    if (isSent && receivedByNone) {
        const warningElement = document.createElement('span');
        warningElement.classList.add('warning');
        warningElement.textContent = ' (not received by anyone)';
        messageElement.appendChild(warningElement);
    }

    messagesDiv.appendChild(messageElement);
    messagesDiv.scrollTop = messagesDiv.scrollHeight;
}


function displaySystemMessage(message) {
    const messageElement = document.createElement('div');
    messageElement.classList.add('message', 'system');
    messageElement.textContent = message;
    messagesDiv.appendChild(messageElement);
    messagesDiv.scrollTop = messagesDiv.scrollHeight;
}

function updateStatus(text) {
    statusDiv.textContent = text;
}

function shortId(id) {
    if (!id) return 'Anonymous';
    // Ensure id is a string before calling substring
    const idStr = String(id);
    return idStr.length > 8 ? `${idStr.substring(0, 4)}...${idStr.substring(idStr.length - 4)}` : idStr;
}

// --- Event Listeners ---

sendButton.addEventListener('click', sendMessage);
messageInput.addEventListener('keypress', (event) => {
    // Allow sending with Enter, but allow Shift+Enter for new lines
    if (event.key === 'Enter' && !event.shiftKey) {
        event.preventDefault(); // Prevent default newline behavior
        sendMessage();
    }
});

// Handle tab visibility changes to reconnect if necessary
document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible') {
        if (peer && peer.disconnected && !peer.destroyed) {
            console.log('Tab became visible, attempting PeerJS reconnect.');
            peer.reconnect();
        }
        // Also attempt to reconnect socket if it's disconnected
        if (socket && !socket.connected) {
            console.log('Tab became visible, attempting Socket.IO reconnect.');
            socket.connect();
        }
    }
});

// --- Start the application ---
initializePeer();