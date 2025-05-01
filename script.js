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
        conn.label = conn.label || conn.peer;
        updateStatus(`${Object.keys(connections).length} user(s) online.`);
    });

    conn.on('data', (data) => {
        console.log(`Data received from ${conn.peer}:`, data);
        if (data.type === 'chat' && data.message) {
            displayMessage(data.sender || conn.label, data.message, false);
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
            sender: myPeerId,
            message: messageText
        };

        console.log(`Sending message to ${openConnections} peers.`);
        for (const peerId in connections) {
            if (connections[peerId].open) {
                connections[peerId].send(messagePayload);
            } else {
                console.warn(`Connection to ${peerId} not open, skipping send.`);
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
    messageElement.appendChild(document.createTextNode(message));

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
    return id.length > 8 ? `${id.substring(0, 4)}...${id.substring(id.length - 4)}` : id;
}

// --- Event Listeners ---

sendButton.addEventListener('click', sendMessage);
messageInput.addEventListener('keypress', (event) => {
    if (event.key === 'Enter') {
        sendMessage();
    }
});

// Handle tab visibility changes to reconnect if necessary
document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible') {
        if (peer && peer.disconnected && !peer.destroyed) {
            peer.reconnect();
        }
    }
});

// --- Start the application ---
initializePeer();