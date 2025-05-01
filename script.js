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
        // You might want to add more robust handling here if needed.
    });

    peer.on('close', () => {
        updateStatus('PeerJS connection closed.');
        console.warn('PeerJS connection closed.');
        // Maybe attempt to re-initialize?
    });

    peer.on('error', (err) => {
        updateStatus(`PeerJS Error: ${err.type}`);
        console.error('PeerJS Error:', err);
        // Common errors: 'network', 'unavailable-id', 'webrtc', 'server-error'
        // Handle specific errors if necessary
        if (err.type === 'unavailable-id') {
            // This shouldn't happen if ID is undefined, but just in case
            peer = null; // Force re-creation
            setTimeout(initializePeer, 3000);
        } else if (err.type === 'network' || err.type === 'server-error') {
             // Might need to retry connection or inform user
             updateStatus('Connection issues. Check network or try refreshing.');
        }
    });
}

function initializeSignaling() {
    // Connect to the '/msg' namespace on the Socket.IO server
    socket = io(`${API_SERVER_URL}/msg`, { // Still connect to the /msg namespace
        path: '/box/socket.io',         // *** ADD THIS LINE *** Tell client the actual server path
        // Optional: Explicitly setting transports can sometimes help if polling fails
        // and websockets are preferred/available.
        // transports: ['websocket', 'polling'],
    });

    socket.on('connect', () => {
        console.log('Connected to signaling server (Socket.IO)');
        updateStatus(`Connected as ${shortId(myPeerId)}. Joining chat...`);
        // Join the chat room with our PeerJS ID
        socket.emit('join-room', myPeerId);
    });

    socket.on('connect_error', (err) => {
        // Log the full error object for more details
        console.error('Signaling connection error:', err);
        // Display a more informative message if possible
        let errorReason = err.message; // Basic message
        if (err.cause) { // Check for underlying cause (like XHR status)
            errorReason += ` (cause: ${err.cause.status || err.cause.message || 'unknown'})`;
        }
        updateStatus(`Error connecting to signaling server: ${errorReason}`);
    });

    socket.on('disconnect', (reason) => {
        console.warn('Disconnected from signaling server:', reason);
        updateStatus('Disconnected from signaling server.');
        // Handle disconnection, maybe attempt reconnection after a delay
        // Note: Socket.IO client usually attempts reconnection automatically
    });

    // --- Signaling Event Handlers --- (rest of the function remains the same)
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
            connections[peerId].close(); // Ensure connection is closed P2P side
            delete connections[peerId];
            updateStatus(`${Object.keys(connections).length} user(s) online.`);
        } else {
            // If we didn't have a direct connection (maybe they left quickly)
             displaySystemMessage(`${shortId(peerId)} left the chat.`);
             updateStatus(`${Object.keys(connections).length} user(s) online.`);
        }
    });
}

// --- Peer Connection Handling ---

function connectToPeer(peerId) {
    console.log(`Attempting to connect to ${peerId}`);
    const conn = peer.connect(peerId, {
        reliable: true, // Use reliable data channel (TCP-like)
        label: myPeerId // Send our ID as label (optional)
    });
    setupConnection(conn);
}

function setupConnection(conn) {
    conn.on('open', () => {
        console.log(`Connection established with ${conn.peer}`);
        connections[conn.peer] = conn;
        conn.label = conn.label || conn.peer; // Store the label if provided
        updateStatus(`${Object.keys(connections).length} user(s) online.`);
         // Optional: Send a handshake message or request username
         // conn.send({ type: 'handshake', sender: myPeerId });
    });

    conn.on('data', (data) => {
        console.log(`Data received from ${conn.peer}:`, data);
        if (data.type === 'chat' && data.message) {
            displayMessage(data.sender || conn.label, data.message, false); // false = received
        }
        // Handle other data types if needed (e.g., username exchange)
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

    // Display message locally immediately
    displayMessage(myPeerId, messageText, true); // true = sent

    // Prepare message payload
    const messagePayload = {
        type: 'chat',
        sender: myPeerId, // Or a chosen nickname
        message: messageText
    };

    // Send to all connected peers
    console.log(`Sending message to ${Object.keys(connections).length} peers.`);
    for (const peerId in connections) {
        if (connections[peerId] && connections[peerId].open) {
            connections[peerId].send(messagePayload);
        } else {
            console.warn(`Connection to ${peerId} not open, skipping send.`);
        }
    }

    messageInput.value = ''; // Clear input field
}

function displayMessage(senderId, message, isSent) {
    const messageElement = document.createElement('div');
    messageElement.classList.add('message');
    messageElement.classList.add(isSent ? 'sent' : 'received');

    const senderElement = document.createElement('span');
    senderElement.classList.add('sender');
    // Show only a short part of the ID for readability
    senderElement.textContent = isSent ? 'You' : shortId(senderId);

    messageElement.appendChild(senderElement);
    messageElement.appendChild(document.createTextNode(message)); // Use textNode to prevent XSS

    messagesDiv.appendChild(messageElement);
    messagesDiv.scrollTop = messagesDiv.scrollHeight; // Scroll to bottom
}

function displaySystemMessage(message) {
     const messageElement = document.createElement('div');
     messageElement.classList.add('message', 'system');
     messageElement.textContent = message;
     messagesDiv.appendChild(messageElement);
     messagesDiv.scrollTop = messagesDiv.scrollHeight; // Scroll to bottom
}


function updateStatus(text) {
    statusDiv.textContent = text;
}

function shortId(id) {
    if (!id) return 'Anonymous';
    // Show first 4 and last 4 chars of the PeerJS ID
    return id.length > 8 ? `${id.substring(0, 4)}...${id.substring(id.length - 4)}` : id;
}

// --- Event Listeners ---

sendButton.addEventListener('click', sendMessage);
messageInput.addEventListener('keypress', (event) => {
    if (event.key === 'Enter') {
        sendMessage();
    }
});

// --- Start the application ---
initializePeer();