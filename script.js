let streamAudioContext = null;
let nextPlayTime = 0;

// Разделенные состояния:
let isPlaying = false; // Состояние воспроизведения аудио от бота
let vadState = 'idle'; // Состояние диктофона (idle, recording, processing)
let isMicrophoneActive = false; // Включен ли микрофон кнопкой в UI

let stopWordRecognizer = null;
const stopWords = ['стоп', 'хватит', 'остановись', 'перестань', 'замолчи'];

function initStopWordDetection() {
    if (!stopWordRecognizer && ('webkitSpeechRecognition' in window)) {
        stopWordRecognizer = new webkitSpeechRecognition();
        stopWordRecognizer.continuous = true;
        stopWordRecognizer.interimResults = true;
        stopWordRecognizer.lang = 'ru-RU';
        stopWordRecognizer.onresult = function(e) {
            if (!isPlaying) return;
            for (let i = e.resultIndex; i < e.results.length; i++) {
                const transcript = e.results[i][0].transcript.toLowerCase();
                if (stopWords.some(w => transcript.includes(w))) {
                    stopPlayback();
                    break;
                }
            }
        };
    }
}

function startStopWordDetection() { if (stopWordRecognizer) { try { stopWordRecognizer.start(); } catch(e){} } }
function stopStopWordDetection() { if (stopWordRecognizer) { try { stopWordRecognizer.stop(); } catch(e){} } }

function stopPlayback() {
    if (streamAudioContext) { streamAudioContext.close(); streamAudioContext = null; nextPlayTime = 0; }
    stopStopWordDetection();
    isPlaying = false;
}

async function playPCM24kHz(base64Data) {
    if (!base64Data) return;
    try {
        if (!streamAudioContext) streamAudioContext = new (window.AudioContext || window.webkitAudioContext)({ sampleRate: 24000 });
        const binaryString = window.atob(base64Data);
        const bytes = new Uint8Array(binaryString.length);
        for (let i = 0; i < binaryString.length; i++) bytes[i] = binaryString.charCodeAt(i);
        
        const pcm16 = new Int16Array(bytes.buffer);
        const audioBuffer = streamAudioContext.createBuffer(1, pcm16.length, 24000);
        const channelData = audioBuffer.getChannelData(0);
        for (let i = 0; i < pcm16.length; i++) channelData[i] = pcm16[i] / 32768.0;
        
        const source = streamAudioContext.createBufferSource();
        source.buffer = audioBuffer;
        source.connect(streamAudioContext.destination);

        if (nextPlayTime < streamAudioContext.currentTime) nextPlayTime = streamAudioContext.currentTime;
        
        isPlaying = true;
        if (isMicrophoneActive) startStopWordDetection();

        source.onended = () => {
            if (streamAudioContext && streamAudioContext.currentTime >= nextPlayTime - 0.1) {
                if (isPlaying) {
                    isPlaying = false;
                    stopStopWordDetection();
                }
            }
        };

        source.start(nextPlayTime);
        nextPlayTime += audioBuffer.duration;
    } catch (e) { }
}

let userLogin = null;
let websocketConnection = null;
let pingInterval = null;
let currentFile = null;
let messageHistory = []; 
let pendingBubbleId = null;

window.editMessage = async function(msgId) {
    const bubble = document.getElementById('msg_' + msgId);
    if (!bubble) return;
    const textDiv = bubble.querySelector('div:first-child');
    const oldText = textDiv.textContent;
    
    const newText = prompt("Редактировать сообщение:", oldText);
    if (newText && newText.trim() !== "" && newText !== oldText) {
        textDiv.textContent = newText;
        const token = localStorage.getItem('token');
        if (token) {
            try {
                await fetch('/edit_message', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ token: token, msg_id: msgId, new_text: newText })
                });
            } catch (e) { showNotification("Ошибка редактирования", "error"); }
        } else {
            const msgObj = messageHistory.find(m => m.timestamp && m.content === oldText);
            if(msgObj) { msgObj.content = newText; localStorage.setItem('guestMessageHistory', JSON.stringify(messageHistory)); }
        }
    }
}

window.deleteMessage = async function(msgId) {
    const bubble = document.getElementById('msg_' + msgId);
    if (!bubble) return;
    
    const token = localStorage.getItem('token');
    if (token) {
        try {
            const resp = await fetch('/delete_message', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ token: token, msg_id: msgId })
            });
            if (resp.ok) bubble.remove();
        } catch (e) { showNotification("Ошибка удаления", "error"); }
    } else {
        const text = bubble.querySelector('div:first-child').textContent;
        messageHistory = messageHistory.filter(m => m.content !== text);
        localStorage.setItem('guestMessageHistory', JSON.stringify(messageHistory));
        bubble.remove();
    }
}

function addMessage(role, content, skipHistory = false, msgId = null) {
    const chatMessages = document.getElementById('chatMessages');
    const messageElement = document.createElement('div');
    messageElement.classList.add('message');
    messageElement.classList.add(role === 'user' ? 'user-message' : 'bot-message');

    const actualMsgId = msgId || (Date.now().toString() + Math.floor(Math.random()*1000).toString());
    
    if (content === '🎤 [Слушаю...]') {
        pendingBubbleId = 'msg_' + actualMsgId;
        messageElement.id = pendingBubbleId;
    } else {
        messageElement.id = 'msg_' + actualMsgId;
    }

    const now = new Date();
    const timeString = now.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });

    function escapeHtml(text) { return text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#039;"); }
    
    let actionsHtml = '';
    if (!content.includes('🎤') && !content.includes('⏳')) {
        actionsHtml = `
            <div class="message-actions">
                <button onclick="editMessage('${actualMsgId}')" title="Редактировать"><i class="fas fa-pencil-alt"></i></button>
                <button onclick="deleteMessage('${actualMsgId}')" title="Удалить"><i class="fas fa-trash"></i></button>
            </div>
        `;
    }

    messageElement.innerHTML = `<div>${escapeHtml(content)}</div><div class="message-time">${timeString}</div>${actionsHtml}`;
    chatMessages.appendChild(messageElement);
    chatMessages.scrollTop = chatMessages.scrollHeight;
    
    if (!localStorage.getItem('token') && !skipHistory && role === 'user' && !content.includes('🎤') && !content.includes('⏳')) {
        messageHistory.push({ role: role, content: content, timestamp: now.toISOString() });
        localStorage.setItem('guestMessageHistory', JSON.stringify(messageHistory));
    } else if (!localStorage.getItem('token') && !skipHistory && role === 'assistant') {
        messageHistory.push({ role: role, content: content, timestamp: now.toISOString() });
        localStorage.setItem('guestMessageHistory', JSON.stringify(messageHistory));
    }
}

function updatePendingBubble(text) {
    if (!pendingBubbleId) return;
    const b = document.getElementById(pendingBubbleId);
    if (b) {
        b.querySelector('div:first-child').textContent = text;
        if (!text.includes('⏳') && !text.includes('🎤') && !text.includes('Аудиосообщение') && !localStorage.getItem('token')) {
            messageHistory.push({ role: 'user', content: text, timestamp: new Date().toISOString() });
            localStorage.setItem('guestMessageHistory', JSON.stringify(messageHistory));
        }
    }
}

function removePendingBubble() {
    if (!pendingBubbleId) return;
    const b = document.getElementById(pendingBubbleId);
    if (b) { b.remove(); pendingBubbleId = null; }
}

function handleIncomingStreamData(data) {
    if (data.type === 'msg_id_map') {
        const userBubble = document.getElementById('msg_' + data.ui_msg_id);
        if (userBubble) {
            userBubble.id = 'msg_' + data.user_msg_id;
            // ИСПРАВЛЕНИЕ: Обновляем pendingBubbleId, если он совпадал с временным ID
            if (pendingBubbleId === 'msg_' + data.ui_msg_id) {
                pendingBubbleId = 'msg_' + data.user_msg_id;
            }
            const editBtn = userBubble.querySelector('button[title="Редактировать"]');
            const delBtn = userBubble.querySelector('button[title="Удалить"]');
            if (editBtn) editBtn.setAttribute('onclick', `editMessage('${data.user_msg_id}')`);
            if (delBtn) delBtn.setAttribute('onclick', `deleteMessage('${data.user_msg_id}')`);
        }
    }

    if (data.type === 'user_transcription') {
        updatePendingBubble(data.text);
        const b = document.getElementById(pendingBubbleId);
        if (b && !data.text.includes('Аудиосообщение')) {
            const actualId = pendingBubbleId.replace('msg_', '');
            b.insertAdjacentHTML('beforeend', `
            <div class="message-actions">
                <button onclick="editMessage('${actualId}')" title="Редактировать"><i class="fas fa-pencil-alt"></i></button>
                <button onclick="deleteMessage('${actualId}')" title="Удалить"><i class="fas fa-trash"></i></button>
            </div>`);
        }
        pendingBubbleId = null; 
    }
    
    if (data.type === 'new_message') {
        if (vadState === 'processing') vadState = 'idle'; // Возвращаем VAD в режим ожидания

        if (data.text) {
            const msgId = data.message_id || data.ui_msg_id;
            const bubbleId = msgId ? 'msg_' + msgId : null;
            let existingBubble = bubbleId ? document.getElementById(bubbleId) : null;

            if (existingBubble) {
                let textDiv = existingBubble.querySelector('div:first-child');
                textDiv.textContent += data.text; 
                document.getElementById('chatMessages').scrollTop = document.getElementById('chatMessages').scrollHeight;

                if (!localStorage.getItem('token') && messageHistory.length > 0) {
                    if (messageHistory[messageHistory.length - 1].role === 'assistant') {
                        messageHistory[messageHistory.length - 1].content += data.text;
                        localStorage.setItem('guestMessageHistory', JSON.stringify(messageHistory));
                    }
                }
            } else {
                addMessage('assistant', data.text, false, msgId);
            }
        }
        
        if (data.actions && Array.isArray(data.actions)) {
            data.actions.forEach(action => {
                if (action.action_type === 'очистка истории') clearHistory();
                if (action.action_type === 'выключить микрофон') {
                    if (isMicrophoneActive) {
                        document.getElementById('microphone-btn').click();
                        showNotification("Микрофон выключен ассистентом", "info");
                    }
                }
                if (action.action_type === 'смена голоса') {
                    const select = document.getElementById('voice-type');
                    for (let i = 0; i < select.options.length; i++) {
                        if (select.options[i].value.toLowerCase() === String(action.action_value).toLowerCase()) {
                            select.selectedIndex = i; saveSettingsToLocalStorage(); break;
                        }
                    }
                }
            });
        }
    }

    if (data.type === 'delete_message') removePendingBubble();
    if (data.type === 'audio_chunk') playPCM24kHz(data.audio_base64);
    if (data.type === 'notification') showNotification(data.message, data.level || 'info');
}

const clearHistoryBtn = document.getElementById('clear-history');
clearHistoryBtn.addEventListener('click', async function() { clearHistory(); });

async function clearHistory(){
    const token = localStorage.getItem('token');
    if (token) {
        try {
            const response = await fetch('/clear_history', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ token: token }) });
            const data = await response.json();
            if (data.status === 'success') {
                const chatMessages = document.getElementById('chatMessages');
                const welcomeMessage = chatMessages.querySelector('.bot-message');
                chatMessages.innerHTML = '';
                if (welcomeMessage) chatMessages.appendChild(welcomeMessage);
                showNotification(data.message, 'success');
            } else showNotification(data.message, 'error');
        } catch (error) { showNotification('Произошла ошибка при очистке истории', 'error'); }
    } else {
        messageHistory = []; localStorage.removeItem('guestMessageHistory');
        const chatMessages = document.getElementById('chatMessages');
        const welcomeMessage = chatMessages.querySelector('.bot-message');
        chatMessages.innerHTML = '';
        if (welcomeMessage) chatMessages.appendChild(welcomeMessage);
        showNotification('История чата очищена', 'info');
    }
}

function connectWebSocket() {
    const token = localStorage.getItem('token');
    const userLogin = localStorage.getItem('userLogin');
    if (!token) return;

    if (websocketConnection) websocketConnection.close();
    websocketConnection = new WebSocket('wss://friday-assistant.ru/ws');

    websocketConnection.onopen = function(event) {
        websocketConnection.send(btoa(unescape(encodeURIComponent(JSON.stringify({ type: 'web_client_auth', token: token, login: userLogin })))));
        pingInterval = setInterval(() => {
            if (websocketConnection.readyState === WebSocket.OPEN) {
                websocketConnection.send(btoa(unescape(encodeURIComponent(JSON.stringify({ type: 'ping', timestamp: Date.now() })))));
            }
        }, 30000);
    };

    websocketConnection.onmessage = function(event) {
        try {
            const data = JSON.parse(decodeURIComponent(escape(atob(event.data))));
            
            if (data.status === 'success' && data.history && Array.isArray(data.history)) {
                const chatMessages = document.getElementById('chatMessages');
                const welcomeMessage = chatMessages.querySelector('.bot-message');
                chatMessages.innerHTML = '';
                if (welcomeMessage) chatMessages.appendChild(welcomeMessage);
                data.history.forEach(msg => {
                    if (msg.sender === 'Вы') addMessage('user', msg.text, true, msg.id);
                    else {
                        const actionBlocks = msg.text.split('⸵');
                        let displayText = "";
                        actionBlocks.forEach(action => {
                            const sepIndex = action.indexOf('|');
                            if (sepIndex !== -1) displayText += action.substring(sepIndex + 1).trim() + '\n\n';
                            else displayText += action + '\n\n';
                        });
                        if (displayText.trim()) addMessage('assistant', displayText.trim(), true, msg.id);
                    }
                });
            } else {
                handleIncomingStreamData(data);
            }
        } catch (error) { }
    };
    
    websocketConnection.onclose = function(event) {
        if (pingInterval) { clearInterval(pingInterval); pingInterval = null; }
        setTimeout(() => { if (localStorage.getItem('token')) connectWebSocket(); }, 5000);
    };
}

function showNotification(message, type = 'success') {
    const notification = document.getElementById('notification');
    const notificationText = document.getElementById('notification-text');
    notificationText.textContent = message;
    notification.className = `notification ${type} show`;
    setTimeout(() => { notification.classList.remove('show'); }, 5000);
}

function openRegisterModal() { document.getElementById('registerModal').style.display = 'flex'; document.body.style.overflow = 'hidden'; }
function openLoginModal() { document.getElementById('loginModal').style.display = 'flex'; document.body.style.overflow = 'hidden'; }
function closeModals() { document.getElementById('registerModal').style.display = 'none'; document.getElementById('loginModal').style.display = 'none'; document.getElementById('recoveryModal').style.display = 'none'; document.body.style.overflow = 'auto'; }
function openRecoveryModal() { document.getElementById('recoveryModal').style.display = 'flex'; }
function closeRecoveryModal() { document.getElementById('recoveryModal').style.display = 'none'; }
function addEventListenerSafe(element, event, handler) { if (element && typeof handler === 'function') element.addEventListener(event, handler); }

function updateAuthUI() {
    const authButtons = document.querySelector('.auth-buttons');
    if (!authButtons) return;
    if (userLogin) {
        authButtons.innerHTML = `<div class="user-info"><span class="user-login">${userLogin}</span><button class="auth-btn logout-btn">Выйти</button></div>`;
        addEventListenerSafe(document.querySelector('.logout-btn'), 'click', logout);
    } else {
        authButtons.innerHTML = `<button class="auth-btn register-btn">Регистрация</button><button class="auth-btn login-btn">Вход</button>`;
        addEventListenerSafe(document.querySelector('.register-btn'), 'click', openRegisterModal);
        addEventListenerSafe(document.querySelector('.login-btn'), 'click', openLoginModal);
    }
}

async function verifyToken() {
    const token = localStorage.getItem('token');
    const savedLogin = localStorage.getItem('userLogin');
    if (!token || !savedLogin) return false;
    try {
        const response = await fetch(`/verify_token?token=${encodeURIComponent(token)}`, { method: 'GET' });
        const data = await response.json();
        if (response.ok && data.status === 'success') { userLogin = data.user_login; return true; } 
        else { localStorage.removeItem('token'); localStorage.removeItem('userLogin'); userLogin = null; return false; }
    } catch (error) { localStorage.removeItem('token'); localStorage.removeItem('userLogin'); userLogin = null; return false; }
}

async function logout() {
    try {
        const token = localStorage.getItem('token');
        if (websocketConnection) { websocketConnection.close(); websocketConnection = null; }
        if (pingInterval) { clearInterval(pingInterval); pingInterval = null; }
        if (token) { try { await fetch('/logout_web', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ token: token }) }); } catch (error) {} }
        localStorage.removeItem('token'); localStorage.removeItem('userLogin'); userLogin = null;
        updateAuthUI();
        messageHistory = []; localStorage.removeItem('guestMessageHistory');
        const chatMessages = document.getElementById('chatMessages');
        const welcomeMessage = chatMessages.querySelector('.bot-message');
        chatMessages.innerHTML = '';
        if (welcomeMessage) chatMessages.appendChild(welcomeMessage);
        showNotification('Вы успешно вышли из системы', 'success');
    } catch (error) { showNotification('Произошла ошибка при выходе из системы', 'error'); }
}

function saveSettingsToLocalStorage() {
    localStorage.setItem('voiceType', document.getElementById('voice-type').value);
}

function loadSettingsFromLocalStorage() {
    const savedVoiceType = localStorage.getItem('voiceType');
    if (savedVoiceType) document.getElementById('voice-type').value = savedVoiceType;
}

function loadGuestMessageHistory() {
    const savedHistory = localStorage.getItem('guestMessageHistory');
    if (savedHistory && !localStorage.getItem('token')) {
        try {
            messageHistory = JSON.parse(savedHistory);
            const chatMessages = document.getElementById('chatMessages');
            const welcomeMessage = chatMessages.querySelector('.bot-message');
            chatMessages.innerHTML = '';
            if (welcomeMessage) chatMessages.appendChild(welcomeMessage);
            messageHistory.forEach((msg, idx) => { addMessage(msg.role, msg.content, true, 'guest_' + idx); });
        } catch (error) { messageHistory = []; }
    }
}

document.addEventListener('DOMContentLoaded', async function() {
    loadSettingsFromLocalStorage();
    loadGuestMessageHistory();
    initStopWordDetection();

    const hasValidToken = await verifyToken();
    if (hasValidToken) { updateAuthUI(); connectWebSocket(); } 
    else { updateAuthUI(); }

    const messageInput = document.getElementById('messageInput');
    const sendButton = document.getElementById('sendMessage');
    const microphoneBtn = document.getElementById('microphone-btn');
    const fileUpload = document.getElementById('file-upload');
    const voiceType = document.getElementById('voice-type');
    const closeButtons = document.querySelectorAll('.modal-close');
    let forgotPasswordLink = document.getElementById('forgotPassword');

    voiceType.addEventListener('change', saveSettingsToLocalStorage);
    messageInput.addEventListener('input', function() { this.style.height = 'auto'; this.style.height = (this.scrollHeight) + 'px'; });

    // Инициализация аудио-переменных
    let micAudioContext = null;
    let audioWorkletNode = null;
    let micStream = null;
    let pcmBuffer = [];
    let preBuffer = [];
    let silenceFrames = 0;
    
    const VAD_THRESHOLD = 0.015;
    const SILENCE_FRAMES = 16000 * 1.5; 
    const PRE_BUFFER_FRAMES = 8000; 

    const workletCode = `
    class PCMProcessor extends AudioWorkletProcessor {
        process(inputs, outputs, parameters) {
            const input = inputs[0];
            if (input && input.length > 0) {
                const channelData = input[0];
                const pcm16 = new Int16Array(channelData.length);
                let sum = 0;
                for (let i = 0; i < channelData.length; i++) {
                    let s = Math.max(-1, Math.min(1, channelData[i]));
                    pcm16[i] = s < 0 ? s * 0x8000 : s * 0x7FFF;
                    sum += Math.abs(s);
                }
                this.port.postMessage({ pcm: pcm16, volume: sum / channelData.length });
            }
            return true;
        }
    }
    registerProcessor('pcm-processor', PCMProcessor);
    `;

    function bufferToBase64(buffer) {
        let binary = '';
        const bytes = new Uint8Array(buffer);
        const chunkSize = 0x8000;
        for (let i = 0; i < bytes.length; i += chunkSize) { binary += String.fromCharCode.apply(null, bytes.subarray(i, i + chunkSize)); }
        return window.btoa(binary);
    }

    function sendCurrentRecording() {
        vadState = 'processing';
        updatePendingBubble('⏳ Транскрибирую...');
        const pcm16 = new Int16Array(pcmBuffer);
        const base64Audio = bufferToBase64(pcm16.buffer);
        pcmBuffer = []; preBuffer = [];
        const actualUiMsgId = pendingBubbleId ? pendingBubbleId.replace('msg_', '') : null;
        sendToServer("", "голосовое сообщение", base64Audio, actualUiMsgId);
    }

    async function startMicStream() {
        micStream = await navigator.mediaDevices.getUserMedia({ audio: { echoCancellation: true, noiseSuppression: true } });
        micAudioContext = new (window.AudioContext || window.webkitAudioContext)({ sampleRate: 16000 });
        
        const blob = new Blob([workletCode], { type: 'application/javascript' });
        const workletUrl = URL.createObjectURL(blob);
        await micAudioContext.audioWorklet.addModule(workletUrl);

        const source = micAudioContext.createMediaStreamSource(micStream);
        audioWorkletNode = new AudioWorkletNode(micAudioContext, 'pcm-processor');
        
        const zeroGain = micAudioContext.createGain();
        zeroGain.gain.value = 0;
        
        source.connect(audioWorkletNode);
        audioWorkletNode.connect(zeroGain);
        zeroGain.connect(micAudioContext.destination);

        audioWorkletNode.port.onmessage = (e) => {
            if (!isMicrophoneActive) return; // Если микрофон выключен аппаратно, игнорируем
            if (isPlaying || vadState === 'processing') return; // Игнорируем, если бот говорит или мы в обработке

            const { pcm, volume } = e.data;

            if (vadState === 'idle') {
                preBuffer.push(...pcm);
                if (preBuffer.length > PRE_BUFFER_FRAMES) preBuffer.splice(0, preBuffer.length - PRE_BUFFER_FRAMES);

                if (volume > VAD_THRESHOLD) {
                    vadState = 'recording';
                    pcmBuffer = [...preBuffer];
                    silenceFrames = 0;
                    addMessage('user', '🎤 [Слушаю...]');
                }
            } else if (vadState === 'recording') {
                pcmBuffer.push(...pcm);
                if (volume < VAD_THRESHOLD) {
                    silenceFrames += pcm.length;
                    if (silenceFrames > SILENCE_FRAMES) {
                        sendCurrentRecording();
                    }
                } else {
                    silenceFrames = 0;
                }
            }
        };
    }
    
    function stopMicStream() {
        if (audioWorkletNode) { audioWorkletNode.disconnect(); audioWorkletNode = null; }
        if (micStream) { micStream.getTracks().forEach(track => track.stop()); micStream = null; }
        if (micAudioContext) { micAudioContext.close(); micAudioContext = null; }
    }

    microphoneBtn.addEventListener('click', async function() {
        if (!isMicrophoneActive) {
            // Включаем микрофон
            try {
                await startMicStream();
                isMicrophoneActive = true;
                vadState = 'idle';
                this.classList.add('active');
                this.querySelector('span').textContent = 'Микрофон включен';
            } catch (err) {
                showNotification('Ошибка доступа к микрофону', 'error');
            }
        } else {
            // Выключаем микрофон
            isMicrophoneActive = false;
            this.classList.remove('active');
            this.querySelector('span').textContent = 'Включить микрофон';
            
            if (vadState === 'recording') {
                // Если мы что-то наговорили и нажали "выключить" - принудительно отправляем!
                sendCurrentRecording();
            } else if (vadState === 'processing') {
                // ИСПРАВЛЕНИЕ: Запрос уже ушел на сервер (Транскрибирую...). 
                // Ничего не сбрасываем и плашку НЕ удаляем! Просто ждем ответа.
            } else {
                // Если мы просто молчали (idle)
                vadState = 'idle';
                pcmBuffer = []; preBuffer = [];
                removePendingBubble(); // Удаляем плашку "Слушаю..."
            }
            stopMicStream();
        }
    });
    
    async function sendToServer(prompt, command_type, audio_base64 = null, ui_msg_id = null) {
        try {
            const token = localStorage.getItem('token');
            const userLogin = localStorage.getItem('userLogin');
            const file = currentFile;
            const selectedVoice = document.getElementById('voice-type').value;
            const finalUiMsgId = ui_msg_id || (Date.now().toString() + Math.floor(Math.random()*1000).toString());
            
            if (token && userLogin && websocketConnection && websocketConnection.readyState === WebSocket.OPEN) {
                const requestData = {
                    type: 'web_command',
                    command: prompt,
                    audio_base64: audio_base64,
                    token: token,
                    timestamp: new Date().toISOString(),
                    name: "Пятница",
                    voice_type: selectedVoice,
                    command_type: command_type,
                    ui_msg_id: finalUiMsgId
                };
                
                if (file) {
                    const reader = new FileReader();
                    reader.onload = function() {
                        requestData.screenshot = reader.result.split(',')[1];
                        websocketConnection.send(btoa(unescape(encodeURIComponent(JSON.stringify(requestData)))));
                        currentFile = null; document.getElementById('imagePreviewContainer').style.display = 'none'; fileUpload.value = '';
                    };
                    reader.readAsDataURL(file);
                } else {
                    websocketConnection.send(btoa(unescape(encodeURIComponent(JSON.stringify(requestData)))));
                }
            } else {
                const requestData = {
                    prompt: prompt,
                    audio_base64: audio_base64,
                    bot_name: "Пятница",
                    voice_type: selectedVoice,
                    command_type: command_type,
                    ui_msg_id: finalUiMsgId
                };
                
                if (!token) requestData.message_history = messageHistory;
                
                if (file) {
                    const reader = new FileReader();
                    reader.onload = function() {
                        requestData.screenshot = reader.result.split(',')[1];
                        sendFetchRequest(requestData);
                    };
                    reader.readAsDataURL(file);
                } else {
                    sendFetchRequest(requestData);
                }
            }
        } catch (error) { 
            showNotification('Ошибка при обработке запроса', 'error'); 
            vadState = 'idle';
            removePendingBubble();
        }
    }

    async function sendFetchRequest(requestData) {
        try {
            const response = await fetch('/generate', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(requestData) });
            if (!response.ok) throw new Error(`Ошибка сервера`);
            
            const reader = response.body.getReader();
            const decoder = new TextDecoder("utf-8");
            let buffer = "";

            while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                
                buffer += decoder.decode(value, { stream: true });
                let messages = buffer.split('\n\n');
                buffer = messages.pop(); 

                for (let msg of messages) {
                    if (msg.startsWith('data: ')) {
                        try {
                            const data = JSON.parse(msg.substring(6));
                            handleIncomingStreamData(data);
                        } catch(e) {}
                    }
                }
            }
            
            currentFile = null; document.getElementById('imagePreviewContainer').style.display = 'none'; fileUpload.value = '';
        } catch (error) { 
            showNotification('Ошибка при обработке запроса', 'error'); 
            vadState = 'idle';
            removePendingBubble(); 
        }
    }

    sendButton.addEventListener('click', function() {
        const message = messageInput.value.trim();
        if (!message && !currentFile) return;
        messageInput.style.height = 'auto';
        const uiMsgId = Date.now().toString() + Math.floor(Math.random()*1000).toString();
        if (message) addMessage('user', message, false, uiMsgId);
        messageInput.value = '';
        document.getElementById('imagePreviewContainer').style.display = 'none';
        sendToServer(message, "текстовое сообщение", null, uiMsgId);
    });
                
    messageInput.addEventListener('keydown', function(e) { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendButton.click(); } });

    fileUpload.addEventListener('change', function(e) {
        const file = e.target.files[0];
        const previewContainer = document.getElementById('imagePreviewContainer');
        previewContainer.innerHTML = ''; previewContainer.style.display = 'none'; currentFile = null; 
        if (file) {
            if (!file.type.match('image/png') && !file.type.match('image/jpeg')) { showNotification('Можно загружать только PNG и JPG', 'error'); this.value = ''; return; }
            currentFile = file;
            const reader = new FileReader();
            reader.onload = function(e) {
                const img = document.createElement('img'); img.src = e.target.result; img.classList.add('image-preview');
                const removeBtn = document.createElement('button'); removeBtn.classList.add('remove-image'); removeBtn.innerHTML = '&times;';
                removeBtn.addEventListener('click', function() { previewContainer.style.display = 'none'; fileUpload.value = ''; currentFile = null; });
                previewContainer.appendChild(img); previewContainer.appendChild(removeBtn); previewContainer.style.display = 'block';
            };
            reader.readAsDataURL(file);
        }
    });

    closeButtons.forEach(btn => { btn.addEventListener('click', closeModals); });
    window.addEventListener('click', function(event) { if (event.target === document.getElementById('registerModal') || event.target === document.getElementById('loginModal') || event.target === document.getElementById('recoveryModal')) closeModals(); });

    document.getElementById('registerForm').addEventListener('submit', async function(e) {
        e.preventDefault();
        const email = document.getElementById('regEmail').value; const login = document.getElementById('regLogin').value; const password = document.getElementById('regPassword').value; const passwordConfirm = document.getElementById('regPasswordConfirm').value; const responseElement = document.getElementById('registerResponse'); const submitButton = this.querySelector('button[type="submit"]'); const originalText = submitButton.textContent;
        if (password !== passwordConfirm) { responseElement.textContent = 'Пароли не совпадают!'; responseElement.className = 'response-message error'; return; }
        try {
            submitButton.disabled = true; submitButton.textContent = 'Регистрация...';
            const response = await fetch('/register', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ email, login, password }) });
            const data = await response.json();
            if (response.ok) { responseElement.textContent = data.message; responseElement.className = 'response-message success'; setTimeout(() => { this.reset(); responseElement.className = 'response-message'; closeModals(); }, 2000); } 
            else { responseElement.textContent = data.message; responseElement.className = 'response-message error'; }
        } catch (error) { responseElement.textContent = 'Ошибка'; responseElement.className = 'response-message error'; } 
        finally { submitButton.disabled = false; submitButton.textContent = originalText; }
    });

    document.getElementById('loginForm').addEventListener('submit', async function(e) {
        e.preventDefault();
        const login = document.getElementById('loginEmail').value; const password = document.getElementById('loginPassword').value; const responseElement = document.getElementById('loginResponse'); const submitButton = this.querySelector('button[type="submit"]'); const originalText = submitButton.textContent;
        try {
            submitButton.disabled = true; submitButton.textContent = 'Вход...';
            const response = await fetch('/login_web', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ login, password }) });
            const data = await response.json();
            if (response.ok) {
                responseElement.textContent = data.message; responseElement.className = 'response-message success';
                userLogin = data.user_login; localStorage.setItem('token', data.token); localStorage.setItem('userLogin', data.user_login);
                messageHistory = []; localStorage.removeItem('guestMessageHistory');
                updateAuthUI(); connectWebSocket();
                setTimeout(() => { this.reset(); responseElement.className = 'response-message'; closeModals(); }, 2000);
            } else { responseElement.textContent = data.message; responseElement.className = 'response-message error'; }
        } catch (error) { responseElement.textContent = 'Ошибка'; responseElement.className = 'response-message error'; } 
        finally { submitButton.disabled = false; submitButton.textContent = originalText; }
    });
            
    forgotPasswordLink.addEventListener('click', function(e) { e.preventDefault(); closeModals(); document.getElementById('recoveryModal').style.display = 'flex'; });
    document.querySelector('.recovery-close').addEventListener('click', function() { document.getElementById('recoveryModal').style.display = 'none'; });
            
    document.getElementById('recoveryForm').addEventListener('submit', async function(e) {
        e.preventDefault();
        const email = document.getElementById('recoveryEmail').value; const responseElement = document.getElementById('recoveryResponse'); const submitButton = this.querySelector('button[type="submit"]'); const originalText = submitButton.textContent;
        try {
            submitButton.disabled = true; submitButton.textContent = 'Отправка...';
            const response = await fetch('/recover-password', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ email }) });
            const data = await response.json();
            if (response.ok) { responseElement.textContent = data.message; responseElement.className = 'response-message success'; setTimeout(() => { document.getElementById('recoveryModal').style.display = 'none'; this.reset(); responseElement.className = 'response-message'; }, 3000); } 
            else { responseElement.textContent = data.message; responseElement.className = 'response-message error'; }
        } catch (error) { responseElement.textContent = 'Ошибка'; responseElement.className = 'response-message error'; } 
        finally { submitButton.disabled = false; submitButton.textContent = originalText; }
    });
});