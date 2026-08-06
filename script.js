let streamAudioContext = null;
let nextPlayTime = 0;
let isPlaying = false; 
let vadState = 'idle'; 
let isMicrophoneActive = false; 

let stopWordRecognizer = null;
const stopWords = ['стоп', 'хватит', 'остановись', 'перестань', 'замолчи'];
let ignoredMessageId = null; 

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

function startStopWordDetection() { 
    // ОТКЛЮЧАЕМ НА МОБИЛЬНЫХ УСТРОЙСТВАХ, ЧТОБЫ ANDROID НЕ ПИЛИКАЛ
    const isMobile = /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent);
    if (isMobile) return;

    if (stopWordRecognizer) { 
        try { stopWordRecognizer.start(); } catch(e){} 
    } 
}

function stopStopWordDetection() { 
    if (stopWordRecognizer) { 
        try { stopWordRecognizer.stop(); } catch(e){} 
    } 
}

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
                if (isPlaying) { isPlaying = false; stopStopWordDetection(); }
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
let activeStreamMsgId = null;
let chunkInterval = null;

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
            try { await fetch('/edit_message', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ token: token, msg_id: msgId, new_text: newText }) }); } catch (e) {}
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
            const resp = await fetch('/delete_message', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ token: token, msg_id: msgId }) });
            if (resp.ok) bubble.remove();
        } catch (e) { }
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
    messageElement.classList.add('message', role === 'user' ? 'user-message' : 'bot-message');
    const actualMsgId = msgId || (Date.now().toString() + Math.floor(Math.random()*1000).toString());
    
    if (content === '🎤 [Слушаю...]') { pendingBubbleId = 'msg_' + actualMsgId; messageElement.id = pendingBubbleId; } 
    else messageElement.id = 'msg_' + actualMsgId;

    const timeString = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    function escapeHtml(text) { return text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#039;"); }
    
    let actionsHtml = (!content.includes('🎤') && !content.includes('⏳')) ? `<div class="message-actions"><button onclick="editMessage('${actualMsgId}')" title="Редактировать"><i class="fas fa-pencil-alt"></i></button><button onclick="deleteMessage('${actualMsgId}')" title="Удалить"><i class="fas fa-trash"></i></button></div>` : '';
    messageElement.innerHTML = `<div>${escapeHtml(content)}</div><div class="message-time">${timeString}</div>${actionsHtml}`;
    chatMessages.appendChild(messageElement);
    chatMessages.scrollTop = chatMessages.scrollHeight;
    
    if (!localStorage.getItem('token') && !skipHistory && role === 'user' && !content.includes('🎤') && !content.includes('⏳')) { messageHistory.push({ role: role, content: content, timestamp: new Date().toISOString() }); localStorage.setItem('guestMessageHistory', JSON.stringify(messageHistory)); } 
    else if (!localStorage.getItem('token') && !skipHistory && role === 'assistant') { messageHistory.push({ role: role, content: content, timestamp: new Date().toISOString() }); localStorage.setItem('guestMessageHistory', JSON.stringify(messageHistory)); }
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

function removePendingBubble() { if (pendingBubbleId) { const b = document.getElementById(pendingBubbleId); if (b) b.remove(); pendingBubbleId = null; } }

function handleIncomingStreamData(data) {
    if (data.type === 'msg_id_map') {
        const userBubble = document.getElementById('msg_' + data.ui_msg_id);
        if (userBubble) {
            userBubble.id = 'msg_' + data.user_msg_id;
            if (pendingBubbleId === 'msg_' + data.ui_msg_id) pendingBubbleId = 'msg_' + data.user_msg_id;
            const editBtn = userBubble.querySelector('button[title="Редактировать"]'); const delBtn = userBubble.querySelector('button[title="Удалить"]');
            if (editBtn) editBtn.setAttribute('onclick', `editMessage('${data.user_msg_id}')`);
            if (delBtn) delBtn.setAttribute('onclick', `deleteMessage('${data.user_msg_id}')`);
        }
    }

    if (data.type === 'user_transcription') {
        updatePendingBubble(data.text);
        const b = document.getElementById(pendingBubbleId);
        if (b && !data.text.includes('Аудиосообщение')) {
            const actualId = pendingBubbleId.replace('msg_', '');
            if(!b.querySelector('.message-actions')) b.insertAdjacentHTML('beforeend', `<div class="message-actions"><button onclick="editMessage('${actualId}')"><i class="fas fa-pencil-alt"></i></button><button onclick="deleteMessage('${actualId}')"><i class="fas fa-trash"></i></button></div>`);
        }
        if(!data.text.includes('🎤')) pendingBubbleId = null; 
    }
    
    if (data.actions && Array.isArray(data.actions)) {
        data.actions.forEach(action => {
            if (action.action_type === 'очистка истории') { ignoredMessageId = data.message_id || data.ui_msg_id; clearHistory(); }
            if (action.action_type === 'выключить микрофон') { if (isMicrophoneActive) { document.getElementById('microphone-btn').click(); showNotification("Микрофон выключен", "info"); } }
            if (action.action_type === 'смена голоса') {
                const select = document.getElementById('voice-type');
                for (let i = 0; i < select.options.length; i++) { if (select.options[i].value.toLowerCase() === String(action.action_value).toLowerCase()) { select.selectedIndex = i; saveSettingsToLocalStorage(); break; } }
            }
        });
    }

    if (data.type === 'new_message') {
        if (vadState === 'processing') vadState = 'idle';
        const msgId = data.message_id || data.ui_msg_id;
        if (data.text && msgId !== ignoredMessageId) {
            const bubbleId = msgId ? 'msg_' + msgId : null;
            let existingBubble = bubbleId ? document.getElementById(bubbleId) : null;
            if (existingBubble) {
                existingBubble.querySelector('div:first-child').textContent += data.text; 
                document.getElementById('chatMessages').scrollTop = document.getElementById('chatMessages').scrollHeight;
                if (!localStorage.getItem('token') && messageHistory.length > 0 && messageHistory[messageHistory.length - 1].role === 'assistant') {
                    messageHistory[messageHistory.length - 1].content += data.text; localStorage.setItem('guestMessageHistory', JSON.stringify(messageHistory));
                }
            } else addMessage('assistant', data.text, false, msgId);
        }
    }

    if (data.type === 'delete_message') removePendingBubble();
    if (data.type === 'audio_chunk') playPCM24kHz(data.audio_base64);
    if (data.type === 'notification') showNotification(data.message, data.level || 'info');
}

document.getElementById('clear-history').addEventListener('click', async function() { clearHistory(); });
async function clearHistory(){
    document.getElementById('chatMessages').innerHTML = '';
    const token = localStorage.getItem('token');
    if (token) {
        try { const r = await fetch('/clear_history', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ token: token }) }); const d = await r.json(); showNotification(d.message, d.status); } catch (e) { showNotification('Ошибка', 'error'); }
    } else { messageHistory = []; localStorage.removeItem('guestMessageHistory'); showNotification('История очищена', 'info'); }
}

function connectWebSocket() {
    const token = localStorage.getItem('token'); const userLogin = localStorage.getItem('userLogin');
    if (!token) return;
    if (websocketConnection) websocketConnection.close();
    websocketConnection = new WebSocket('wss://friday-assistant.ru/ws');
    websocketConnection.onopen = function() {
        websocketConnection.send(btoa(unescape(encodeURIComponent(JSON.stringify({ type: 'web_client_auth', token: token, login: userLogin })))));
        pingInterval = setInterval(() => { if (websocketConnection.readyState === WebSocket.OPEN) websocketConnection.send(btoa(unescape(encodeURIComponent(JSON.stringify({ type: 'ping', timestamp: Date.now() }))))); }, 30000);
    };
    websocketConnection.onmessage = function(event) {
        try {
            const data = JSON.parse(decodeURIComponent(escape(atob(event.data))));
            if (data.status === 'success' && data.history && Array.isArray(data.history)) {
                const chatMessages = document.getElementById('chatMessages'); chatMessages.innerHTML = '';
                data.history.forEach(msg => {
                    if (msg.sender === 'Вы') addMessage('user', msg.text, true, msg.id);
                    else {
                        let displayText = ""; msg.text.split('⸵').forEach(action => { const sep = action.indexOf('|'); displayText += (sep !== -1 ? action.substring(sep + 1).trim() : action) + '\n\n'; });
                        if (displayText.trim()) addMessage('assistant', displayText.trim(), true, msg.id);
                    }
                });
            } else handleIncomingStreamData(data);
        } catch (error) { }
    };
    websocketConnection.onclose = function() { if (pingInterval) { clearInterval(pingInterval); pingInterval = null; } setTimeout(() => { if (localStorage.getItem('token')) connectWebSocket(); }, 5000); };
}

function showNotification(msg, type = 'success') { const n = document.getElementById('notification'); document.getElementById('notification-text').textContent = msg; n.className = `notification ${type} show`; setTimeout(() => n.classList.remove('show'), 5000); }
function openRegisterModal() { document.getElementById('registerModal').style.display = 'flex'; document.body.style.overflow = 'hidden'; }
function openLoginModal() { document.getElementById('loginModal').style.display = 'flex'; document.body.style.overflow = 'hidden'; }
function closeModals() { document.getElementById('registerModal').style.display = 'none'; document.getElementById('loginModal').style.display = 'none'; document.getElementById('recoveryModal').style.display = 'none'; document.body.style.overflow = 'auto'; }

function updateAuthUI() {
    const ab = document.querySelector('.auth-buttons'); if (!ab) return;
    if (userLogin) { ab.innerHTML = `<div class="user-info"><span class="user-login">${userLogin}</span><button class="auth-btn logout-btn">Выйти</button></div>`; document.querySelector('.logout-btn').addEventListener('click', logout); } 
    else { ab.innerHTML = `<button class="auth-btn register-btn">Регистрация</button><button class="auth-btn login-btn">Вход</button>`; document.querySelector('.register-btn').addEventListener('click', openRegisterModal); document.querySelector('.login-btn').addEventListener('click', openLoginModal); }
}

async function verifyToken() {
    const token = localStorage.getItem('token'); const savedLogin = localStorage.getItem('userLogin'); if (!token || !savedLogin) return false;
    try { const r = await fetch(`/verify_token?token=${encodeURIComponent(token)}`); const d = await r.json(); if (r.ok && d.status === 'success') { userLogin = d.user_login; return true; } else { localStorage.removeItem('token'); localStorage.removeItem('userLogin'); userLogin = null; return false; } } catch (e) { return false; }
}

async function logout() {
    try {
        const token = localStorage.getItem('token');
        if (websocketConnection) { websocketConnection.close(); websocketConnection = null; }
        if (pingInterval) { clearInterval(pingInterval); pingInterval = null; }
        if (token) { try { await fetch('/logout_web', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ token: token }) }); } catch (e) {} }
        localStorage.removeItem('token'); localStorage.removeItem('userLogin'); userLogin = null;
        updateAuthUI(); messageHistory = []; localStorage.removeItem('guestMessageHistory'); document.getElementById('chatMessages').innerHTML = ''; showNotification('Вы вышли из системы', 'success');
    } catch (e) { showNotification('Ошибка выхода', 'error'); }
}

function saveSettingsToLocalStorage() { localStorage.setItem('voiceType', document.getElementById('voice-type').value); }
function loadSettingsFromLocalStorage() { const savedVoiceType = localStorage.getItem('voiceType'); if (savedVoiceType) document.getElementById('voice-type').value = savedVoiceType; }

document.addEventListener('DOMContentLoaded', async function() {
    loadSettingsFromLocalStorage();
    const sh = localStorage.getItem('guestMessageHistory');
    if (sh && !localStorage.getItem('token')) { try { messageHistory = JSON.parse(sh); if (messageHistory.length > 0) { document.getElementById('chatMessages').innerHTML = ''; messageHistory.forEach((msg, idx) => addMessage(msg.role, msg.content, true, 'guest_' + idx)); } } catch (e) { messageHistory = []; } }
    initStopWordDetection();

    if (await verifyToken()) { updateAuthUI(); connectWebSocket(); } else updateAuthUI();

    document.getElementById('voice-type').addEventListener('change', saveSettingsToLocalStorage);
    document.getElementById('messageInput').addEventListener('input', function() { this.style.height = 'auto'; this.style.height = (this.scrollHeight) + 'px'; });

    let micAudioContext = null; let audioWorkletNode = null; let micStream = null;
    let pcmBuffer = []; let preBuffer = []; let silenceFrames = 0;
    const VAD_THRESHOLD = 0.015; const SILENCE_FRAMES = 16000 * 1.5; const PRE_BUFFER_FRAMES = 8000; 

    const workletCode = `class PCMProcessor extends AudioWorkletProcessor { process(inputs, outputs, parameters) { const input = inputs[0]; if (input && input.length > 0) { const channelData = input[0]; const pcm16 = new Int16Array(channelData.length); let sum = 0; for (let i = 0; i < channelData.length; i++) { let s = Math.max(-1, Math.min(1, channelData[i])); pcm16[i] = s < 0 ? s * 0x8000 : s * 0x7FFF; sum += Math.abs(s); } this.port.postMessage({ pcm: pcm16, volume: sum / channelData.length }); } return true; } } registerProcessor('pcm-processor', PCMProcessor);`;

    function bufferToBase64(buffer) { let binary = ''; const bytes = new Uint8Array(buffer); const chunkSize = 0x8000; for (let i = 0; i < bytes.length; i += chunkSize) { binary += String.fromCharCode.apply(null, bytes.subarray(i, i + chunkSize)); } return window.btoa(binary); }

    function sendStreamChunk() {
        if (pcmBuffer.length === 0 || !activeStreamMsgId || !websocketConnection || websocketConnection.readyState !== WebSocket.OPEN) return;
        const pcm16 = new Int16Array(pcmBuffer); pcmBuffer = [];
        websocketConnection.send(btoa(unescape(encodeURIComponent(JSON.stringify({ type: 'audio_stream_chunk', audio_base64: bufferToBase64(pcm16.buffer), ui_msg_id: activeStreamMsgId })))));
    }

    async function startMicStream() {
        try {
            micStream = await navigator.mediaDevices.getUserMedia({ audio: { echoCancellation: true, noiseSuppression: true } });
        } catch (e) {
            // Если строгие параметры не подошли (привет, iOS), просим просто ЛЮБОЙ микрофон
            micStream = await navigator.mediaDevices.getUserMedia({ audio: true });
        }
        micAudioContext = new (window.AudioContext || window.webkitAudioContext)({ sampleRate: 16000 });
        await micAudioContext.audioWorklet.addModule(URL.createObjectURL(new Blob([workletCode], { type: 'application/javascript' })));
        const source = micAudioContext.createMediaStreamSource(micStream);
        audioWorkletNode = new AudioWorkletNode(micAudioContext, 'pcm-processor');
        const zeroGain = micAudioContext.createGain(); zeroGain.gain.value = 0;
        source.connect(audioWorkletNode); audioWorkletNode.connect(zeroGain); zeroGain.connect(micAudioContext.destination);

        audioWorkletNode.port.onmessage = (e) => {
            if (!isMicrophoneActive || isPlaying || vadState === 'processing') return; 
            const { pcm, volume } = e.data;
            const useRealtime = (websocketConnection && websocketConnection.readyState === WebSocket.OPEN);

            if (vadState === 'idle') {
                preBuffer.push(...pcm);
                if (preBuffer.length > PRE_BUFFER_FRAMES) preBuffer.splice(0, preBuffer.length - PRE_BUFFER_FRAMES);
                if (volume > VAD_THRESHOLD) {
                    vadState = 'recording'; pcmBuffer = [...preBuffer]; silenceFrames = 0;
                    activeStreamMsgId = Date.now().toString();
                    addMessage('user', '🎤 [Слушаю...]', false, activeStreamMsgId);
                    
                    if (useRealtime) {
                        sendToServer("", "голосовое сообщение", null, activeStreamMsgId, true);
                        chunkInterval = setInterval(sendStreamChunk, 250);
                    }
                }
            } else if (vadState === 'recording') {
                pcmBuffer.push(...pcm);
                if (volume < VAD_THRESHOLD) {
                    silenceFrames += pcm.length;
                    if (silenceFrames > SILENCE_FRAMES) {
                        vadState = 'processing';
                        if (useRealtime) {
                            clearInterval(chunkInterval); sendStreamChunk(); 
                            websocketConnection.send(btoa(unescape(encodeURIComponent(JSON.stringify({ type: 'audio_stream_end', ui_msg_id: activeStreamMsgId })))));
                            updatePendingBubble('⏳ Транскрибирую...'); activeStreamMsgId = null; preBuffer = [];
                        } else {
                            updatePendingBubble('⏳ Транскрибирую...');
                            const pcm16 = new Int16Array(pcmBuffer); pcmBuffer = []; preBuffer = [];
                            sendToServer("", "голосовое сообщение", bufferToBase64(pcm16.buffer), activeStreamMsgId, false);
                            activeStreamMsgId = null;
                        }
                    }
                } else silenceFrames = 0;
            }
        };
    }
    
    function stopMicStream() {
        if (chunkInterval) clearInterval(chunkInterval);
        if (audioWorkletNode) { audioWorkletNode.disconnect(); audioWorkletNode = null; }
        if (micStream) { micStream.getTracks().forEach(track => track.stop()); micStream = null; }
        if (micAudioContext) { micAudioContext.close(); micAudioContext = null; }
    }

    let isMicToggling = false;
    document.getElementById('microphone-btn').addEventListener('click', async function(e) {
        e.preventDefault(); if (isMicToggling) return; isMicToggling = true;
        
        try {
            if (!isMicrophoneActive) {
                await startMicStream(); isMicrophoneActive = true; vadState = 'idle';
                this.classList.add('active'); this.querySelector('span').textContent = 'Микрофон включен';
            } else {
                isMicrophoneActive = false; this.classList.remove('active'); this.querySelector('span').textContent = 'Включить микрофон';
                if (vadState === 'recording') {
                    vadState = 'processing';
                    if (websocketConnection && websocketConnection.readyState === WebSocket.OPEN) {
                        clearInterval(chunkInterval); sendStreamChunk();
                        websocketConnection.send(btoa(unescape(encodeURIComponent(JSON.stringify({ type: 'audio_stream_end', ui_msg_id: activeStreamMsgId })))));
                    } else {
                        const pcm16 = new Int16Array(pcmBuffer); sendToServer("", "голосовое сообщение", bufferToBase64(pcm16.buffer), activeStreamMsgId, false);
                    }
                    updatePendingBubble('⏳ Транскрибирую...');
                } else if (vadState === 'idle') { removePendingBubble(); }
                pcmBuffer = []; preBuffer = []; activeStreamMsgId = null; stopMicStream();
            }
        } catch (err) { showNotification('Ошибка микрофона', 'error'); } 
        finally { setTimeout(() => isMicToggling = false, 300); }
    });

    async function sendToServer(prompt, command_type, audio_base64 = null, ui_msg_id = null, stream_audio = false) {
        try {
            const token = localStorage.getItem('token'); const selectedVoice = document.getElementById('voice-type').value;
            const finalUiMsgId = ui_msg_id || Date.now().toString();
            
            if (token && websocketConnection && websocketConnection.readyState === WebSocket.OPEN) {
                const requestData = { type: 'web_command', command: prompt, audio_base64: audio_base64, token: token, timestamp: new Date().toISOString(), name: "Пятница", voice_type: selectedVoice, command_type: command_type, ui_msg_id: finalUiMsgId, stream_audio: stream_audio };
                if (currentFile) { const r = new FileReader(); r.onload = function() { requestData.screenshot = r.result.split(',')[1]; websocketConnection.send(btoa(unescape(encodeURIComponent(JSON.stringify(requestData))))); currentFile = null; document.getElementById('imagePreviewContainer').style.display = 'none'; document.getElementById('file-upload').value = ''; }; r.readAsDataURL(currentFile); } 
                else websocketConnection.send(btoa(unescape(encodeURIComponent(JSON.stringify(requestData)))));
            } else {
                const requestData = { prompt: prompt, audio_base64: audio_base64, bot_name: "Пятница", voice_type: selectedVoice, command_type: command_type, ui_msg_id: finalUiMsgId };
                if (!token) requestData.message_history = messageHistory;
                if (currentFile) { const r = new FileReader(); r.onload = function() { requestData.screenshot = r.result.split(',')[1]; sendFetchRequest(requestData); }; r.readAsDataURL(currentFile); } else sendFetchRequest(requestData);
            }
        } catch (error) { showNotification('Ошибка', 'error'); vadState = 'idle'; removePendingBubble(); }
    }

    async function sendFetchRequest(requestData) {
        try {
            const response = await fetch('/generate', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(requestData) });
            if (!response.ok) throw new Error();
            const reader = response.body.getReader(); const decoder = new TextDecoder("utf-8"); let buffer = "";
            while (true) {
                const { done, value } = await reader.read(); if (done) break;
                buffer += decoder.decode(value, { stream: true }); let msgs = buffer.split('\n\n'); buffer = msgs.pop();
                for (let msg of msgs) { if (msg.startsWith('data: ')) { try { handleIncomingStreamData(JSON.parse(msg.substring(6))); } catch(e) {} } }
            }
            currentFile = null; document.getElementById('imagePreviewContainer').style.display = 'none'; document.getElementById('file-upload').value = '';
        } catch (error) { vadState = 'idle'; removePendingBubble(); }
    }

    document.getElementById('sendMessage').addEventListener('click', function() {
        const message = document.getElementById('messageInput').value.trim(); if (!message && !currentFile) return;
        document.getElementById('messageInput').style.height = 'auto'; const uiMsgId = Date.now().toString();
        if (message) addMessage('user', message, false, uiMsgId); document.getElementById('messageInput').value = ''; document.getElementById('imagePreviewContainer').style.display = 'none';
        sendToServer(message, "текстовое сообщение", null, uiMsgId, false);
    });
                
    document.getElementById('messageInput').addEventListener('keydown', function(e) { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); document.getElementById('sendMessage').click(); } });
    document.getElementById('file-upload').addEventListener('change', function(e) {
        const f = e.target.files[0]; const c = document.getElementById('imagePreviewContainer'); c.innerHTML = ''; c.style.display = 'none'; currentFile = null; 
        if (f) {
            if (!f.type.match('image/png') && !f.type.match('image/jpeg')) { showNotification('Только PNG и JPG', 'error'); this.value = ''; return; }
            currentFile = f; const r = new FileReader();
            r.onload = function(e) { const img = document.createElement('img'); img.src = e.target.result; img.className = 'image-preview'; const btn = document.createElement('button'); btn.className = 'remove-image'; btn.innerHTML = '&times;'; btn.onclick = function() { c.style.display = 'none'; document.getElementById('file-upload').value = ''; currentFile = null; }; c.appendChild(img); c.appendChild(btn); c.style.display = 'block'; };
            r.readAsDataURL(f);
        }
    });

    document.querySelectorAll('.modal-close').forEach(btn => btn.addEventListener('click', closeModals));
    window.addEventListener('click', function(e) { if (e.target === document.getElementById('registerModal') || e.target === document.getElementById('loginModal') || e.target === document.getElementById('recoveryModal')) closeModals(); });

    document.getElementById('registerForm').addEventListener('submit', async function(e) {
        e.preventDefault(); const em = document.getElementById('regEmail').value; const lo = document.getElementById('regLogin').value; const pw = document.getElementById('regPassword').value; const pwc = document.getElementById('regPasswordConfirm').value; const re = document.getElementById('registerResponse'); const sb = this.querySelector('button[type="submit"]');
        if (pw !== pwc) { re.textContent = 'Пароли не совпадают!'; re.className = 'response-message error'; return; }
        try { sb.disabled = true; sb.textContent = 'Регистрация...'; const r = await fetch('/register', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ email: em, login: lo, password: pw }) }); const d = await r.json(); if (r.ok) { re.textContent = d.message; re.className = 'response-message success'; setTimeout(() => { this.reset(); re.className = 'response-message'; closeModals(); }, 2000); } else { re.textContent = d.message; re.className = 'response-message error'; } } catch (error) { re.textContent = 'Ошибка'; re.className = 'response-message error'; } finally { sb.disabled = false; sb.textContent = 'Зарегистрироваться'; }
    });

    document.getElementById('loginForm').addEventListener('submit', async function(e) {
        e.preventDefault(); const lo = document.getElementById('loginEmail').value; const pw = document.getElementById('loginPassword').value; const re = document.getElementById('loginResponse'); const sb = this.querySelector('button[type="submit"]');
        try { sb.disabled = true; sb.textContent = 'Вход...'; const r = await fetch('/login_web', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ login: lo, password: pw }) }); const d = await r.json(); if (r.ok) { re.textContent = d.message; re.className = 'response-message success'; userLogin = d.user_login; localStorage.setItem('token', d.token); localStorage.setItem('userLogin', d.user_login); messageHistory = []; localStorage.removeItem('guestMessageHistory'); updateAuthUI(); connectWebSocket(); setTimeout(() => { this.reset(); re.className = 'response-message'; closeModals(); }, 2000); } else { re.textContent = d.message; re.className = 'response-message error'; } } catch (error) { re.textContent = 'Ошибка'; re.className = 'response-message error'; } finally { sb.disabled = false; sb.textContent = 'Войти'; }
    });
            
    document.getElementById('forgotPassword').addEventListener('click', function(e) { e.preventDefault(); closeModals(); document.getElementById('recoveryModal').style.display = 'flex'; });
    document.querySelector('.recovery-close').addEventListener('click', function() { document.getElementById('recoveryModal').style.display = 'none'; });
            
    document.getElementById('recoveryForm').addEventListener('submit', async function(e) {
        e.preventDefault(); const em = document.getElementById('recoveryEmail').value; const re = document.getElementById('recoveryResponse'); const sb = this.querySelector('button[type="submit"]');
        try { sb.disabled = true; sb.textContent = 'Отправка...'; const r = await fetch('/recover-password', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ email: em }) }); const d = await r.json(); if (r.ok) { re.textContent = d.message; re.className = 'response-message success'; setTimeout(() => { document.getElementById('recoveryModal').style.display = 'none'; this.reset(); re.className = 'response-message'; }, 3000); } else { re.textContent = d.message; re.className = 'response-message error'; } } catch (error) { re.textContent = 'Ошибка'; re.className = 'response-message error'; } finally { sb.disabled = false; sb.textContent = 'Восстановить пароль'; }
    });
});