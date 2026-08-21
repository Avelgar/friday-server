// ==========================================
// app.js - Глобальное состояние и Сеть
// ==========================================

// --- ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ---
let currentDialogId = null;
let userLogin = null;
let websocketConnection = null;
let pingInterval = null;
let currentFile = null;
let messageHistory = []; 
let pendingBubbleId = null;
let activeStreamMsgId = null;
let chunkInterval = null;
let ignoredMessageId = null; 

// Переменные Аудио/Видео
let streamAudioContext = null;
let nextPlayTime = 0;
let isPlaying = false; 
let vadState = 'idle'; 
let isMicrophoneActive = false; 
let lastAudioStopTime = 0; 
let stopWordRecognizer = null;
const stopWords = ['стоп', 'хватит', 'остановись', 'перестань', 'замолчи'];
let videoStream = null;
let currentVideoSource = null; 
let videoInterval = null;

let micAudioContext = null; 
let audioWorkletNode = null; 
let micStream = null;
let pcmBuffer = []; 
let preBuffer = []; 
let silenceFrames = 0;

// --- ФУНКЦИИ СЕТИ И АВТОРИЗАЦИИ ---
function connectWebSocket() {
    const token = localStorage.getItem('token'); 
    if (!token) return;
    if (websocketConnection) websocketConnection.close();
    
    websocketConnection = new WebSocket('wss://friday-assistant.ru/ws');
    websocketConnection.onopen = function() {
        websocketConnection.send(btoa(unescape(encodeURIComponent(JSON.stringify({ type: 'web_client_auth', token: token, login: userLogin })))));
        pingInterval = setInterval(() => { 
            if (websocketConnection.readyState === WebSocket.OPEN) 
                websocketConnection.send(btoa(unescape(encodeURIComponent(JSON.stringify({ type: 'ping', timestamp: Date.now() }))))); 
        }, 30000);
    };
    websocketConnection.onmessage = function(event) {
        try {
            const data = JSON.parse(decodeURIComponent(escape(atob(event.data))));
            if (data.status === 'success' && data.message === "Данные успешно обработаны!") {
                console.log("WebSocket Auth OK");
            } else { 
                handleIncomingStreamData(data); // Функция из ui.js
            }
        } catch (error) { }
    };
    websocketConnection.onclose = function() { 
        if (pingInterval) { clearInterval(pingInterval); pingInterval = null; } 
        setTimeout(() => { if (localStorage.getItem('token')) connectWebSocket(); }, 5000); 
    };
}

async function verifyToken() {
    const token = localStorage.getItem('token'); 
    const savedLogin = localStorage.getItem('userLogin'); 
    if (!token || !savedLogin) return false;
    try { 
        const r = await fetch(`/verify_token?token=${encodeURIComponent(token)}`); 
        const d = await r.json(); 
        if (r.ok && d.status === 'success') { userLogin = d.user_login; return true; } 
        else { localStorage.removeItem('token'); localStorage.removeItem('userLogin'); userLogin = null; return false; } 
    } catch (e) { return false; }
}

async function logout() {
    try {
        const token = localStorage.getItem('token');
        if (websocketConnection) { websocketConnection.close(); websocketConnection = null; }
        if (pingInterval) { clearInterval(pingInterval); pingInterval = null; }
        if (token) { try { await fetch('/logout_web', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ token: token }) }); } catch (e) {} }
        
        localStorage.removeItem('token'); localStorage.removeItem('userLogin'); userLogin = null;
        currentDialogId = null;
        
        updateAuthUI(); 
        messageHistory = []; localStorage.removeItem('guestMessageHistory'); 
        document.getElementById('chatMessages').innerHTML = ''; 
        document.getElementById('dialogList').innerHTML = '<div class="dialog-item active" data-id="local">Гостевой диалог</div>';
        showNotification('Вы вышли из системы', 'success');
    } catch (e) { showNotification('Ошибка выхода', 'error'); }
}

async function sendToServer(prompt, command_type, audio_base64 = null, ui_msg_id = null, stream_audio = false) {
    try {
        const token = localStorage.getItem('token'); 
        const selectedVoice = document.getElementById('voice-type').value;
        const finalUiMsgId = ui_msg_id || Date.now().toString();
        let frameBase64 = currentVideoSource ? captureSingleFrame() : null; // Из media.js
        
        if (token && websocketConnection && websocketConnection.readyState === WebSocket.OPEN) {
            const requestData = { 
                type: 'web_command', command: prompt, audio_base64: audio_base64, 
                token: token, timestamp: new Date().toISOString(), name: "Пятница", 
                voice_type: selectedVoice, command_type: command_type, ui_msg_id: finalUiMsgId, 
                stream_audio: stream_audio, dialog_id: currentDialogId
            };
            
            if (currentFile) { 
                const r = new FileReader(); 
                r.onload = function() { 
                    requestData.screenshot = r.result.split(',')[1]; 
                    websocketConnection.send(btoa(unescape(encodeURIComponent(JSON.stringify(requestData))))); 
                    currentFile = null; document.getElementById('imagePreviewContainer').style.display = 'none'; document.getElementById('file-upload').value = ''; 
                }; 
                r.readAsDataURL(currentFile); 
            } else {
                if (!stream_audio && frameBase64) requestData.screenshot = frameBase64;
                websocketConnection.send(btoa(unescape(encodeURIComponent(JSON.stringify(requestData)))));
            }
        } else {
            const requestData = { 
                prompt: prompt, audio_base64: audio_base64, bot_name: "Пятница", 
                voice_type: selectedVoice, command_type: command_type, ui_msg_id: finalUiMsgId,
                dialog_id: currentDialogId, token: token 
            };
            if (!token) requestData.message_history = messageHistory; 
            
            if (currentFile) { 
                const r = new FileReader(); 
                r.onload = function() { 
                    requestData.screenshot = r.result.split(',')[1]; 
                    sendFetchRequest(requestData); 
                }; 
                r.readAsDataURL(currentFile); 
            } else {
                if (frameBase64) requestData.screenshot = frameBase64;
                sendFetchRequest(requestData);
            }
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

// --- ИНИЦИАЛИЗАЦИЯ И СЛУШАТЕЛИ ---
document.addEventListener('DOMContentLoaded', async function() {
    const savedVoiceType = localStorage.getItem('voiceType'); 
    if (savedVoiceType) document.getElementById('voice-type').value = savedVoiceType;
    document.getElementById('voice-type').addEventListener('change', () => localStorage.setItem('voiceType', document.getElementById('voice-type').value));

    document.getElementById('newChatBtn').addEventListener('click', () => createNewDialog());

    const sh = localStorage.getItem('guestMessageHistory');
    if (sh && !localStorage.getItem('token')) { 
        try { 
            messageHistory = JSON.parse(sh); 
            if (messageHistory.length > 0) { 
                document.getElementById('chatMessages').innerHTML = ''; 
                messageHistory.forEach((msg, idx) => addMessage(msg.role, msg.content, true, 'guest_' + idx)); 
            } 
        } catch (e) { messageHistory = []; } 
    }
    
    initStopWordDetection(); // из media.js

    if (await verifyToken()) { 
        updateAuthUI(); connectWebSocket(); loadDialogs(); 
    } else { 
        updateAuthUI(); document.getElementById('dialogList').innerHTML = '<div class="dialog-item active" data-id="local">Гостевой диалог</div>';
    }

    document.getElementById('messageInput').addEventListener('input', function() { this.style.height = 'auto'; this.style.height = (this.scrollHeight) + 'px'; });
    
    document.getElementById('sendMessage').addEventListener('click', function() {
        const message = document.getElementById('messageInput').value.trim(); 
        if (!message && !currentFile && !currentVideoSource) return;
        document.getElementById('messageInput').style.height = 'auto'; const uiMsgId = Date.now().toString();
        if (message) addMessage('user', message, false, uiMsgId); 
        document.getElementById('messageInput').value = ''; document.getElementById('imagePreviewContainer').style.display = 'none';
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

    // Обработчики модальных окон
    document.querySelectorAll('.modal-close').forEach(btn => btn.addEventListener('click', closeModals));
    window.addEventListener('click', function(e) { if (e.target === document.getElementById('registerModal') || e.target === document.getElementById('loginModal') || e.target === document.getElementById('recoveryModal')) closeModals(); });

    document.getElementById('registerForm').addEventListener('submit', async function(e) {
        e.preventDefault(); const em = document.getElementById('regEmail').value; const lo = document.getElementById('regLogin').value; const pw = document.getElementById('regPassword').value; const pwc = document.getElementById('regPasswordConfirm').value; const re = document.getElementById('registerResponse'); const sb = this.querySelector('button[type="submit"]');
        if (pw !== pwc) { re.textContent = 'Пароли не совпадают!'; re.className = 'response-message error'; return; }
        try { sb.disabled = true; sb.textContent = 'Регистрация...'; const r = await fetch('/register', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ email: em, login: lo, password: pw }) }); const d = await r.json(); if (r.ok) { re.textContent = d.message; re.className = 'response-message success'; setTimeout(() => { this.reset(); re.className = 'response-message'; closeModals(); }, 2000); } else { re.textContent = d.message; re.className = 'response-message error'; } } catch (error) { re.textContent = 'Ошибка'; re.className = 'response-message error'; } finally { sb.disabled = false; sb.textContent = 'Зарегистрироваться'; }
    });

    document.getElementById('loginForm').addEventListener('submit', async function(e) {
        e.preventDefault(); const lo = document.getElementById('loginEmail').value; const pw = document.getElementById('loginPassword').value; const re = document.getElementById('loginResponse'); const sb = this.querySelector('button[type="submit"]');
        try { 
            sb.disabled = true; sb.textContent = 'Вход...'; 
            const r = await fetch('/login_web', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ login: lo, password: pw }) }); 
            const d = await r.json(); 
            if (r.ok) { 
                re.textContent = d.message; re.className = 'response-message success'; 
                userLogin = d.user_login; localStorage.setItem('token', d.token); localStorage.setItem('userLogin', d.user_login); 
                messageHistory = []; localStorage.removeItem('guestMessageHistory'); 
                updateAuthUI(); connectWebSocket(); await loadDialogs();
                setTimeout(() => { this.reset(); re.className = 'response-message'; closeModals(); }, 2000); 
            } else { re.textContent = d.message; re.className = 'response-message error'; } 
        } catch (error) { re.textContent = 'Ошибка'; re.className = 'response-message error'; } finally { sb.disabled = false; sb.textContent = 'Войти'; }
    });
            
    document.getElementById('forgotPassword').addEventListener('click', function(e) { e.preventDefault(); closeModals(); document.getElementById('recoveryModal').style.display = 'flex'; });
    document.querySelector('.recovery-close').addEventListener('click', function() { document.getElementById('recoveryModal').style.display = 'none'; });
            
    document.getElementById('recoveryForm').addEventListener('submit', async function(e) {
        e.preventDefault(); const em = document.getElementById('recoveryEmail').value; const re = document.getElementById('recoveryResponse'); const sb = this.querySelector('button[type="submit"]');
        try { sb.disabled = true; sb.textContent = 'Отправка...'; const r = await fetch('/recover-password', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ email: em }) }); const d = await r.json(); if (r.ok) { re.textContent = d.message; re.className = 'response-message success'; setTimeout(() => { document.getElementById('recoveryModal').style.display = 'none'; this.reset(); re.className = 'response-message'; }, 3000); } else { re.textContent = d.message; re.className = 'response-message error'; } } catch (error) { re.textContent = 'Ошибка'; re.className = 'response-message error'; } finally { sb.disabled = false; sb.textContent = 'Восстановить пароль'; }
    });
});