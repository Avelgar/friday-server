// ==========================================
// ui.js - Интерфейс, Чат и Уведомления
// ==========================================

function showNotification(msg, type = 'success') { 
    const n = document.getElementById('notification'); 
    document.getElementById('notification-text').textContent = msg; 
    n.className = `notification ${type} show`; 
    setTimeout(() => n.classList.remove('show'), 5000); 
}

function openRegisterModal() { document.getElementById('registerModal').style.display = 'flex'; document.body.style.overflow = 'hidden'; }
function openLoginModal() { document.getElementById('loginModal').style.display = 'flex'; document.body.style.overflow = 'hidden'; }
function closeModals() { document.getElementById('registerModal').style.display = 'none'; document.getElementById('loginModal').style.display = 'none'; document.getElementById('recoveryModal').style.display = 'none'; document.body.style.overflow = 'auto'; }

function updateAuthUI() {
    const ab = document.querySelector('.auth-buttons'); if (!ab) return;
    if (userLogin) { 
        ab.innerHTML = `<div class="user-info"><span class="user-login">${userLogin}</span><button class="auth-btn logout-btn">Выйти</button></div>`; 
        document.querySelector('.logout-btn').addEventListener('click', logout); 
        document.getElementById('clear-history').style.display = 'none'; 
    } else { 
        ab.innerHTML = `<button class="auth-btn register-btn">Регистрация</button><button class="auth-btn login-btn">Вход</button>`; 
        document.querySelector('.register-btn').addEventListener('click', openRegisterModal); 
        document.querySelector('.login-btn').addEventListener('click', openLoginModal); 
        document.getElementById('clear-history').style.display = 'flex'; 
    }
}

async function loadDialogs() {
    const token = localStorage.getItem('token');
    if (!token) { document.getElementById('dialogList').innerHTML = '<div class="dialog-item active" data-id="local">Гостевой диалог</div>'; return; }
    try {
        const response = await fetch('/api/get_dialogs', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ token: token }) });
        const data = await response.json();
        if (data.status === 'success') {
            renderDialogs(data.dialogs);
            if (data.dialogs.length > 0 && currentDialogId === null) selectDialog(data.dialogs[0].id);
        }
    } catch (e) { console.error("Ошибка загрузки диалогов:", e); }
}

function renderDialogs(dialogs) {
    const list = document.getElementById('dialogList'); list.innerHTML = '';
    dialogs.forEach(d => {
        const div = document.createElement('div');
        div.className = `dialog-item ${d.id === currentDialogId ? 'active' : ''}`;
        div.dataset.id = d.id;
        div.style.display = 'flex'; div.style.justifyContent = 'space-between'; div.style.alignItems = 'center';
        
        const nameSpan = document.createElement('span');
        nameSpan.textContent = d.name; nameSpan.style.flex = '1'; nameSpan.style.overflow = 'hidden'; nameSpan.style.textOverflow = 'ellipsis';
        nameSpan.onclick = () => selectDialog(d.id);
        
        const delBtn = document.createElement('i'); delBtn.className = 'fas fa-trash'; delBtn.style.color = '#aaa'; delBtn.style.cursor = 'pointer'; delBtn.style.marginLeft = '10px'; delBtn.title = "Удалить чат";
        delBtn.onmouseover = () => delBtn.style.color = '#e74c3c'; delBtn.onmouseout = () => delBtn.style.color = '#aaa';
        delBtn.onclick = (e) => { e.stopPropagation(); if(confirm('Удалить диалог?')) deleteDialog(d.id); };
        
        div.appendChild(nameSpan); div.appendChild(delBtn); list.appendChild(div);
    });
}

async function selectDialog(dialogId) {
    currentDialogId = dialogId;
    document.querySelectorAll('.dialog-item').forEach(el => el.classList.remove('active'));
    const activeEl = document.querySelector(`.dialog-item[data-id="${dialogId}"]`);
    if (activeEl) activeEl.classList.add('active');
    document.getElementById('sidebar').classList.remove('open');
    const token = localStorage.getItem('token'); if (!token) return;
    
    document.getElementById('chatMessages').innerHTML = ''; 
    try {
        const response = await fetch('/api/get_history', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ token: token, dialog_id: dialogId }) });
        const data = await response.json();
        if (data.status === 'success' && data.history) {
            data.history.forEach(msg => {
                if (msg.sender === 'Вы') addMessage('user', msg.text, true, msg.id);
                else {
                    let displayText = ""; 
                    msg.text.split('⸵').forEach(action => { const sep = action.indexOf('|'); displayText += (sep !== -1 ? action.substring(sep + 1).trim() : action) + '\n\n'; });
                    if (displayText.trim()) addMessage('assistant', displayText.trim(), true, msg.id);
                }
            });
        }
    } catch (e) {}
}

function createNewDialog() {
    if (!localStorage.getItem('token')) { openLoginModal(); return; }
    currentDialogId = null; document.getElementById('chatMessages').innerHTML = '';
    document.querySelectorAll('.dialog-item').forEach(el => el.classList.remove('active'));
}

async function deleteDialog(dialogId) {
    const token = localStorage.getItem('token'); if (!token) return;
    try {
        const response = await fetch('/api/delete_dialog', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ token: token, dialog_id: dialogId }) });
        const data = await response.json();
        if (data.status === 'success') {
            if (currentDialogId === dialogId) { currentDialogId = null; document.getElementById('chatMessages').innerHTML = ''; }
            await loadDialogs();
        } else { showNotification(data.message, 'error'); }
    } catch (e) { }
}

window.editMessage = async function(msgId) {
    const bubble = document.getElementById('msg_' + msgId); if (!bubble) return;
    const textDiv = bubble.querySelector('div:first-child'); const oldText = textDiv.textContent;
    const newText = prompt("Редактировать сообщение:", oldText);
    if (newText && newText.trim() !== "" && newText !== oldText) {
        textDiv.textContent = newText; const token = localStorage.getItem('token');
        if (token) { try { await fetch('/edit_message', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ token: token, msg_id: msgId, new_text: newText }) }); } catch (e) {} } 
        else { const msgObj = messageHistory.find(m => m.timestamp && m.content === oldText); if(msgObj) { msgObj.content = newText; localStorage.setItem('guestMessageHistory', JSON.stringify(messageHistory)); } }
    }
}

window.deleteMessage = async function(msgId) {
    const bubble = document.getElementById('msg_' + msgId); if (!bubble) return;
    const token = localStorage.getItem('token');
    if (token) { try { const resp = await fetch('/delete_message', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ token: token, msg_id: msgId }) }); if (resp.ok) bubble.remove(); } catch (e) { } } 
    else { const text = bubble.querySelector('div:first-child').textContent; messageHistory = messageHistory.filter(m => m.content !== text); localStorage.setItem('guestMessageHistory', JSON.stringify(messageHistory)); bubble.remove(); }
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
    chatMessages.appendChild(messageElement); chatMessages.scrollTop = chatMessages.scrollHeight;
    
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
    if (data.type === 'dialog_created') { currentDialogId = data.dialog_id; loadDialogs(); }
    if (data.type === 'dialog_renamed') { const chatSpan = document.querySelector(`.dialog-item[data-id="${data.dialog_id}"] span`); if (chatSpan) chatSpan.textContent = data.name; }

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
                for (let i = 0; i < select.options.length; i++) { if (select.options[i].value.toLowerCase() === String(action.action_value).toLowerCase()) { select.selectedIndex = i; localStorage.setItem('voiceType', document.getElementById('voice-type').value); break; } }
            }
        });
    }

    if (data.type === 'new_message') {
        const msgId = data.message_id || data.ui_msg_id;
        
        // Проверяем, является ли это сообщение финальным (пустой текст и нет действий)
        const isFinal = !data.text && (!data.actions || data.actions.length === 0);
        
        if (isFinal) {
            // === СНИМАЕМ ЖЕСТКУЮ БЛОКИРОВКУ МИКРОФОНА ===
            vadState = 'idle';
            // Если ИИ прислал аудио, ждем пока оно доиграет (playPCM24kHz сам снимет isPlaying)
            // Если аудио нет (только текст), снимаем блокировку принудительно прямо сейчас:
            if (!window.streamAudioContext || window.streamAudioContext.currentTime >= window.nextPlayTime) {
                isPlaying = false;
            }
        }

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

    if (data.type === 'delete_message') {
        removePendingBubble();
        // === СНИМАЕМ БЛОКИРОВКУ ДАЖЕ ЕСЛИ ПРОИЗОШЕЛ ТАЙМАУТ ===
        isPlaying = false; 
        vadState = 'idle';
    }

    if (data.type === 'audio_chunk') playPCM24kHz(data.audio_base64);
    if (data.type === 'notification') showNotification(data.message, data.level || 'info');
}

document.getElementById('clear-history').addEventListener('click', clearHistory);
async function clearHistory(){
    document.getElementById('chatMessages').innerHTML = '';
    const token = localStorage.getItem('token');
    if (token) {
        try { 
            const r = await fetch('/clear_history', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ token: token, dialog_id: currentDialogId }) }); 
            const d = await r.json(); showNotification(d.message, d.status); 
        } catch (e) { showNotification('Ошибка', 'error'); }
    } else { messageHistory = []; localStorage.removeItem('guestMessageHistory'); showNotification('История очищена', 'info'); }
}