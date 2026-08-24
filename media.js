// ==========================================
// media.js - Работа с Медиа (Камера, Микрофон, Звук)
// ==========================================

const VAD_THRESHOLD = 0.015; 
const SILENCE_FRAMES = 16000 * 1.5; 
const PRE_BUFFER_FRAMES = 8000; 
const workletCode = `class PCMProcessor extends AudioWorkletProcessor { process(inputs, outputs, parameters) { const input = inputs[0]; if (input && input.length > 0) { const channelData = input[0]; const pcm16 = new Int16Array(channelData.length); let sum = 0; for (let i = 0; i < channelData.length; i++) { let s = Math.max(-1, Math.min(1, channelData[i])); pcm16[i] = s < 0 ? s * 0x8000 : s * 0x7FFF; sum += Math.abs(s); } this.port.postMessage({ pcm: pcm16, volume: sum / channelData.length }); } return true; } } registerProcessor('pcm-processor', PCMProcessor);`;

// --- УПРАВЛЕНИЕ ВИДЕО И ЭКРАНОМ ---
const liveVideo = document.getElementById('live-video');
const videoPreviewContainer = document.getElementById('video-preview-container');
const hiddenCanvas = document.getElementById('hidden-canvas');
const ctx = hiddenCanvas.getContext('2d');
const cameraBtn = document.getElementById('camera-btn');
const screenBtn = document.getElementById('screen-btn');
const closeVideoBtn = document.getElementById('close-video-btn');

cameraBtn.addEventListener('click', () => toggleVideoSource('camera'));
screenBtn.addEventListener('click', () => toggleVideoSource('screen'));

if (closeVideoBtn) {
    closeVideoBtn.addEventListener('click', (e) => {
        e.stopPropagation();
        // Скрываем только визуальный блок для пользователя.
        // Сам видеопоток (videoStream) и отправка кадров ИИ продолжают работать!
        videoPreviewContainer.style.display = 'none';
    });
}

async function stopVideoStream() {
    if (videoStream) {
        videoStream.getTracks().forEach(track => track.stop());
        videoStream = null;
    }
    liveVideo.srcObject = null;
    videoPreviewContainer.style.display = 'none';
    currentVideoSource = null;
    cameraBtn.classList.remove('active');
    screenBtn.classList.remove('active');
    if (videoInterval) { clearInterval(videoInterval); videoInterval = null; }
}

async function toggleVideoSource(sourceType) {
    if (currentVideoSource === sourceType) {
        // Если блок был скрыт крестиком, повторный клик по кнопке покажет его снова
        if (videoPreviewContainer.style.display === 'none') {
            videoPreviewContainer.style.display = 'flex';
            return;
        }
        // Если уже был открыт — полностью выключаем камеру/экран
        await stopVideoStream();
        return;
    }
    await stopVideoStream();
    
    try {
        if (sourceType === 'camera') {
            videoStream = await navigator.mediaDevices.getUserMedia({ video: { width: 640, height: 480, frameRate: 15 } });
            cameraBtn.classList.add('active');
            liveVideo.style.transform = 'scaleX(-1)';
        } else if (sourceType === 'screen') {
            videoStream = await navigator.mediaDevices.getDisplayMedia({ video: { width: 1280, height: 720, frameRate: 15 } });
            screenBtn.classList.add('active');
            liveVideo.style.transform = 'none';
            videoStream.getVideoTracks()[0].addEventListener('ended', stopVideoStream);
        }
        currentVideoSource = sourceType;
        liveVideo.srcObject = videoStream;
        videoPreviewContainer.style.display = 'flex';
    } catch (err) {
        showNotification('Ошибка доступа к ' + (sourceType==='camera' ? 'камере' : 'экрану'), 'error');
        await stopVideoStream();
    }
}

function captureSingleFrame() {
    if (!currentVideoSource || !videoStream) return null;
    const w = liveVideo.videoWidth;
    const h = liveVideo.videoHeight;
    if (w === 0 || h === 0) return null;

    hiddenCanvas.width = w;
    hiddenCanvas.height = h;
    // Отрисовка на Canvas всегда берет реальные пиксели (не отзеркаленные). 
    // Это идеально: сервер получит правильную картинку.
    ctx.drawImage(liveVideo, 0, 0, w, h);
    return hiddenCanvas.toDataURL('image/jpeg', 0.5).split(',')[1];
}

function sendWSVideoFrame() {
    if (!currentVideoSource || !activeStreamMsgId) return;
    if (!websocketConnection || websocketConnection.readyState !== WebSocket.OPEN) return;

    const base64Image = captureSingleFrame();
    if (base64Image) {
        websocketConnection.send(btoa(unescape(encodeURIComponent(JSON.stringify({ 
            type: 'video_stream_chunk', 
            video_base64: base64Image, 
            ui_msg_id: activeStreamMsgId 
        })))));
    }
}

// --- ВОСПРОИЗВЕДЕНИЕ АУДИО ---
function initStopWordDetection() {
    if (!stopWordRecognizer && ('webkitSpeechRecognition' in window)) {
        stopWordRecognizer = new webkitSpeechRecognition();
        stopWordRecognizer.continuous = true; stopWordRecognizer.interimResults = true; stopWordRecognizer.lang = 'ru-RU';
        stopWordRecognizer.onresult = function(e) {
            if (!isPlaying) return;
            for (let i = e.resultIndex; i < e.results.length; i++) {
                if (stopWords.some(w => e.results[i][0].transcript.toLowerCase().includes(w))) { stopPlayback(); break; }
            }
        };
    }
}
function startStopWordDetection() { const isMobile = /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent); if (isMobile) return; if (stopWordRecognizer) { try { stopWordRecognizer.start(); } catch(e){} } }
function stopStopWordDetection() { if (stopWordRecognizer) { try { stopWordRecognizer.stop(); } catch(e){} } }
function stopPlayback() { if (streamAudioContext) { streamAudioContext.close(); streamAudioContext = null; nextPlayTime = 0; } stopStopWordDetection(); isPlaying = false; lastAudioStopTime = Date.now(); }

async function playPCM24kHz(base64Data) {
    if (!base64Data) return;
    try {
        if (!streamAudioContext) streamAudioContext = new (window.AudioContext || window.webkitAudioContext)({ sampleRate: 24000 });
        const binaryString = window.atob(base64Data); const bytes = new Uint8Array(binaryString.length);
        for (let i = 0; i < binaryString.length; i++) bytes[i] = binaryString.charCodeAt(i);
        const pcm16 = new Int16Array(bytes.buffer); const audioBuffer = streamAudioContext.createBuffer(1, pcm16.length, 24000);
        const channelData = audioBuffer.getChannelData(0); for (let i = 0; i < pcm16.length; i++) channelData[i] = pcm16[i] / 32768.0;
        
        const source = streamAudioContext.createBufferSource(); source.buffer = audioBuffer; source.connect(streamAudioContext.destination);
        if (nextPlayTime < streamAudioContext.currentTime) nextPlayTime = streamAudioContext.currentTime;
        isPlaying = true; if (isMicrophoneActive) startStopWordDetection();

        source.onended = () => { if (streamAudioContext && streamAudioContext.currentTime >= nextPlayTime - 0.1) { if (isPlaying) { isPlaying = false; lastAudioStopTime = Date.now(); stopStopWordDetection(); } } };
        source.start(nextPlayTime); nextPlayTime += audioBuffer.duration;
    } catch (e) { }
}

// --- МИКРОФОН И VAD ---
function bufferToBase64(buffer) { let binary = ''; const bytes = new Uint8Array(buffer); const chunkSize = 0x8000; for (let i = 0; i < bytes.length; i += chunkSize) binary += String.fromCharCode.apply(null, bytes.subarray(i, i + chunkSize)); return window.btoa(binary); }

function sendStreamChunk() {
    if (pcmBuffer.length === 0 || !activeStreamMsgId || !websocketConnection || websocketConnection.readyState !== WebSocket.OPEN) return;
    const pcm16 = new Int16Array(pcmBuffer); pcmBuffer = [];
    websocketConnection.send(btoa(unescape(encodeURIComponent(JSON.stringify({ type: 'audio_stream_chunk', audio_base64: bufferToBase64(pcm16.buffer), ui_msg_id: activeStreamMsgId })))));
}

async function startMicStream() {
    if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
        const oldGetUserMedia = navigator.getUserMedia || navigator.webkitGetUserMedia || navigator.mozGetUserMedia;
        if (oldGetUserMedia) navigator.mediaDevices = navigator.mediaDevices || {}; navigator.mediaDevices.getUserMedia = function(c) { return new Promise((res, rej) => oldGetUserMedia.call(navigator, c, res, rej)); };
    }
    try { micStream = await navigator.mediaDevices.getUserMedia({ audio: { echoCancellation: true, noiseSuppression: true } }); } 
    catch (e) { micStream = await navigator.mediaDevices.getUserMedia({ audio: true }); }

    micAudioContext = new (window.AudioContext || window.webkitAudioContext)({ sampleRate: 16000 });
    await micAudioContext.audioWorklet.addModule(URL.createObjectURL(new Blob([workletCode], { type: 'application/javascript' })));
    const source = micAudioContext.createMediaStreamSource(micStream);
    audioWorkletNode = new AudioWorkletNode(micAudioContext, 'pcm-processor');
    const zeroGain = micAudioContext.createGain(); zeroGain.gain.value = 0;
    source.connect(audioWorkletNode); audioWorkletNode.connect(zeroGain); zeroGain.connect(micAudioContext.destination);

    audioWorkletNode.port.onmessage = (e) => {
        if (!isMicrophoneActive || isPlaying || vadState === 'processing') return; 
        if (Date.now() - lastAudioStopTime < 600) return;

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
                    if (currentVideoSource && !videoInterval) videoInterval = setInterval(sendWSVideoFrame, 1000);
                }
            }
        } else if (vadState === 'recording') {
                pcmBuffer.push(...pcm);
                if (volume < VAD_THRESHOLD) {
                    silenceFrames += pcm.length;
                    if (silenceFrames > SILENCE_FRAMES) {
                        vadState = 'processing';
                        
                        // === ДОБАВЛЕНО: ЖЕСТКАЯ БЛОКИРОВКА ===
                        isPlaying = true; // Обманываем скрипт, чтобы микрофон игнорил звуки
                        // ===================================

                        if (videoInterval) { clearInterval(videoInterval); videoInterval = null; }
                        
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
    if (videoInterval) { clearInterval(videoInterval); videoInterval = null; }
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
                if (videoInterval) { clearInterval(videoInterval); videoInterval = null; }
                
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
    } catch (err) { 
        showNotification('Ошибка микрофона', 'error'); vadState = 'idle'; isMicrophoneActive = false;
        this.classList.remove('active'); this.querySelector('span').textContent = 'Включить микрофон';
    } 
    finally { setTimeout(() => isMicToggling = false, 300); }
});