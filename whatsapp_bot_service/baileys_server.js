const { default: makeWASocket, DisconnectReason, useMultiFileAuthState, Browsers } = require('@whiskeysockets/baileys');
const express = require('express');
const axios = require('axios');
const qrcode = require('qrcode-terminal');
const pino = require('pino');

const app = express();
app.use(express.json());

const PYTHON_WEBHOOK_URL = 'http://localhost:5000/whatsapp-webhook'; // Endpoint no seu Flask App
const BAILEYS_API_PORT = 3000; // Porta para este serviço Baileys

let sock;
let globalAuthState;

async function connectToWhatsApp() {
    const { state, saveCreds } = await useMultiFileAuthState('baileys_auth_info');
    globalAuthState = state;

    sock = makeWASocket({
        auth: state,
        printQRInTerminal: true,
        logger: pino({ level: 'silent' }), // Ou 'info' para mais logs
        browser: Browsers.macOS('Desktop'), // Simula um navegador
        syncFullHistory: false // Sincroniza apenas o histórico recente
    });

    sock.ev.on('connection.update', (update) => {
        const { connection, lastDisconnect, qr } = update;
        if (qr) {
            console.log("QR Code recebido, escaneie com seu WhatsApp:");
            qrcode.generate(qr, { small: true });
        }
        if (connection === 'close') {
            const shouldReconnect = (lastDisconnect.error)?.output?.statusCode !== DisconnectReason.loggedOut;
            console.log('Conexão fechada devido a ', lastDisconnect.error, ', reconectando... ', shouldReconnect);
            if (shouldReconnect) {
                connectToWhatsApp();
            } else {
                console.log('Desconectado permanentemente. Apague a pasta baileys_auth_info e reinicie se quiser reconectar com um novo QR code.');
                // Você pode precisar apagar a pasta 'baileys_auth_info' para gerar um novo QR code
            }
        } else if (connection === 'open') {
            console.log('Conectado ao WhatsApp!');
        }
    });

    sock.ev.on('creds.update', saveCreds);

    sock.ev.on('messages.upsert', async (m) => {
        const msg = m.messages[0];
        if (!msg.message || m.type !== 'notify') return;

        // Ignorar mensagens de status, etc.
        if (msg.key && msg.key.remoteJid === 'status@broadcast') return;
        // Ignorar suas próprias mensagens (eco)
        if (msg.key.fromMe) return;


        const remoteJid = msg.key.remoteJid; // ID do grupo ou usuário
        const sender = msg.key.participant || msg.key.remoteJid; // ID do remetente (em grupo ou chat privado)
        let messageText = '';

        if (msg.message.conversation) {
            messageText = msg.message.conversation;
        } else if (msg.message.extendedTextMessage) {
            messageText = msg.message.extendedTextMessage.text;
        }

        if (messageText) {
            console.log(`Mensagem recebida de ${sender} em ${remoteJid}: ${messageText}`);
            try {
                // Envia para o webhook do Python
                await axios.post(PYTHON_WEBHOOK_URL, {
                    groupId: remoteJid, // ID do chat (pode ser grupo ou usuário)
                    senderId: sender, // Quem enviou a mensagem
                    message: messageText,
                    pushName: msg.pushName || '' // Nome do contato como aparece no WhatsApp
                });
            } catch (error) {
                console.error('Erro ao enviar mensagem para o webhook Python:', error.message);
            }
        }
    });
}

// Endpoint para o Python enviar mensagens
app.post('/send-message', async (req, res) => {
    const { to, message } = req.body;
    if (!sock) {
        return res.status(503).json({ error: 'Serviço WhatsApp não está conectado.' });
    }
    if (!to || !message) {
        return res.status(400).json({ error: 'Parâmetros "to" e "message" são obrigatórios.' });
    }

    try {
        console.log(`Enviando mensagem para ${to}: ${message}`);
        await sock.sendMessage(to, { text: message });
        res.status(200).json({ success: true, message: 'Mensagem enviada.' });
    } catch (error) {
        console.error('Erro ao enviar mensagem via Baileys:', error);
        res.status(500).json({ success: false, error: 'Falha ao enviar mensagem.' });
    }
});

connectToWhatsApp().catch(err => console.log("Erro inesperado: " + err));

app.listen(BAILEYS_API_PORT, () => {
    console.log(`Serviço Baileys escutando na porta ${BAILEYS_API_PORT}`);
});