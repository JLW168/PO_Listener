/**
 * ABA PayWay Telegram Group Listener Bridge (listener.mjs)
 * Watches your Telegram Group for @PayWayByABA_bot messages using your personal
 * MTProto session and writes parsed payments into Firestore `aba_payments/{trxId}`.
 *
 * Runs 24/7 in the cloud (Render / Railway / Fly.io) without needing local PC CMD open.
 */
import http from "node:http";
import { TelegramClient } from "telegram";
import { StringSession } from "telegram/sessions/index.js";
import { NewMessage } from "telegram/events/index.js";
import { initializeApp, applicationDefault, cert } from "firebase-admin/app";
import { getFirestore } from "firebase-admin/firestore";

// Supports both Cloud Hosting (via FIREBASE_SERVICE_ACCOUNT or FIREBASE_SERVICE_ACCOUNT_JSON env var) and Local PC
const serviceAccountEnv = process.env.FIREBASE_SERVICE_ACCOUNT || process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
if (serviceAccountEnv) {
  try {
    const parsedCreds = typeof serviceAccountEnv === 'string' ? JSON.parse(serviceAccountEnv) : serviceAccountEnv;
    initializeApp({
      credential: cert(parsedCreds),
    });
  } catch (e) {
    console.error("Failed to parse Firebase service account JSON from environment:", e.message);
    initializeApp({
      credential: applicationDefault(),
    });
  }
} else {
  initializeApp({
    credential: applicationDefault(),
  });
}

const db = getFirestore();

// Tiny HTTP Health-Check Server for Free Cloud Hosts (Render.com / Koyeb / UptimeRobot)
const port = Number(process.env.PORT || 3000);
http
  .createServer((req, res) => {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(
      JSON.stringify({
        status: "ok",
        service: "solaris-aba-listener",
        uptimeSeconds: Math.round(process.uptime()),
      })
    );
  })
  .listen(port, () => {
    console.log(`🌐 Health-check server listening on port ${port}`);
  });

// Parses both KHR (៛) and USD ($) ABA PayWay Bot notifications (including `( *232 )` spaces)
const parseABAPayWay = (raw) => {
  if (!raw || typeof raw !== "string") return null;
  const t = raw.replace(/[\r\n\t]+/g, " ").replace(/\s+/g, " ").trim();
  const m = t.match(
    /(?:(៛|\$|KHR|USD)\s*([0-9,]+(?:\.[0-9]{1,2})?))\s+paid by\s+(.+?)(?:\s*\(\s*\*?([0-9A-Za-z]+)\s*\))?\s+on\s+(.+?)\s+via\s+(.+?)\s+at\s+(.+?)(?:\.\s*|\s+)Trx\.?\s*ID:\s*([0-9A-Za-z]+)[,\s]+APV:\s*([0-9A-Za-z]+)/i
  );
  if (!m) return null;
  const cur = m[1] === "៛" || /khr/i.test(m[1]) ? "KHR" : "USD";
  return {
    currency: cur,
    amount: parseFloat(m[2].replace(/,/g, "")),
    payerName: m[3].trim(),
    maskedAccount: m[4] ? `*${m[4].trim()}` : "*ABA",
    paidAtStr: m[5].trim(),
    paymentChannel: m[6].trim(),
    merchantName: m[7].replace(/\.$/, "").trim(),
    trxId: m[8].trim(),
    apv: m[9].replace(/\.$/, "").trim(),
    rawText: t,
  };
};

const apiId = Number(process.env.TG_API_ID);
const apiHash = String(process.env.TG_API_HASH || "").trim();
const sessionStr = String(process.env.TG_SESSION || "").trim();

const rawTargetChats = String(process.env.PAYWAY_CHAT_ID || "").trim();
const allowedChatIds = rawTargetChats
  .split(/[\s,|;]+/)
  .map((id) => id.replace(/^-100/, "").replace(/^-/, "").trim())
  .filter(Boolean);

const isChatAllowed = (chatIdStr) => {
  if (allowedChatIds.length === 0) return true; // Blank = allow ALL groups
  const stripped = String(chatIdStr || "").replace(/^-100/, "").replace(/^-/, "").trim();
  if (!stripped) return true;
  return allowedChatIds.includes(stripped);
};

if (!apiId || !apiHash || !sessionStr) {
  console.error("Missing TG_API_ID, TG_API_HASH, or TG_SESSION in environment variables.");
  process.exit(1);
}

const savedTrxMemory = new Set();

async function saveParsedPaymentToFirestore(parsed, chatIdStr) {
  if (!parsed || !parsed.trxId || savedTrxMemory.has(parsed.trxId)) return false;

  try {
    await db.collection("aba_payments").doc(parsed.trxId).create({
      ...parsed,
      chatId: String(chatIdStr || rawTargetChats),
      receivedAt: Date.now(),
      claimedByOrderId: null,
    });
    savedTrxMemory.add(parsed.trxId);
    console.log(
      `✅ [ABA PAYWAY SAVED] Chat: ${chatIdStr} | Trx: ${parsed.trxId} | ${parsed.currency} ${parsed.amount} | Payer: ${parsed.payerName} (${parsed.maskedAccount}) | Merchant: ${parsed.merchantName} | APV: ${parsed.apv}`
    );
    return true;
  } catch (e) {
    if (e.code === 6 || /ALREADY_EXISTS/i.test(e.message || "")) {
      savedTrxMemory.add(parsed.trxId);
    } else {
      console.error("Firestore write error:", e.message || e);
    }
    return false;
  }
}

const client = new TelegramClient(new StringSession(sessionStr), apiId, apiHash, {
  connectionRetries: 10,
  retryDelay: 2000,
  autoReconnect: true,
  useWSS: true,
});

client.setLogLevel("error");

await client.connect();
console.log("==================================================================");
console.log("✅ Connected to Telegram MTProto as user account!");
console.log(
  "   Target Chat ID Filter:",
  allowedChatIds.length > 0 ? rawTargetChats : "ALL GROUPS (Multi-Shop Mode)"
);
console.log("==================================================================");

// 1. REAL-TIME EVENT LISTENER
client.addEventHandler(async (ev) => {
  try {
    const rawMessage = ev.message?.message || ev.message?.text || "";
    const parsed = parseABAPayWay(rawMessage);
    if (!parsed) return;

    const incomingChatId = String(
      ev.chatId ||
        ev.message?.chatId ||
        ev.message?.peerId?.channelId ||
        ev.message?.peerId?.chatId ||
        ""
    );
    if (!isChatAllowed(incomingChatId)) {
      return;
    }

    await saveParsedPaymentToFirestore(parsed, incomingChatId);
  } catch (err) {
    console.warn("Live event handler warning:", err.message || err);
  }
}, new NewMessage({}));

// 1B. RAW MTPROTO UPDATE FALLBACK
client.addEventHandler(async (update) => {
  try {
    const msgObj = update?.message;
    if (!msgObj || typeof msgObj.message !== "string") return;

    const parsed = parseABAPayWay(msgObj.message);
    if (!parsed) return;

    const peer = msgObj.peerId;
    const rawPeerId = peer?.channelId
      ? `-100${peer.channelId}`
      : peer?.chatId
      ? `-${peer.chatId}`
      : "";

    if (!isChatAllowed(rawPeerId)) return;

    await saveParsedPaymentToFirestore(parsed, rawPeerId);
  } catch (err) {}
});

// 2. LIGHTWEIGHT STARTUP & CATCH-UP SWEEP
let cachedTargetDialogs = [];
let isSweeping = false;

async function syncRecentGroupHistory(isInitialBoot = false) {
  if (isSweeping) return;
  isSweeping = true;
  try {
    if (isInitialBoot || cachedTargetDialogs.length === 0) {
      const dialogs = await client.getDialogs({ limit: 25 });
      cachedTargetDialogs = dialogs.filter((d) => {
        const dialogIdStr = String(d.id || "");
        return (d.isGroup || d.isChannel) && isChatAllowed(dialogIdStr);
      });
    }

    for (const d of cachedTargetDialogs) {
      const dialogIdStr = String(d.id || "");
      const messages = await client.getMessages(d.entity, { limit: isInitialBoot ? 15 : 5 });
      for (const msg of messages) {
        const text = msg?.message || "";
        const parsed = parseABAPayWay(text);
        if (parsed) {
          await saveParsedPaymentToFirestore(parsed, dialogIdStr);
        }
      }
      await new Promise((r) => setTimeout(r, 400));
    }
  } catch (err) {
    if (!/RPCError|INTERNAL|FLOOD/i.test(err.message || "")) {
      console.warn("History sweep warning:", err.message || err);
    }
  } finally {
    isSweeping = false;
  }
}

await syncRecentGroupHistory(true);
setInterval(() => syncRecentGroupHistory(false), 60000);
