/**
 * ABA PayWay Telegram Group Listener Bridge — Ultra-Low-Latency Edition (listener.mjs)
 * Watches your Telegram Group(s) for @PayWayByABA_bot notifications (Indoor ABA PAY & Outdoor KHQR)
 * using your MTProto session and pushes parsed payments in parallel to:
 *   1. Firestore `system_settings/aba_live_feed` (0ms single-doc real-time stream for POS PaymentModal)
 *   2. Firestore `aba_payments/{trxId}` (persistent transaction ledger & claim guard)
 *   3. Built-in HTTP `/recent` & `/events` (SSE) endpoints for instant diagnostics & health checks
 */
import http from "node:http";
import { TelegramClient } from "telegram";
import { StringSession } from "telegram/sessions/index.js";
import { NewMessage } from "telegram/events/index.js";
import { initializeApp, applicationDefault, cert } from "firebase-admin/app";
import { getFirestore } from "firebase-admin/firestore";

// 1. Initialize Firebase Admin SDK (Cloud Env or Local serviceAccountKey.json)
const serviceAccountEnv =
  process.env.FIREBASE_SERVICE_ACCOUNT || process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
if (serviceAccountEnv) {
  try {
    const parsedCreds =
      typeof serviceAccountEnv === "string"
        ? JSON.parse(serviceAccountEnv)
        : serviceAccountEnv;
    initializeApp({ credential: cert(parsedCreds) });
  } catch (e) {
    console.error("Failed to parse Firebase service account JSON:", e.message);
    initializeApp({ credential: applicationDefault() });
  }
} else {
  initializeApp({ credential: applicationDefault() });
}

const db = getFirestore();
db.settings({ ignoreUndefinedProperties: true, preferRest: false });

// Pre-warm Firestore gRPC HTTP/2 channel & OAuth2 token on boot and keep it warm every 45s
// so the very first payment write never suffers a cold-start gRPC handshake delay
async function prewarmFirestoreChannel() {
  try {
    await db
      .collection("system_settings")
      .doc("aba_live_feed")
      .set({ listenerHeartbeatAt: Date.now() }, { merge: true });
  } catch (e) {}
}
prewarmFirestoreChannel();
setInterval(prewarmFirestoreChannel, 45000);

// 2. In-Memory Ring Buffer & SSE Clients for <5ms Instant Delivery
const recentPaymentsBuffer = [];
const sseClients = new Set();

function broadcastToSseClients(paymentObj) {
  const payload = `data: ${JSON.stringify(paymentObj)}\n\n`;
  for (const res of sseClients) {
    try {
      res.write(payload);
    } catch (e) {
      sseClients.delete(res);
    }
  }
}

// 3. HTTP Health-Check + Instant `/recent` & `/events` SSE Server
const port = Number(process.env.PORT || 3000);
http
  .createServer((req, res) => {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type");

    if (req.method === "OPTIONS") {
      res.writeHead(204);
      res.end();
      return;
    }

    if (req.url === "/events") {
      res.writeHead(200, {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache, no-transform",
        Connection: "keep-alive",
      });
      res.write(`data: ${JSON.stringify({ type: "CONNECTED", ts: Date.now() })}\n\n`);
      sseClients.add(res);
      req.on("close", () => sseClients.delete(res));
      return;
    }

    if (req.url === "/recent") {
      res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
      res.end(JSON.stringify({ ok: true, payments: recentPaymentsBuffer.slice(0, 30) }));
      return;
    }

    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(
      JSON.stringify({
        status: "ok",
        service: "solaris-aba-listener-v2",
        uptimeSeconds: Math.round(process.uptime()),
        cachedPayments: recentPaymentsBuffer.length,
      })
    );
  })
  .listen(port, () => {
    console.log(`🌐 Listener HTTP & SSE server active on port ${port}`);
  });

// 4. Minute-Granularity Timestamp Alignment Helper
// Fixes the 1-minute truncation bug where "Nov 11, 09:39 PM" parses to 09:39:00.000
function parseAbaPaidAtToMs(paidAtStr, referenceMs = Date.now()) {
  if (!paidAtStr || typeof paidAtStr !== "string") return referenceMs;
  const clean = paidAtStr.trim();
  const refDate = new Date(referenceMs);
  const currentYear = refDate.getFullYear();

  const match = clean.match(
    /^([A-Za-z]{3,9})\s+(\d{1,2})(?:,\s*(\d{4}))?,?\s+(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(AM|PM)?$/i
  );
  if (match) {
    const months = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"];
    const mIdx = months.indexOf(match[1].slice(0, 3).toLowerCase());
    if (mIdx !== -1) {
      const day = parseInt(match[2], 10);
      const year = match[3] ? parseInt(match[3], 10) : currentYear;
      let hours = parseInt(match[4], 10);
      const mins = parseInt(match[5], 10);
      const hasSecs = !!match[6];
      const secs = hasSecs ? parseInt(match[6], 10) : 0;
      const ampm = (match[7] || "").toUpperCase();

      if (ampm === "PM" && hours < 12) hours += 12;
      if (ampm === "AM" && hours === 12) hours = 0;

      const constructed = new Date(year, mIdx, day, hours, mins, secs, 0);
      if (!isNaN(constructed.getTime())) {
        if (!hasSecs) {
          const diffMs = referenceMs - constructed.getTime();
          if (diffMs >= 0 && diffMs < 60000) return referenceMs;
          return constructed.getTime() + 59000;
        }
        return constructed.getTime();
      }
    }
  }
  return referenceMs;
}

// 5. Unified Indoor ABA PAY + Outdoor Bank KHQR Parser
// Supports:
//  - 1A: "៛2,000 paid by CHE SOKUNTH (*636) on Nov 11, 09:39 PM via ABA PAY at CHE SOKUNTH MA. Trx. ID: 179440795392550, APV: 831467."
//  - 1B: "$0.38 paid by CHE SOKUNTH (*636) on Nov 11, 09:14 PM via ABA PAY at CHE SOKUNTH MA. Trx. ID: 179440645984571, APV: 841463."
//  - 2A: "៛2,500 paid by Che Sokunth (*815) on Nov 11, 09:45 PM via ABA KHQR (ACLEDA Bank Plc.) at CHE SOKUNTH MA. Trx. ID: 179440830368182, APV: 602725."
function parseABAPayWay(raw, msgDateMs = Date.now()) {
  if (!raw || typeof raw !== "string") return null;
  const t = raw.replace(/[\r\n\t]+/g, " ").replace(/\s+/g, " ").trim();

  const strictRegex =
    /(?:(៛|\$|KHR|USD)\s*([0-9,]+(?:\.[0-9]{1,2})?))\s+paid by\s+(.+?)(?:\s*\(\s*\*\s*([0-9A-Za-z]+)\s*\))?\s+on\s+([A-Za-z]{3,9}\s+\d{1,2}(?:,\s*\d{4})?,?\s+\d{1,2}:\d{2}(?::\d{2})?\s*(?:AM|PM)?|\d{1,2}:\d{2}(?::\d{2})?\s*(?:AM|PM)?)\s+via\s+(.+?)\s+at\s+(.+?)(?:\.\s*|\s+)Trx\.?\s*ID\s*[:#]?\s*([0-9A-Za-z]+)[,\s]+APV\s*[:#]?\s*([0-9A-Za-z]+)/i;

  const m = t.match(strictRegex);
  if (!m) return null;

  const cur = m[1] === "៛" || /^khr$/i.test(m[1]) ? "KHR" : "USD";
  const amount = parseFloat(m[2].replace(/,/g, ""));
  if (isNaN(amount) || amount <= 0) return null;

  const payerName = m[3].replace(/\(\s*\*\s*[0-9A-Za-z]+\s*\)\s*$/, "").trim();
  const maskedAccount = m[4] ? `*${m[4].trim()}` : "*ABA";
  const paidAtStr = m[5].trim();
  const paymentChannel = m[6].trim();
  const bankParenMatch = paymentChannel.match(/ABA\s*KHQR\s*\(\s*([^)]+)\s*\)/i);
  const isOutdoor = /ABA\s*KHQR/i.test(paymentChannel) || !!bankParenMatch;
  const sourceBank = bankParenMatch
    ? bankParenMatch[1].trim()
    : isOutdoor
    ? paymentChannel.replace(/^ABA\s*KHQR\s*/i, "").replace(/[()]/g, "").trim() || "External Bank"
    : "ABA Bank";

  const merchantName = m[7].replace(/\.$/, "").trim();
  const trxId = m[8].trim();
  const apv = m[9].replace(/\.$/, "").trim();

  return {
    currency: cur,
    amount,
    payerName,
    maskedAccount,
    paidAtStr,
    paidTimestamp: parseAbaPaidAtToMs(paidAtStr, msgDateMs),
    paymentChannel,
    paymentCategory: isOutdoor ? "OUTDOOR_KHQR" : "INDOOR_ABA",
    sourceBank,
    merchantName,
    trxId,
    apv,
    rawText: t,
  };
}

const apiId = Number(process.env.TG_API_ID);
const apiHash = String(process.env.TG_API_HASH || "").trim();
const sessionStr = String(process.env.TG_SESSION || "").trim();

const rawTargetChats = String(process.env.PAYWAY_CHAT_ID || "").trim();
const allowedChatIds = rawTargetChats
  .split(/[\s,|;]+/)
  .map((id) => id.replace(/^-100/, "").replace(/^-/, "").trim())
  .filter(Boolean);

const isChatAllowed = (chatIdStr) => {
  if (allowedChatIds.length === 0) return true;
  const stripped = String(chatIdStr || "").replace(/^-100/, "").replace(/^-/, "").trim();
  if (!stripped) return true;
  return allowedChatIds.includes(stripped);
};

if (!apiId || !apiHash || !sessionStr) {
  console.error("Missing TG_API_ID, TG_API_HASH, or TG_SESSION in environment variables.");
  process.exit(1);
}

const savedTrxMemory = new Set();

// 6. Lock-First Parallel Dual-Write to Firestore (`aba_payments` + `system_settings/aba_live_feed`)
async function saveParsedPaymentToFirestore(parsed, chatIdStr) {
  if (!parsed || !parsed.trxId) return false;
  const trxId = String(parsed.trxId).trim();

  // CRITICAL FIX: Claim in-memory lock BEFORE `await` so simultaneous NewMessage + RawUpdate handlers never double-write!
  if (savedTrxMemory.has(trxId)) return false;
  savedTrxMemory.add(trxId);

  const writeStartMs = performance.now();
  const nowMs = Date.now();
  const docPayload = {
    ...parsed,
    chatId: String(chatIdStr || rawTargetChats).trim(),
    receivedAt: nowMs,
    claimedByOrderId: null,
  };

  // Push to in-memory ring buffer & SSE clients in 0ms
  recentPaymentsBuffer.unshift(docPayload);
  if (recentPaymentsBuffer.length > 50) recentPaymentsBuffer.pop();
  broadcastToSseClients(docPayload);

  try {
    // Write to BOTH `aba_payments/{trxId}` AND `system_settings/aba_live_feed` in parallel
    // Single-document listeners on `aba_live_feed` fire 5x faster in browsers than collection queries!
    await Promise.all([
      db.collection("aba_payments").doc(trxId).set(docPayload, { merge: true }),
      db
        .collection("system_settings")
        .doc("aba_live_feed")
        .set(
          {
            lastUpdated: nowMs,
            latestPayment: docPayload,
            [`payments.${trxId}`]: docPayload,
          },
          { merge: true }
        ),
    ]);

    const elapsedMs = Math.round(performance.now() - writeStartMs);
    console.log(
      `⚡ [ABA VERIFIED -> CLOUD in ${elapsedMs}ms] Trx: ${trxId} | ${parsed.currency} ${parsed.amount} | ${parsed.payerName} (${parsed.maskedAccount}) via ${parsed.paymentChannel} at ${parsed.merchantName} | APV: ${parsed.apv}`
    );
    return true;
  } catch (e) {
    savedTrxMemory.delete(trxId);
    console.error("Firestore write error:", e.message || e);
    return false;
  }
}

// 7. Use Native TCP Transport in Node.js (useWSS: false) for lowest latency
const client = new TelegramClient(new StringSession(sessionStr), apiId, apiHash, {
  connectionRetries: 10,
  retryDelay: 1000,
  autoReconnect: true,
  useWSS: false,
});

client.setLogLevel("error");

await client.connect();
console.log("==================================================================");
console.log("⚡ Connected to Telegram MTProto (Ultra-Fast TCP Push Mode)!");
console.log(
  "   Target Chat ID Filter:",
  allowedChatIds.length > 0 ? rawTargetChats : "ALL GROUPS (Multi-Shop Mode)"
);
console.log("==================================================================");

// 8A. REAL-TIME EVENT LISTENER
client.addEventHandler(async (ev) => {
  try {
    const rawMessage = ev.message?.message || ev.message?.text || "";
    if (!rawMessage) return;

    const incomingChatId = String(
      ev.chatId ||
        ev.message?.chatId ||
        ev.message?.peerId?.channelId ||
        ev.message?.peerId?.chatId ||
        ""
    );
    if (!isChatAllowed(incomingChatId)) return;

    const parsed = parseABAPayWay(rawMessage, Date.now());
    if (!parsed) return;

    await saveParsedPaymentToFirestore(parsed, incomingChatId);
  } catch (err) {}
}, new NewMessage({}));

// 8B. RAW MTPROTO UPDATE FALLBACK
client.addEventHandler(async (update) => {
  try {
    const msgObj = update?.message || update?.newMessage?.message;
    const rawText = typeof msgObj === "string" ? msgObj : msgObj?.message || msgObj?.text || "";
    if (!rawText) return;

    const peer = msgObj?.peerId || update?.peerId;
    const rawPeerId = peer?.channelId
      ? `-100${peer.channelId}`
      : peer?.chatId
      ? `-${peer.chatId}`
      : "";

    if (!isChatAllowed(rawPeerId)) return;

    const parsed = parseABAPayWay(rawText, Date.now());
    if (!parsed) return;

    await saveParsedPaymentToFirestore(parsed, rawPeerId);
  } catch (err) {}
});

// 9. FAST PARALLEL TARGET-GROUP SWEEP (Every 3.5 Seconds — No Sequential 400ms Sleeps!)
let cachedTargetDialogs = [];
let lastDialogRefreshMs = 0;
let isSweeping = false;

async function syncRecentGroupHistory(isInitialBoot = false) {
  if (isSweeping) return;
  isSweeping = true;
  try {
    const now = Date.now();
    if (isInitialBoot || cachedTargetDialogs.length === 0 || now - lastDialogRefreshMs > 300000) {
      const dialogs = await client.getDialogs({ limit: 20 });
      cachedTargetDialogs = dialogs.filter((d) => {
        const dialogIdStr = String(d.id || "");
        return (d.isGroup || d.isChannel) && isChatAllowed(dialogIdStr);
      });
      lastDialogRefreshMs = now;
    }

    // Poll all target PayWay groups in PARALLEL without blocking 400ms sleeps
    await Promise.all(
      cachedTargetDialogs.map(async (d) => {
        try {
          const dialogIdStr = String(d.id || "");
          const messages = await client.getMessages(d.entity, { limit: isInitialBoot ? 10 : 4 });
          for (const msg of messages) {
            const text = msg?.message || msg?.text || "";
            if (!text) continue;
            const msgServerMs = msg.date ? Number(msg.date) * 1000 : Date.now();
            const effectiveMs = Math.abs(Date.now() - msgServerMs) < 120000 ? Date.now() : msgServerMs;
            const parsed = parseABAPayWay(text, effectiveMs);
            if (parsed) {
              await saveParsedPaymentToFirestore(parsed, dialogIdStr);
            }
          }
        } catch (innerErr) {}
      })
    );
  } catch (err) {
    if (!/RPCError|INTERNAL|FLOOD/i.test(err.message || "")) {
      console.warn("History sweep warning:", err.message || err);
    }
  } finally {
    isSweeping = false;
  }
}

await syncRecentGroupHistory(true);
// Keep MTProto channel state warm every 3.5 seconds (instead of 60 seconds!)
setInterval(() => syncRecentGroupHistory(false), 3500);
