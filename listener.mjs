/**
 * ABA PayWay Telegram Group Listener Bridge — Quota-Safe & Anti-Spin-Down Edition (listener.mjs)
 *
 * ROOT CAUSES FIXED IN THIS VERSION:
 * 1. REMOVED `prewarmFirestoreChannel()` 45-second heartbeat write! Previously, writing to Firestore
 *    every 45s consumed ~2,000 writes/day + multiplied by N connected snapshot listeners (~150,000 reads/day),
 *    which exhausted your Firebase Free Tier quota and caused `RESOURCE_EXHAUSTED`!
 * 2. FIXED URL parsing (`const urlObj = new URL(req.url, ...)`): Previously, `if (req.url === "/recent")`
 *    failed whenever query parameters like `/recent?t=12345` or `/recent?scan=1` were sent by the POS!
 * 3. FIXED `receivedAt` timestamp on wake-up / boot: Previously, `saveParsedPaymentToFirestore` overwrote
 *    `receivedAt = Date.now()` for historical messages scanned after a Render wake-up, causing old payments
 *    to look brand-new or get dropped. Now preserves the actual Telegram message timestamp!
 * 4. ADDED Self-Keep-Alive Ping (`RENDER_EXTERNAL_URL`) every 4 minutes + SSE 20s heartbeat comments
 *    so Render.com Free Tier stays warm and active.
 * 5. DECOUPLED SSE & `/recent` from Firestore quota: Even if Firebase hits `RESOURCE_EXHAUSTED`,
 *    `savedTrxMemory` stays locked and `/events` (SSE) + `/recent` continue serving 100% of payments!
 */
import http from "node:http";
import https from "node:https";
import { TelegramClient } from "telegram";
import { StringSession } from "telegram/sessions/index.js";
import { NewMessage } from "telegram/events/index.js";
import { initializeApp, applicationDefault, cert } from "firebase-admin/app";
import { getFirestore } from "firebase-admin/firestore";

// 1. Initialize Firebase Admin SDK
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

// Quota Circuit Breaker for Firestore Writes (Prevents retry storms if daily free quota is exhausted)
let firestoreQuotaExhaustedUntil = 0;

// 2. In-Memory Ring Buffer & SSE Clients for 0ms Direct-to-POS Delivery
const recentPaymentsBuffer = [];
const savedTrxMemory = new Set();
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

// Keep SSE connections alive through Render's load balancer every 20 seconds
setInterval(() => {
  for (const res of sseClients) {
    try {
      res.write(`: heartbeat ${Date.now()}\n\n`);
    } catch (e) {
      sseClients.delete(res);
    }
  }
}, 20000);

// 3. HTTP Health-Check + Query-Safe `/recent`, `/events` (SSE), and `/claim` Server
const port = Number(process.env.PORT || 10000);
http
  .createServer(async (req, res) => {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type");

    if (req.method === "OPTIONS") {
      res.writeHead(204);
      res.end();
      return;
    }

    const urlObj = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);
    const pathname = urlObj.pathname;
    const shouldScan = urlObj.searchParams.get("scan") === "1";

    // Optional on-demand Telegram group sweep when POS requests `?scan=1`
    if (shouldScan) {
      syncRecentGroupHistory(false).catch(() => {});
    }

    // Endpoint A: Server-Sent Events (`/events`, `/stream`, `/sse`)
    if (pathname === "/events" || pathname === "/stream" || pathname === "/sse") {
      res.writeHead(200, {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache, no-transform",
        Connection: "keep-alive",
        "X-Accel-Buffering": "no",
      });
      // Immediately send the connected event AND the recent payments buffer so POS gets instant history
      res.write(
        `data: ${JSON.stringify({
          type: "CONNECTED",
          ts: Date.now(),
          payments: recentPaymentsBuffer.slice(0, 35),
        })}\n\n`
      );
      sseClients.add(res);
      req.on("close", () => sseClients.delete(res));
      return;
    }

    // Endpoint B: Mark a payment as claimed in memory (`/claim?trxId=123&orderId=abc&invoiceNo=HOP-001`)
    if (pathname === "/claim") {
      const trxId = String(urlObj.searchParams.get("trxId") || "").trim();
      const orderId = String(urlObj.searchParams.get("orderId") || "").trim();
      const invoiceNo = String(urlObj.searchParams.get("invoiceNo") || "").trim();
      if (trxId) {
        const found = recentPaymentsBuffer.find((p) => String(p.trxId) === trxId);
        if (found) {
          found.claimedByOrderId = orderId || "CLAIMED";
          if (invoiceNo) found.claimedByInvoiceNo = invoiceNo;
        }
      }
      res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
      res.end(JSON.stringify({ ok: true, trxId }));
      return;
    }

    // Endpoint C: JSON Payment Feed (`/recent`, `/payments`, or root `/?scan=1`)
    if (pathname === "/recent" || pathname === "/payments" || shouldScan) {
      res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
      res.end(
        JSON.stringify({
          ok: true,
          status: "ok",
          service: "solaris-aba-listener-v3",
          uptimeSeconds: Math.round(process.uptime()),
          firestoreQuotaPaused: Date.now() < firestoreQuotaExhaustedUntil,
          payments: recentPaymentsBuffer.slice(0, 50),
        })
      );
      return;
    }

    // Default Root Health Check (`/`) — Also includes recent payments so any ping hydrates the POS!
    res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
    res.end(
      JSON.stringify({
        ok: true,
        status: "ok",
        service: "solaris-aba-listener-v3",
        uptimeSeconds: Math.round(process.uptime()),
        cachedPayments: recentPaymentsBuffer.length,
        firestoreQuotaPaused: Date.now() < firestoreQuotaExhaustedUntil,
        payments: recentPaymentsBuffer.slice(0, 50),
      })
    );
  })
  .listen(port, () => {
    console.log(`🌐 Listener HTTP & SSE server active on port ${port}`);
  });

// Self-Keep-Alive Ping every 4 minutes to prevent Render Free Tier 15-minute inactivity spin-down
const selfExternalUrl =
  process.env.RENDER_EXTERNAL_URL || "https://po-listener.onrender.com";
setInterval(() => {
  try {
    https
      .get(`${selfExternalUrl}/?keepalive=${Date.now()}`, (res) => {
        res.resume();
      })
      .on("error", () => {});
  } catch (e) {}
}, 4 * 60 * 1000);

// 4. Minute-Granularity Timestamp Alignment Helper
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

// 5. Unified Indoor ABA PAY + Outdoor Bank KHQR Parser (Supports All ABA Bot Formats)
function parseABAPayWay(raw, msgDateMs = Date.now()) {
  if (!raw || typeof raw !== "string") return null;
  const t = raw.replace(/[\r\n\t]+/g, " ").replace(/\s+/g, " ").trim();

  const strictRegex =
    /(?:(៛|\$|KHR|USD)\s*([0-9,]+(?:\.[0-9]{1,2})?))\s+paid by\s+(.+?)(?:\s*\(\s*\*\s*([0-9A-Za-z]+)\s*\))?\s+on\s+([A-Za-z]{3,9}\s+\d{1,2}(?:,\s*\d{4})?,?\s+\d{1,2}:\d{2}(?::\d{2})?\s*(?:AM|PM)?|\d{1,2}:\d{2}(?::\d{2})?\s*(?:AM|PM)?)\s+via\s+(.+?)\s+at\s+(.+?)(?:\.\s*|\s+)Trx\.?\s*ID\s*[:#]?\s*([0-9A-Za-z]+)[,\s]+APV\s*[:#]?\s*([0-9A-Za-z]+)/i;

  const m = t.match(strictRegex);
  if (m) {
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

  // Fallback Flexible Matcher in case ABA PayWay Bot slightly alters punctuation
  const amtMatch = t.match(/(?:(៛|\$|KHR|USD)\s*([0-9,]+(?:\.[0-9]{1,2})?))\s*(?:paid by|received from)/i);
  const trxMatch = t.match(/Trx\.?\s*ID\s*[:#]?\s*([0-9A-Za-z]+)/i);
  const apvMatch = t.match(/APV\s*[:#]?\s*([0-9A-Za-z]+)/i);
  if (amtMatch && (trxMatch || apvMatch)) {
    const cur = amtMatch[1] === "៛" || /^khr$/i.test(amtMatch[1]) ? "KHR" : "USD";
    const amount = parseFloat(amtMatch[2].replace(/,/g, ""));
    if (isNaN(amount) || amount <= 0) return null;
    const payerMatch = t.match(/(?:paid by|received from)\s+([^(]+?)(?:\s*\(\s*\*\s*([0-9A-Za-z]+)\s*\)|\s+on\s+|\s+via\s+)/i);
    const onMatch = t.match(/\bon\s+(.+?)\s+via\s+/i);
    const viaMatch = t.match(/\bvia\s+(.+?)\s+at\s+/i);
    const atMatch = t.match(/\bat\s+(.+?)(?:\.\s*Trx|\s+Trx)/i);
    const trxId = trxMatch ? trxMatch[1].trim() : `APV-${apvMatch[1].trim()}`;
    const apv = apvMatch ? apvMatch[1].trim().replace(/\.$/, "") : trxId.slice(-6);
    const paidAtStr = onMatch ? onMatch[1].trim() : "";
    const paymentChannel = viaMatch ? viaMatch[1].trim() : "ABA PAY";
    const isOutdoor = /ABA\s*KHQR/i.test(paymentChannel);
    return {
      currency: cur,
      amount,
      payerName: payerMatch ? payerMatch[1].trim() : "ABA Customer",
      maskedAccount: payerMatch && payerMatch[2] ? `*${payerMatch[2].trim()}` : "*ABA",
      paidAtStr,
      paidTimestamp: parseAbaPaidAtToMs(paidAtStr, msgDateMs),
      paymentChannel,
      paymentCategory: isOutdoor ? "OUTDOOR_KHQR" : "INDOOR_ABA",
      sourceBank: isOutdoor ? paymentChannel : "ABA Bank",
      merchantName: atMatch ? atMatch[1].replace(/\.$/, "").trim() : "",
      trxId,
      apv,
      rawText: t,
    };
  }

  return null;
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

// 6. Quota-Safe Single-Write + 0ms SSE & Ring-Buffer Broadcast
async function saveParsedPaymentToFirestore(parsed, chatIdStr, messageTimestampMs = null) {
  if (!parsed || !parsed.trxId) return false;
  const trxId = String(parsed.trxId).trim();

  if (savedTrxMemory.has(trxId)) return false;
  savedTrxMemory.add(trxId);

  const writeStartMs = performance.now();
  const effectiveReceivedAt =
    Number(messageTimestampMs) > 0 ? Number(messageTimestampMs) : Date.now();

  // Never store a comma-separated list in chatId; store the specific group's chatId
  const singleDefaultChatId = rawTargetChats.split(/[\s,|;]+/)[0] || "";
  const cleanChatId = String(chatIdStr || singleDefaultChatId).trim();

  const docPayload = {
    ...parsed,
    chatId: cleanChatId,
    receivedAt: effectiveReceivedAt,
    claimedByOrderId: null,
  };

  // 1. Push to in-memory ring buffer (sorted newest first) & SSE clients in 0ms
  recentPaymentsBuffer.unshift(docPayload);
  recentPaymentsBuffer.sort((a, b) => (Number(b.receivedAt) || 0) - (Number(a.receivedAt) || 0));
  if (recentPaymentsBuffer.length > 80) recentPaymentsBuffer.length = 80;
  broadcastToSseClients(docPayload);

  // 2. Write to Firestore ONLY if daily quota is not currently exhausted
  if (Date.now() < firestoreQuotaExhaustedUntil) {
    console.log(
      `⚡ [ABA VERIFIED -> SSE/MEMORY (Firestore Quota Paused)] Trx: ${trxId} | ${parsed.currency} ${parsed.amount} | ${parsed.payerName} | APV: ${parsed.apv}`
    );
    return true;
  }

  try {
    // Write ONLY to `aba_payments/{trxId}` (cuts Firestore writes in half!)
    await db.collection("aba_payments").doc(trxId).set(docPayload, { merge: true });

    const elapsedMs = Math.round(performance.now() - writeStartMs);
    console.log(
      `⚡ [ABA VERIFIED -> CLOUD+SSE in ${elapsedMs}ms] Trx: ${trxId} | ${parsed.currency} ${parsed.amount} | ${parsed.payerName} (${parsed.maskedAccount}) via ${parsed.paymentChannel} at ${parsed.merchantName} | APV: ${parsed.apv}`
    );
    return true;
  } catch (e) {
    const errMsg = String(e?.message || e || "");
    if (e?.code === 8 || /RESOURCE_EXHAUSTED|Quota exceeded/i.test(errMsg)) {
      // Pause Firestore writes for 10 minutes so we don't spam errors; SSE & /recent still work 100%!
      firestoreQuotaExhaustedUntil = Date.now() + 10 * 60 * 1000;
      console.warn(
        "⚠️ Firestore Daily Quota Exhausted — Serving payments directly via Render SSE (/events) & HTTP (/recent)!"
      );
      return true;
    }
    console.error("Firestore write warning (still available in /recent & SSE):", errMsg);
    return false;
  }
}

// 7. Native TCP Transport in Node.js
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

    const nowMs = Date.now();
    const parsed = parseABAPayWay(rawMessage, nowMs);
    if (!parsed) return;

    await saveParsedPaymentToFirestore(parsed, incomingChatId, nowMs);
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

    const nowMs = Date.now();
    const parsed = parseABAPayWay(rawText, nowMs);
    if (!parsed) return;

    await saveParsedPaymentToFirestore(parsed, rawPeerId, nowMs);
  } catch (err) {}
});

// 9. FAST PARALLEL TARGET-GROUP SWEEP (Every 3.5 Seconds)
let cachedTargetDialogs = [];
let lastDialogRefreshMs = 0;
let isSweeping = false;

async function syncRecentGroupHistory(isInitialBoot = false) {
  if (isSweeping) return;
  isSweeping = true;
  try {
    const now = Date.now();
    if (isInitialBoot || cachedTargetDialogs.length === 0 || now - lastDialogRefreshMs > 300000) {
      const dialogs = await client.getDialogs({ limit: 35 });
      cachedTargetDialogs = dialogs.filter((d) => {
        const dialogIdStr = String(d.id || "");
        return (d.isGroup || d.isChannel) && isChatAllowed(dialogIdStr);
      });
      lastDialogRefreshMs = now;
    }

    await Promise.all(
      cachedTargetDialogs.map(async (d) => {
        try {
          const dialogIdStr = String(d.id || "");
          const messages = await client.getMessages(d.entity, { limit: isInitialBoot ? 15 : 5 });
          for (const msg of messages) {
            const text = msg?.message || msg?.text || "";
            if (!text) continue;
            const msgServerMs = msg.date ? Number(msg.date) * 1000 : Date.now();
            // Preserve exact Telegram server timestamp for historical messages on boot/wake-up!
            const effectiveMs =
              !isInitialBoot && Math.abs(Date.now() - msgServerMs) < 90000
                ? Date.now()
                : msgServerMs;
            const parsed = parseABAPayWay(text, effectiveMs);
            if (parsed) {
              await saveParsedPaymentToFirestore(parsed, dialogIdStr, effectiveMs);
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
setInterval(() => syncRecentGroupHistory(false), 3500);
