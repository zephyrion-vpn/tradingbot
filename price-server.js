import dotenv from "dotenv";
dotenv.config();
import express from "express";
import http from "http";
import cors from "cors";
import fetch from "node-fetch";
import WebSocket, { WebSocketServer } from "ws";
import crypto from "crypto";
//import { HttpsProxyAgent } from "https-proxy-agent";
import { Pool } from "pg";
import cron from "node-cron";
import fs from 'fs';
import path from 'path';
import {
  normalizeMarginMode,
  buildAccountSnapshot,
  buildPositionRiskView,
  calculatePositionPnl,
} from "./margin-utils.js";

const app = express();
app.use(cors());
app.use(express.json());

app.get("/health", (req, res) => {
    res.status(200).send("Im Alive");
});

app.post('/internal/price-alerts/refresh', async (req, res) => {
    try {
        const pair = req.body?.pair ? normalizeAlertPair(req.body.pair) : null;
        if (pair) {
            await refreshPriceAlertsCache(pair);
        } else {
            await refreshPriceAlertsCache();
        }
        res.json({ ok: true });
    } catch (e) {
        console.error('❌ Alert cache refresh failed:', e.message);
        res.status(500).json({ ok: false, error: 'REFRESH_FAILED' });
    }
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const PRODUCTS = ["BTC-USDT", "ETH-USDT"];
const COINBASE_REST = "https://api.exchange.coinbase.com";
const BINANCE_WS_BASE = "wss://stream.binance.com:9443/stream?streams=";
const DATABASE_URL = process.env.DATABASE_URL;
const BOT_TOKEN = process.env.BOT_TOKEN;
const AUTH_SECRET = process.env.COOKIE_SECRET || process.env.BOT_TOKEN || crypto.randomBytes(32).toString("hex");
const WS_AUTH_TOKEN_MAX_AGE_MS = 10 * 60 * 1000;
const VAULT_FRACTION = 0.5;              // 50% P&L идёт из пула
const MAX_VAULT_DRAIN_PER_TRADE = 0.1;   // Макс 10% пула за сделку
const TIMEFRAMES = [60, 300, 900, 3600, 21600, 86400];

const COINBASE_MAX_CANDLES_PER_REQUEST = 300;
const INITIAL_HISTORY_CANDLES = 100;
const MAX_CACHED_CANDLES = 20000;

const MIN_PARTIAL_PERCENT = 10;
const MAX_TP_PER_POSITION = 3;
const MAX_SL_PER_POSITION = 3;

const historyStore = {};
const orderbookStore = {};
const tradesStore = {};
const latestPrice = {};

const historyLocks = new Map();

const userWebSockets = new Map();
const activePriceAlertsByPair = new Map();
let activePriceAlertsLoadedAt = 0;
let activePriceAlertsRefreshPromise = null;
const activePriceAlertsLocks = new Set();

if (!process.env.COOKIE_SECRET && !process.env.BOT_TOKEN) {
    console.warn("⚠️ COOKIE_SECRET/BOT_TOKEN not set; generated ephemeral WebSocket auth secret for this process.");
}

function getAuthSecret() {
    return AUTH_SECRET;
}

function safeEqual(a, b) {
    const left = Buffer.from(String(a || ""), "hex");
    const right = Buffer.from(String(b || ""), "hex");
    return left.length === right.length && crypto.timingSafeEqual(left, right);
}

function verifyWsAuthToken(token) {
    if (!token || typeof token !== "string") return null;
    const [userId, expiresAtRaw, mac] = token.split(":");
    const expiresAt = Number(expiresAtRaw);
    if (!userId || !Number.isFinite(expiresAt) || !mac) return null;
    if (expiresAt < Date.now() || expiresAt > Date.now() + WS_AUTH_TOKEN_MAX_AGE_MS) return null;

    const payload = `${userId}:${expiresAtRaw}`;
    const expected = crypto.createHmac("sha256", getAuthSecret()).update(payload).digest("hex");
    return safeEqual(mac, expected) ? String(userId) : null;
}

let sslConfig;

if (process.env.CA_CERT) {
  // 1. Заменяем экранированные переносы строк
  // 2. Убираем случайные кавычки в начале и в конце строки (частая проблема в Render)
  // 3. Убираем лишние пробелы
  const ca = process.env.CA_CERT
    .replace(/\\n/g, '\n')
    .replace(/^"/, '')
    .replace(/"$/, '')
    .trim();

  console.log(`✅ CA cert loaded from env (${ca.length} chars)`);
  
  sslConfig = { 
      ca: ca, 
      rejectUnauthorized: true 
  };
} else {
  console.warn('⚠️ No CA_CERT env var — using rejectUnauthorized: false');
  sslConfig = { rejectUnauthorized: false };
}

// Очищаем DATABASE_URL от параметров sslmode, так как мы передаем настройки в объекте ssl
let cleanDbUrl = process.env.DATABASE_URL;
if (cleanDbUrl && cleanDbUrl.includes('?')) {
    cleanDbUrl = cleanDbUrl.split('?')[0]; // Отрезаем ?sslmode=require
}

const db = new Pool({
  connectionString: cleanDbUrl,
  ssl: sslConfig
});
async function initDatabase() {
    try {
        await db.connect();
        console.log("✅ Database Connected");

        await db.query(`
            CREATE TABLE IF NOT EXISTS tp_sl_orders (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                position_id INTEGER NOT NULL,
                pair VARCHAR(20) NOT NULL,
                order_type VARCHAR(10) NOT NULL CHECK (order_type IN ('TP', 'SL')),
                trigger_price DECIMAL(20, 8) NOT NULL,
                size_percent DECIMAL(5, 2) NOT NULL DEFAULT 100,
                status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'TRIGGERED', 'CANCELLED')),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                triggered_at TIMESTAMP
            )
        `);

        await db.query(`
            CREATE INDEX IF NOT EXISTS idx_tpsl_position ON tp_sl_orders(position_id)
        `);
        await db.query(`
            CREATE INDEX IF NOT EXISTS idx_tpsl_status ON tp_sl_orders(status)
        `);
        await db.query(`
            CREATE INDEX IF NOT EXISTS idx_tpsl_user ON tp_sl_orders(user_id)
        `);

        console.log("✅ TP/SL Orders table ready");

        await db.query(`
            CREATE TABLE IF NOT EXISTS limit_orders (
                id BIGSERIAL PRIMARY KEY,
                user_id TEXT NOT NULL,
                pair TEXT NOT NULL,
                type TEXT NOT NULL CHECK (type IN ('LONG', 'SHORT')),
                limit_price NUMERIC NOT NULL,
                margin NUMERIC NOT NULL,
                leverage INT NOT NULL,
                size NUMERIC NOT NULL,
                status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'FILLED', 'CANCELLED')),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                filled_at TIMESTAMP,
                cancelled_at TIMESTAMP
            );
        `);
        await db.query(`CREATE INDEX IF NOT EXISTS limit_orders_status_idx ON limit_orders(status) WHERE status = 'PENDING';`);
        console.log("✅ Limit Orders table ready");

        await db.query(`
            CREATE TABLE IF NOT EXISTS price_alerts (
                id BIGSERIAL PRIMARY KEY,
                user_id TEXT NOT NULL,
                pair TEXT NOT NULL,
                trigger_price NUMERIC NOT NULL,
                direction TEXT NOT NULL CHECK (direction IN ('ABOVE', 'BELOW')),
                status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'TRIGGERED', 'CANCELLED')),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                triggered_at TIMESTAMP,
                cancelled_at TIMESTAMP
            );
        `);
        await db.query(`CREATE INDEX IF NOT EXISTS price_alerts_user_idx ON price_alerts(user_id);`);
        await db.query(`CREATE INDEX IF NOT EXISTS price_alerts_pair_status_idx ON price_alerts(pair, status) WHERE status = 'ACTIVE';`);
        console.log("✅ Price Alerts table ready");

        await db.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS margin_mode TEXT NOT NULL DEFAULT 'isolated'`);
        console.log("✅ User margin mode column ready");
    } catch (e) {
        console.error("DB Init Error:", e.message);
    }
}

initDatabase()
    .then(() => refreshPriceAlertsCache().catch(e => console.error('❌ Initial alert cache load failed:', e.message)))
    .catch(e => console.error('❌ initDatabase failed:', e.message));

setInterval(() => {
    refreshPriceAlertsCache().catch(e => console.error('❌ Periodic alert cache refresh failed:', e.message));
}, 10000);

// ======================== VAULT FUNCTIONS ========================

async function getVaultPool() {
  try {
    const res = await db.query("SELECT * FROM vault_pool LIMIT 1");
    return res.rows[0] || {
      total_balance: 0,
      total_locked_balance: 0,
      total_unlocked_balance: 0,
      cumulative_pnl: 0
    };
  } catch (e) {
    console.error("Error getting vault pool:", e.message);
    return { total_balance: 0 };
  }
}

async function applyTraderPnlToVault(client, traderPnl, reason, traderId, positionId) {
  try {
    const pool = await getVaultPool();
    const poolBalance = Number(pool.total_balance);

    if (poolBalance <= 0) return 0;

    const PROTOCOL_FEE = 0.1; // 10% от дельты идёт протоколу

    // Сырая дельта пула (противоположна P&L трейдера)
    let rawDelta = -traderPnl * VAULT_FRACTION;

    // Защита от опустошения пула: макс 10% за одну сделку
    if (Math.abs(rawDelta) > poolBalance * MAX_VAULT_DRAIN_PER_TRADE) {
      rawDelta = rawDelta > 0
        ? poolBalance * MAX_VAULT_DRAIN_PER_TRADE
        : -poolBalance * MAX_VAULT_DRAIN_PER_TRADE;
    }

    // Комиссия протокола
    const protocolCut = rawDelta * PROTOCOL_FEE;
    const lpDelta = rawDelta - protocolCut;

    const newBalance = Math.max(0, poolBalance + lpDelta);

    // Обновляем только total_balance — total_shares НЕ трогаем
    await client.query(`
      UPDATE vault_pool SET
        total_balance = $1,
        cumulative_pnl = cumulative_pnl + $2,
        updated_at = CURRENT_TIMESTAMP
      WHERE id = 1
    `, [newBalance, lpDelta]);

    await client.query(`
      INSERT INTO vault_pnl_history
        (delta, reason, trader_user_id, position_id, fraction_applied, pool_balance_before, pool_balance_after)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
    `, [lpDelta, reason, traderId, positionId, VAULT_FRACTION, poolBalance, newBalance]);

    return lpDelta;
  } catch (e) {
    console.error("Error applying vault PnL:", e.message);
    return 0;
  }
}

function registerUserWebSocket(userId, ws) {
    if (!userId) return;
    const id = String(userId);
    if (!userWebSockets.has(id)) {
        userWebSockets.set(id, new Set());
    }
    userWebSockets.get(id).add(ws);
    ws.userId = id;
    console.log(`📱 User ${id} connected via WebSocket`);
}

function unregisterUserWebSocket(ws) {
    if (ws.userId && userWebSockets.has(ws.userId)) {
        userWebSockets.get(ws.userId).delete(ws);
        if (userWebSockets.get(ws.userId).size === 0) {
            userWebSockets.delete(ws.userId);
        }
        console.log(`📴 User ${ws.userId} disconnected from WebSocket`);
    }
}

function sendToUser(userId, message) {
    const id = String(userId);
    const sockets = userWebSockets.get(id);
    if (!sockets || sockets.size === 0) return;
    
    const text = JSON.stringify(message);
    for (const ws of sockets) {
        if (ws.readyState === WebSocket.OPEN) {
            ws.send(text);
        }
    }
}

function normalizeAlertPair(pair) {
    if (!pair) return "";
    return String(pair).trim().replace(/\//g, "-").toUpperCase();
}

function normalizeAlertDirection(direction) {
    return String(direction || "ABOVE").trim().toUpperCase() === "BELOW" ? "BELOW" : "ABOVE";
}

function cachePriceAlerts(rows = []) {
    activePriceAlertsByPair.clear();
    for (const row of rows) {
        const pair = normalizeAlertPair(row.pair);
        if (!activePriceAlertsByPair.has(pair)) activePriceAlertsByPair.set(pair, []);
        activePriceAlertsByPair.get(pair).push(row);
    }
    activePriceAlertsLoadedAt = Date.now();
}

async function refreshPriceAlertsCache(pair = null) {
    const normalizedPair = pair ? normalizeAlertPair(pair) : null;
    const query = normalizedPair
      ? "SELECT * FROM price_alerts WHERE status = 'ACTIVE' AND pair = $1 ORDER BY created_at ASC"
      : "SELECT * FROM price_alerts WHERE status = 'ACTIVE' ORDER BY created_at ASC";
    const params = normalizedPair ? [normalizedPair] : [];
    const res = await db.query(query, params);

    if (normalizedPair) {
        activePriceAlertsByPair.set(normalizedPair, res.rows);
    } else {
        cachePriceAlerts(res.rows);
    }

    activePriceAlertsLoadedAt = Date.now();
    return normalizedPair ? (activePriceAlertsByPair.get(normalizedPair) || []) : res.rows;
}

async function processPriceAlertsForPair(pair, currentPrice) {
    const normalizedPair = normalizeAlertPair(pair);
    if (!normalizedPair || !Number.isFinite(Number(currentPrice))) return;
    if (activePriceAlertsLocks.has(normalizedPair)) return;
    activePriceAlertsLocks.add(normalizedPair);

    try {
        if (Date.now() - activePriceAlertsLoadedAt > 5000) {
            await refreshPriceAlertsCache(normalizedPair);
        }

        const alerts = activePriceAlertsByPair.get(normalizedPair) || [];
        if (!alerts.length) return;

        for (const alert of alerts) {
            const triggerPrice = Number(alert.trigger_price);
            const direction = normalizeAlertDirection(alert.direction);
            const price = Number(currentPrice);
            const shouldTrigger = direction === 'ABOVE' ? price >= triggerPrice : price <= triggerPrice;
            if (!shouldTrigger) continue;

            const client = await db.connect();
            let triggered = false;
            try {
                await client.query('BEGIN');
                const fresh = await client.query(
                    "SELECT * FROM price_alerts WHERE id = $1 AND status = 'ACTIVE' FOR UPDATE",
                    [alert.id]
                );
                if (!fresh.rows.length) {
                    await client.query('COMMIT');
                    continue;
                }

                const freshAlert = fresh.rows[0];
                const freshDirection = normalizeAlertDirection(freshAlert.direction);
                const freshTriggerPrice = Number(freshAlert.trigger_price);
                const freshShouldTrigger = freshDirection === 'ABOVE' ? price >= freshTriggerPrice : price <= freshTriggerPrice;
                if (!freshShouldTrigger) {
                    await client.query('COMMIT');
                    continue;
                }

                await client.query(
                    "UPDATE price_alerts SET status = 'TRIGGERED', triggered_at = CURRENT_TIMESTAMP WHERE id = $1",
                    [freshAlert.id]
                );
                await client.query('COMMIT');
                triggered = true;
                const currentCache = activePriceAlertsByPair.get(normalizedPair) || [];
                activePriceAlertsByPair.set(normalizedPair, currentCache.filter(a => String(a.id) !== String(freshAlert.id)));

                const label = freshDirection === 'BELOW' ? 'below' : 'above';
                const message = `🔔 <b>Price Alert Triggered</b>

${normalizePair(freshAlert.pair)} ${label} ${freshTriggerPrice}
Current price: ${price}`;
                sendTelegramAlert(freshAlert.user_id, message);
                sendToUser(freshAlert.user_id, {
                    type: 'priceAlertTriggered',
                    alertId: freshAlert.id,
                    pair: normalizePair(freshAlert.pair),
                    triggerPrice: freshTriggerPrice,
                    currentPrice: price,
                    direction: freshDirection.toLowerCase(),
                    message,
                    timestamp: Date.now()
                });
                console.log(`🔔 Price alert triggered for user ${freshAlert.user_id}: ${normalizePair(freshAlert.pair)} ${freshDirection} ${freshTriggerPrice}`);
            } catch (e) {
                try { await client.query('ROLLBACK'); } catch(_) {}
                console.error('Price alert trigger error:', e.message);
            } finally {
                client.release();
            }
        }
    } finally {
        activePriceAlertsLocks.delete(normalizedPair);
    }
}

async function fetchUserPositionsAndOrders(userId) {
    const positionsRes = await db.query(
        "SELECT * FROM positions WHERE user_id = $1 ORDER BY created_at ASC",
        [userId]
    );
    
    const ordersRes = await db.query(
        "SELECT * FROM tp_sl_orders WHERE user_id = $1 AND status = 'ACTIVE' ORDER BY created_at ASC",
        [userId]
    );
    
    const userRes = await db.query(
        "SELECT balance, margin_mode FROM users WHERE user_id = $1",
        [userId]
    );

    const limitOrdersRes = await db.query(
        "SELECT * FROM limit_orders WHERE user_id = $1 AND status = 'PENDING' ORDER BY created_at DESC",
        [userId]
    );

    const account = buildAccountSnapshot({
      balance: Number(userRes.rows[0]?.balance || 0),
      positions: positionsRes.rows,
      priceMap: latestPrice
    });
    
    return {
        positions: positionsRes.rows,
        tpSlOrders: ordersRes.rows,
        limitOrders: limitOrdersRes.rows,
        balance: Number(userRes.rows[0]?.balance || 0),
        marginMode: normalizeMarginMode(userRes.rows[0]?.margin_mode),
        account
    };
}

async function notifyUserPositionUpdate(userId, eventType, details = {}) {
    try {
        const data = await fetchUserPositionsAndOrders(userId);
        
        sendToUser(userId, {
            type: "positionUpdate",
            eventType: eventType,
            positions: data.positions,
            tpSlOrders: data.tpSlOrders,
            limitOrders: data.limitOrders,
            balance: Number(data.balance),
            marginMode: data.marginMode,
            account: data.account,
            details: details,
            timestamp: Date.now()
        });
        
        console.log(`📤 Sent positionUpdate to user ${userId}: ${eventType}`);
    } catch (e) {
        console.error(`Failed to notify user ${userId}:`, e.message);
    }
}

function getBinanceSymbol(product) {
    return product.replace("-", "").toLowerCase();
}

function normalizeGranularity(granularity) {
    const g = Number(granularity);
    return TIMEFRAMES.includes(g) ? g : 60;
}

function normalizePair(p) {
    if (!p) return "";
    return String(p).trim().replace("/", "-").toUpperCase();
}

function ensureHistoryBucket(product, granularity) {
    if (!historyStore[product]) historyStore[product] = {};
    if (!historyStore[product][granularity]) historyStore[product][granularity] = [];
    return historyStore[product][granularity];
}

function mergeCandles(existing, incoming) {
    if (!incoming || incoming.length === 0) return existing;

    const map = new Map();
    for (const c of existing) map.set(c.time, c);
    for (const c of incoming) map.set(c.time, c);

    const merged = Array.from(map.values()).sort((a, b) => a.time - b.time);
    if (merged.length > MAX_CACHED_CANDLES) {
        return merged.slice(-MAX_CACHED_CANDLES);
    }
    return merged;
}

function withHistoryLock(product, granularity, fn) {
    const key = `${product}:${granularity}`;
    const prev = historyLocks.get(key) || Promise.resolve();

    const next = prev
        .catch(() => {})
        .then(fn)
        .finally(() => {
            if (historyLocks.get(key) === next) historyLocks.delete(key);
        });

    historyLocks.set(key, next);
    return next;
}

function getCoinbaseSymbol(binanceStreamName) {
    const symbol = binanceStreamName.split("@")[0];
    return symbol.toUpperCase().replace("USDT", "-USDT");
}

function formatBinanceOrderBook(bids, asks) {
    const format = (arr) => arr.map(([p, s]) => ({ price: Number(p), size: Number(s) }));
    return { buy: format(bids), sell: format(asks) };
}

function broadcast(msg) {
    const text = JSON.stringify(msg);
    const pair = msg.pair;
    wss.clients.forEach(ws => {
        if (ws.readyState === WebSocket.OPEN) {
            if (pair && ws.subscriptions && !ws.subscriptions.has(pair)) return;
            ws.send(text);
        }
    });
}

async function sendTelegramAlert(userId, message) {
    if (!BOT_TOKEN || !userId) {
        console.error("⚠️ TG Alert skipped: No Token or User ID");
        return;
    }

    try {
        const url = `https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`;
        const response = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                chat_id: userId,
                text: message,
                parse_mode: 'HTML'
            })
        });

        const data = await response.json();

        if (!data.ok) {
            console.error(`❌ TELEGRAM API ERROR for User ${userId}: ${data.description}`);
        } else {
            console.log(`✅ Message sent to ${userId}`);
        }
    } catch (e) {
        console.error("❌ NETWORK/FETCH ERROR:", e.message);
    }
}

let isProcessingLiquidations = false;
let isProcessingTpSl = false;
let isProcessingLimitOrders = false;

app.post("/api/tp-sl/create", async (req, res) => {
    const { positionId, orderType, triggerPrice, sizePercent } = req.body;

    if (!positionId || !orderType || !triggerPrice) return res.status(400).json({ ok: false, error: "MISSING_FIELDS" });
    if (!['TP', 'SL'].includes(orderType)) return res.status(400).json({ ok: false, error: "INVALID_ORDER_TYPE" });

    return res.status(410).json({ ok: false, error: "USE_MAIN_API" });
});

app.post("/api/tp-sl/legacy-create", async (req, res) => {
    return res.status(410).json({ ok: false, error: "USE_MAIN_API" });
});

app.post("/api/tp-sl/delete", async (req, res) => {
    const { orderId } = req.body;
    if (!orderId) return res.status(400).json({ ok: false, error: "MISSING_ORDER_ID" });
    return res.status(410).json({ ok: false, error: "USE_MAIN_API" });
});

app.get("/api/tp-sl/list", async (req, res) => {
    return res.status(410).json({ ok: false, error: "USE_MAIN_API" });
});

async function checkLiquidations() {
    if (isProcessingLiquidations || Object.keys(latestPrice).length === 0) return;

    isProcessingLiquidations = true;

    try {
        const res = await db.query(`
            SELECT u.user_id, u.margin_mode, u.balance, p.*
            FROM users u
            JOIN positions p ON p.user_id = u.user_id
            ORDER BY u.user_id ASC, p.created_at ASC
        `);

        if (res.rows.length === 0) {
            isProcessingLiquidations = false;
            return;
        }

        const groups = new Map();
        for (const row of res.rows) {
            const userId = String(row.user_id);
            if (!groups.has(userId)) {
                groups.set(userId, {
                    userId,
                    marginMode: normalizeMarginMode(row.margin_mode),
                    balance: Number(row.balance || 0),
                    positions: []
                });
            }
            const group = groups.get(userId);
            const pos = { ...row };
            delete pos.margin_mode;
            delete pos.balance;
            group.positions.push(pos);
        }

        for (const group of groups.values()) {
            const snapshot = buildAccountSnapshot({ balance: group.balance, positions: group.positions, priceMap: latestPrice });
            const maintenance = snapshot.maintenanceMargin + snapshot.closeFees;
            const safeThreshold = maintenance * 1.2;
            const severeThreshold = maintenance;

            if (group.marginMode === 'cross') {
                const riskyPositions = snapshot.positions
                    .map((position) => ({
                        position,
                        riskView: buildPositionRiskView(position, snapshot, position.markPrice),
                    }))
                    .sort((a, b) => a.riskView.pnl - b.riskView.pnl);

                if (snapshot.equity <= safeThreshold) {
                    const shouldWarn = snapshot.positions.some((p) => !p.warning_sent);
                    if (shouldWarn) {
                        const top = riskyPositions[0]?.position;
                        const msg = `⚠️ <b>MARGIN CALL WARNING</b> ⚠️

` +
                            `Account equity: ${snapshot.equity.toFixed(2)} VP
` +
                            `Available margin: ${snapshot.availableMargin.toFixed(2)} VP
` +
                            `Used margin: ${snapshot.reservedMargin.toFixed(2)} VP
` +
                            `Risk level: ${snapshot.riskLevel.toFixed(1)}%
` +
                            `Maintenance threshold: ${safeThreshold.toFixed(2)} VP

` +
                            `Cross margin is under pressure. Reduce exposure or add collateral.`;
                        await sendTelegramAlert(group.userId, msg);
                        await db.query(`UPDATE positions SET warning_sent = TRUE WHERE user_id = $1`, [group.userId]);
                        console.log(`⚠️ Cross warning sent to user ${group.userId}${top ? ` for ${top.pair}` : ''}`);
                    }
                } else {
                    await db.query(`UPDATE positions SET warning_sent = FALSE WHERE user_id = $1`, [group.userId]);
                }

                if (snapshot.equity <= severeThreshold) {
                    for (const item of riskyPositions) {
                        const current = item.position.markPrice || latestPrice[normalizePair(item.position.pair)];
                        console.log(`💀 LIQUIDATING CROSS: User ${group.userId} | ${item.position.pair}`);
                        await executeLiquidation(item.position, current, { marginMode: 'cross', snapshot });

                        const refreshed = await db.query(`
                            SELECT u.user_id, u.margin_mode, u.balance, p.*
                            FROM users u
                            JOIN positions p ON p.user_id = u.user_id
                            WHERE u.user_id = $1
                        `, [group.userId]);
                        if (!refreshed.rows.length) break;

                        const nextPositions = refreshed.rows.map(r => {
                            const pos = { ...r };
                            delete pos.margin_mode;
                            delete pos.balance;
                            return pos;
                        });
                        const nextSnapshot = buildAccountSnapshot({ balance: Number(refreshed.rows[0].balance || 0), positions: nextPositions, priceMap: latestPrice });
                        if (nextSnapshot.equity > nextSnapshot.maintenanceMargin + nextSnapshot.closeFees) break;
                    }
                    continue;
                }

                continue;
            }

            for (const pos of group.positions) {
                const pair = normalizePair(pos.pair);
                const currentPrice = latestPrice[pair];
                if (!currentPrice) continue;

                const entry = Number(pos.entry_price);
                const size = Number(pos.size);
                const margin = Number(pos.margin);
                let pnl = 0;
                const diff = (currentPrice - entry) / entry;
                if (pos.type === 'LONG') pnl = diff * size;
                else pnl = -diff * size;

                const closeCommission = size * 0.0003;
                const maintenanceMargin = size * 0.004;
                const remainingEquity = margin + pnl;
                const liquidationThreshold = closeCommission + maintenanceMargin;

                if (remainingEquity <= liquidationThreshold) {
                    console.log(`💀 LIQUIDATING: User ${pos.user_id} | ${pos.pair}`);
                    await executeLiquidation(pos, currentPrice, { marginMode: 'isolated' });
                    continue;
                }

                const warningThreshold = liquidationThreshold * 1.2;
                if (!pos.warning_sent && remainingEquity <= warningThreshold) {
                    const pnlFormatted = pnl.toFixed(2);
                    const msg = `⚠️ <b>MARGIN CALL WARNING</b> ⚠️

` +
                        `Your position <b>${pos.type} ${pos.pair}</b> (x${pos.leverage}) is at risk!

` +
                        `📉 PnL: ${pnlFormatted} VP
` +
                        `💰 Remaining Equity: ${remainingEquity.toFixed(2)} VP
` +
                        `💀 Liquidation at approx: ${liquidationThreshold.toFixed(2)} VP

` +
                        `System will auto-liquidate if equity drops further.`;
                    await sendTelegramAlert(pos.user_id, msg);
                    await db.query(`UPDATE positions SET warning_sent = TRUE WHERE id = $1`, [pos.id]);
                    console.log(`⚠️ Warning sent to user ${pos.user_id}`);
                }
            }
        }
    } catch (e) {
        console.error("Liquidation Loop Error:", e.message);
    } finally {
        isProcessingLiquidations = false;
    }
}

async function executeLiquidation(pos, exitPrice, context = {}) {
    const client = await db.connect();
    try {
        const size = Number(pos.size);
        const margin = Number(pos.margin);
        const closeCommission = size * 0.0003;
        const mode = normalizeMarginMode(context.marginMode || pos.margin_mode || 'isolated');
        const isCross = mode === 'cross';

        const currentMark = Number(exitPrice || latestPrice[normalizePair(pos.pair)] || pos.entry_price);
        const pnlData = calculatePositionPnl(pos, currentMark);
        const pnlGross = isCross ? pnlData.pnl - closeCommission : closeCommission - margin;
        const balanceDelta = isCross ? margin + pnlData.pnl - closeCommission : 0;

        await client.query("BEGIN");

        await client.query(
            "UPDATE tp_sl_orders SET status = 'CANCELLED' WHERE position_id = $1 AND status = 'ACTIVE'",
            [pos.id]
        );

        // Применяем P&L к пулу
        const vaultDelta = await applyTraderPnlToVault(
            client,
            pnlGross,
            'liquidation',
            pos.user_id,
            pos.id
        );

        if (balanceDelta !== 0) {
            await client.query(
                "UPDATE users SET balance = GREATEST(balance + $1, 0) WHERE user_id = $2",
                [balanceDelta, pos.user_id]
            );
        }

        await client.query(`
            INSERT INTO trades_history (user_id, pair, type, entry_price, exit_price, size, leverage, pnl, commission)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        `, [pos.user_id, pos.pair, pos.type, pos.entry_price, exitPrice, size, pos.leverage, pnlGross, closeCommission]);

        await client.query(`DELETE FROM positions WHERE id = $1`, [pos.id]);
        await client.query("COMMIT");

        console.log(`💀 Liquidation: vault_delta=${vaultDelta.toFixed(2)}`);

        const msg = `⛔️ <b>LIQUIDATED</b>\n\n` +
            `Your position <b>${pos.pair}</b> has been forcefully closed.\n` +
            `📉 Loss: ${(isCross ? Math.min(0, balanceDelta) : -margin).toFixed(2)} VP\n` +
            `💸 Fee: ${closeCommission.toFixed(2)} VP\n` +
            `Price reached liquidation level.`;

        sendTelegramAlert(pos.user_id, msg);

        await notifyUserPositionUpdate(pos.user_id, 'LIQUIDATION', {
            positionId: pos.id,
            pair: pos.pair,
            type: pos.type,
            pnl: pnlGross,
            exitPrice: exitPrice
        });

    } catch (e) {
        await client.query("ROLLBACK");
        console.error("Liquidation DB Error:", e);
    } finally {
        client.release();
    }
}

async function checkTpSlOrders() {
    if (isProcessingTpSl || Object.keys(latestPrice).length === 0) return;

    isProcessingTpSl = true;

    try {
        const ordersRes = await db.query(
            "SELECT * FROM tp_sl_orders WHERE status = 'ACTIVE' ORDER BY created_at ASC"
        );

        if (ordersRes.rows.length === 0) {
            isProcessingTpSl = false;
            return;
        }

        const positionCache = new Map();

        for (const order of ordersRes.rows) {
            const pair = normalizePair(order.pair);
            const currentPrice = latestPrice[pair];
            if (!currentPrice) continue;

            let pos = positionCache.get(Number(order.position_id));
            if (!pos) {
                const posRes = await db.query("SELECT * FROM positions WHERE id = $1", [order.position_id]);
                if (!posRes.rows.length) {
                    await db.query("UPDATE tp_sl_orders SET status = 'CANCELLED' WHERE id = $1", [order.id]);
                    continue;
                }
                pos = posRes.rows[0];
                positionCache.set(Number(pos.id), pos);
            }

            const posType = pos.type.toUpperCase();
            const triggerPrice = Number(order.trigger_price);
            let triggered = false;

            if (order.order_type === 'TP') {
                if (posType === 'LONG' && currentPrice >= triggerPrice) {
                    triggered = true;
                } else if (posType === 'SHORT' && currentPrice <= triggerPrice) {
                    triggered = true;
                }
            } else if (order.order_type === 'SL') {
                if (posType === 'LONG' && currentPrice <= triggerPrice) {
                    triggered = true;
                } else if (posType === 'SHORT' && currentPrice >= triggerPrice) {
                    triggered = true;
                }
            }

            if (triggered) {
                console.log(`🎯 ${order.order_type} TRIGGERED: Position ${order.position_id}, Price ${triggerPrice}, Current ${currentPrice}`);
                await executeTpSlOrder(order, pos, triggerPrice);
                positionCache.delete(Number(order.position_id));
            }
        }
    } catch (e) {
        console.error("TP/SL Engine Error:", e.message);
    } finally {
        isProcessingTpSl = false;
    }
}

async function executeTpSlOrder(order, pos, executionPrice) {
    const client = await db.connect();
    try {
        await client.query("BEGIN");

        const freshPos = await client.query(
            "SELECT * FROM positions WHERE id = $1 FOR UPDATE",
            [order.position_id]
        );

        if (!freshPos.rows.length) {
            await client.query("UPDATE tp_sl_orders SET status = 'CANCELLED' WHERE id = $1", [order.id]);
            await client.query("COMMIT");
            return;
        }

        const position = freshPos.rows[0];
        const posSize = Number(position.size);
        const posMargin = Number(position.margin);
        const entryPrice = Number(position.entry_price);
        const sizePercent = Number(order.size_percent);
        const posType = position.type.toUpperCase();

        const closeSize = (posSize * sizePercent) / 100;
        const closeMargin = (posMargin * sizePercent) / 100;

        const priceChangePct = (executionPrice - entryPrice) / entryPrice;
        let pnl = priceChangePct * closeSize;
        if (posType === "SHORT") pnl = -pnl;

        const commission = closeSize * 0.0003;
        let totalReturn = closeMargin + pnl - commission;

        if (totalReturn < 0) totalReturn = 0;

        // Применяем P&L к пулу
        const vaultDelta = await applyTraderPnlToVault(
            client,
            pnl,
            'tp_sl_execution',
            position.user_id,
            position.id
        );

        if (totalReturn > 0) {
            await client.query(
                "UPDATE users SET balance = balance + $1 WHERE user_id = $2",
                [totalReturn, position.user_id]
            );
        }

        await client.query(`
            INSERT INTO trades_history (user_id, pair, type, entry_price, exit_price, size, leverage, pnl, commission)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        `, [position.user_id, position.pair, posType, entryPrice, executionPrice, closeSize, position.leverage, pnl, commission]);

        await client.query(
            "UPDATE tp_sl_orders SET status = 'TRIGGERED', triggered_at = CURRENT_TIMESTAMP WHERE id = $1",
            [order.id]
        );

        const isFullClose = sizePercent >= 99.99;
        const remainingSize = posSize - closeSize;
        const remainingMargin = posMargin - closeMargin;

        let positionClosed = false;

        if (isFullClose || remainingSize < 0.01 || remainingMargin < 0.01) {
            await client.query(
                "UPDATE tp_sl_orders SET status = 'CANCELLED' WHERE position_id = $1 AND status = 'ACTIVE'",
                [position.id]
            );
            await client.query("DELETE FROM positions WHERE id = $1", [position.id]);
            positionClosed = true;
        } else {
            await client.query(
                "UPDATE positions SET size = $1, margin = $2 WHERE id = $3",
                [remainingSize, remainingMargin, position.id]
            );

            const remainingOrders = await client.query(
                "SELECT * FROM tp_sl_orders WHERE position_id = $1 AND status = 'ACTIVE' ORDER BY created_at ASC",
                [position.id]
            );

            for (const remainingOrder of remainingOrders.rows) {
                const orderSizeAbs = (posSize * Number(remainingOrder.size_percent)) / 100;
                if (orderSizeAbs > remainingSize) {
                    const newPercent = Math.min(100, (remainingSize / posSize) * Number(remainingOrder.size_percent) * (posSize / remainingSize));
                    const cappedPercent = Math.min(100, Math.max(MIN_PARTIAL_PERCENT, newPercent));
                    await client.query(
                        "UPDATE tp_sl_orders SET size_percent = $1 WHERE id = $2",
                        [Math.round(cappedPercent * 100) / 100, remainingOrder.id]
                    );
                }
            }

            const sameTypeOrders = remainingOrders.rows.filter(o => o.order_type === order.order_type);
            let totalSamePercent = 0;
            for (const o of sameTypeOrders) {
                const updatedRes = await client.query("SELECT size_percent FROM tp_sl_orders WHERE id = $1", [o.id]);
                if (updatedRes.rows.length) {
                    totalSamePercent += Number(updatedRes.rows[0].size_percent);
                }
            }

            if (totalSamePercent > 100) {
                const scale = 100 / totalSamePercent;
                for (const o of sameTypeOrders) {
                    const updatedRes = await client.query("SELECT size_percent FROM tp_sl_orders WHERE id = $1", [o.id]);
                    if (updatedRes.rows.length) {
                        const newPct = Math.max(MIN_PARTIAL_PERCENT, Number(updatedRes.rows[0].size_percent) * scale);
                        await client.query(
                            "UPDATE tp_sl_orders SET size_percent = $1 WHERE id = $2",
                            [Math.round(newPct * 100) / 100, o.id]
                        );
                    }
                }
            }
        }

        await client.query("COMMIT");

        const pnlFormatted = pnl.toFixed(2);
        const isProfit = pnl >= 0;
        const emoji = order.order_type === 'TP' ? '🎯' : '🛡️';
        const typeLabel = order.order_type === 'TP' ? 'Take Profit' : 'Stop Loss';

        let msg;
        if (positionClosed) {
            msg = `${emoji} <b>${typeLabel} Triggered!</b>\n\n` +
                `Position <b>${posType} ${position.pair}</b> (x${position.leverage}) fully closed.\n\n` +
                `📊 Entry: ${entryPrice}\n` +
                `📊 Exit: ${executionPrice}\n` +
                `${isProfit ? '📈' : '📉'} PnL: ${isProfit ? '+' : ''}${pnlFormatted} VP\n` +
                `💸 Fee: ${commission.toFixed(2)} VP`;
        } else {
            msg = `${emoji} <b>Partial ${typeLabel} Triggered!</b>\n\n` +
                `Position <b>${posType} ${position.pair}</b> (x${position.leverage})\n` +
                `Closed ${sizePercent}% of position.\n\n` +
                `📊 Entry: ${entryPrice}\n` +
                `📊 Exit: ${executionPrice}\n` +
                `${isProfit ? '📈' : '📉'} PnL: ${isProfit ? '+' : ''}${pnlFormatted} VP\n` +
                `💸 Fee: ${commission.toFixed(2)} VP\n` +
                `📋 Remaining size: ${remainingSize.toFixed(2)} VP`;
        }

        sendTelegramAlert(position.user_id, msg);

        await notifyUserPositionUpdate(position.user_id, positionClosed ? 'TP_SL_FULL_CLOSE' : 'TP_SL_PARTIAL_CLOSE', {
            orderId: order.id,
            orderType: order.order_type,
            positionId: position.id,
            pair: position.pair,
            type: posType,
            pnl: pnl,
            sizePercent: sizePercent,
            executionPrice: executionPrice,
            positionClosed: positionClosed,
            remainingSize: positionClosed ? 0 : remainingSize,
            remainingMargin: positionClosed ? 0 : remainingMargin
        });

        console.log(`✅ ${order.order_type} executed: Position ${position.id}, PnL: ${pnlFormatted}, Size: ${sizePercent}%`);

    } catch (e) {
        await client.query("ROLLBACK");
        console.error("TP/SL Execution Error:", e.message);
    } finally {
        client.release();
    }
}

setInterval(checkLiquidations, 500);
setInterval(checkTpSlOrders, 500);

// ======================== LIMIT ORDERS ========================

async function checkLimitOrders() {
    if (isProcessingLimitOrders || Object.keys(latestPrice).length === 0) return;
    isProcessingLimitOrders = true;

    try {
        const ordersRes = await db.query(
            "SELECT * FROM limit_orders WHERE status = 'PENDING' ORDER BY created_at ASC"
        );

        if (ordersRes.rows.length === 0) {
            isProcessingLimitOrders = false;
            return;
        }

        for (const order of ordersRes.rows) {
            const pair = normalizePair(order.pair);
            const currentPrice = latestPrice[pair];
            if (!currentPrice) continue;

            const limitPrice = Number(order.limit_price);
            const type = order.type.toUpperCase();
            let triggered = false;

            // LONG limit: triggers when price drops to or below limit
            if (type === 'LONG' && currentPrice <= limitPrice) {
                triggered = true;
            }
            // SHORT limit: triggers when price rises to or above limit
            if (type === 'SHORT' && currentPrice >= limitPrice) {
                triggered = true;
            }

            if (triggered) {
                console.log(`🎯 LIMIT ORDER TRIGGERED: ${type} ${pair} @ ${limitPrice}, current=${currentPrice}`);
                await executeLimitOrder(order, limitPrice);
            }
        }
    } catch (e) {
        console.error("Limit Order Check Error:", e.message);
    } finally {
        isProcessingLimitOrders = false;
    }
}

async function executeLimitOrder(order, fillPrice) {
    const client = await db.connect();
    try {
        await client.query("BEGIN");

        // Re-check order is still PENDING (prevent double-execution)
        const freshOrder = await client.query(
            "SELECT * FROM limit_orders WHERE id = $1 AND status = 'PENDING' FOR UPDATE",
            [order.id]
        );
        if (!freshOrder.rows.length) {
            await client.query("COMMIT");
            return;
        }

        const lo = freshOrder.rows[0];
        const pair = normalizePair(lo.pair);
        const margin = Number(lo.margin);
        const size = Number(lo.size);
        const leverage = Number(lo.leverage);
        const type = lo.type.toUpperCase();

        // Create position at the limit price
        const posRes = await client.query(`
            INSERT INTO positions (user_id, pair, type, entry_price, margin, leverage, size)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING *
        `, [lo.user_id, lo.pair, type, fillPrice, margin, leverage, size]);

        // Mark limit order as filled
        await client.query(
            "UPDATE limit_orders SET status = 'FILLED', filled_at = CURRENT_TIMESTAMP WHERE id = $1",
            [lo.id]
        );

        await client.query("COMMIT");

        console.log(`✅ Limit order ${lo.id} filled: ${type} ${pair} @ ${fillPrice}`);

        // Send Telegram alert
        const msg = `📋 <b>Limit Order Filled!</b>\n\n` +
            `Your limit <b>${type} ${pair}</b> (x${leverage}) has been filled.\n\n` +
            `📍 Fill Price: ${fillPrice}\n` +
            `💰 Margin: ${margin.toFixed(2)} VP\n` +
            `📐 Size: ${size.toFixed(2)} VP`;
        sendTelegramAlert(lo.user_id, msg);

        // Notify via WebSocket
        const data = await fetchUserPositionsAndOrders(lo.user_id);

        sendToUser(lo.user_id, {
            type: "limitOrderFilled",
            orderId: lo.id,
            fillPrice: fillPrice,
            orderType: type,
            pair: pair,
            position: posRes.rows[0],
            positions: data.positions,
            tpSlOrders: data.tpSlOrders,
            limitOrders: data.limitOrders,
            balance: Number(data.balance),
            timestamp: Date.now()
        });

    } catch (e) {
        try { await client.query("ROLLBACK"); } catch(ex) {}
        console.error("Limit Order Execution Error:", e.message);
    } finally {
        client.release();
    }
}

setInterval(checkLimitOrders, 500);

async function fetchCoinbaseCandlesPage(product, granularity, endSec) {
    try {
        // Coinbase имеет глубокую историю только для USD-пар, поэтому
        // при запросе к API подменяем USDT → USD, данные остаются совместимыми
        const coinbaseProduct = product.replace('-USDT', '-USD');

        const end = new Date(endSec * 1000);
        const start = new Date((endSec - (granularity * COINBASE_MAX_CANDLES_PER_REQUEST)) * 1000);

        const url = `${COINBASE_REST}/products/${coinbaseProduct}/candles` +
            `?granularity=${granularity}` +
            `&start=${encodeURIComponent(start.toISOString())}` +
            `&end=${encodeURIComponent(end.toISOString())}`;

        const r = await fetch(url, { headers: { "User-Agent": "TradeSimBot/1.0" } });
        if (!r.ok) return;
        const chunk = await r.json();

        return chunk
            .map(c => ({
                time: Math.floor(c[0]),
                open: Number(c[3]),
                high: Number(c[2]),
                low: Number(c[1]),
                close: Number(c[4]),
            }))
            .sort((a, b) => a.time - b.time);
    } catch (e) {
        console.error(`Ошибка истории ${product} (${granularity}s):`, e.message);
        return;
    }
}

async function refreshLatestHistory(product, granularity = 60) {
    const g = normalizeGranularity(granularity);
    const nowSec = Math.floor(Date.now() / 1000);
    const page = await fetchCoinbaseCandlesPage(product, g, nowSec);
    if (!page || page.length === 0) return;

    const existing = ensureHistoryBucket(product, g);
    historyStore[product][g] = mergeCandles(existing, page);
}

async function ensureHistoryLength(product, granularity = 60, minCandles = INITIAL_HISTORY_CANDLES) {
    const g = normalizeGranularity(granularity);

    await refreshLatestHistory(product, g);
    ensureHistoryBucket(product, g);

    let arr = historyStore[product][g];
    let safetyPages = 0;
    while (arr.length < minCandles && safetyPages < 20 && arr.length > 0) {
        safetyPages++;
        const oldest = arr[0].time;
        const endSec = oldest - 1;
        const page = await fetchCoinbaseCandlesPage(product, g, endSec);
        if (!page || page.length === 0) break;
        arr = mergeCandles(arr, page);
        historyStore[product][g] = arr;

        if (arr[0].time >= oldest) break;
    }
}

async function ensureHistoryBefore(product, granularity, untilSec) {
    const g = normalizeGranularity(granularity);
    ensureHistoryBucket(product, g);

    let arr = historyStore[product][g];
    if (arr.length === 0) {
        await ensureHistoryLength(product, g, INITIAL_HISTORY_CANDLES);
        arr = historyStore[product][g];
        if (arr.length === 0) return;
    }

    let safetyPages = 0;
    while (arr.length > 0 && arr[0].time >= untilSec && safetyPages < 20) {
        safetyPages++;
        const oldest = arr[0].time;
        const endSec = oldest - 1;
        const page = await fetchCoinbaseCandlesPage(product, g, endSec);
        if (!page || page.length === 0) break;
        arr = mergeCandles(arr, page);
        historyStore[product][g] = arr;

        if (arr[0].time >= oldest) break;
    }
}

let binanceWS;

function connectBinanceWS() {
    const streams = PRODUCTS.map(p => {
        const sym = getBinanceSymbol(p);
        return `${sym}@depth20@100ms/${sym}@aggTrade/${sym}@ticker`;
    }).join("/");

    console.log("🌐 Подключение к Binance Global через прокси (NL)...");

    binanceWS = new WebSocket(BINANCE_WS_BASE + streams);

    binanceWS.on("open", () => console.log("✅ Соединение с Binance установлено!"));

    binanceWS.on("message", raw => {
        try {
            const msg = JSON.parse(raw.toString());
            if (!msg.data || !msg.stream) return;

            const pair = getCoinbaseSymbol(msg.stream);
            const streamName = msg.stream.split("@")[1];

            if (streamName.startsWith("depth")) {
                orderbookStore[pair] = formatBinanceOrderBook(msg.data.bids, msg.data.asks);
            }
            else if (streamName === "ticker") {
                latestPrice[pair] = Number(msg.data.c);
                broadcast({ 
                    type: "price", 
                    pair: pair, 
                    price: latestPrice[pair],
                    changePct: Number(msg.data.P), // Изменение цены в % за 24ч
                    high24h: Number(msg.data.h),   // Максимум за 24ч
                    low24h: Number(msg.data.l),    // Минимум за 24ч
                    volume24h: Number(msg.data.v), // Объем торгов за 24ч (Base asset)
                    ts: Date.now() 
                });
                processPriceAlertsForPair(pair, latestPrice[pair]).catch(e => console.error('❌ Alert processing error:', e.message));
            }
            else if (streamName === "aggTrade") {
                if (!tradesStore[pair]) tradesStore[pair] = [];
                const trade = {
                    price: Number(msg.data.p),
                    size: Number(msg.data.q),
                    side: msg.data.m ? "sell" : "buy",
                    time: msg.data.T
                };
                tradesStore[pair].push(trade);
                if (tradesStore[pair].length > 50) tradesStore[pair].shift();
                broadcast({ type: "trades", pair, trades: [trade] });
            }
        } catch (e) { console.error("Parse error:", e); }
    });

    binanceWS.on("error", err => {
        console.error("❌ WS Error:", err.message);
    });

    binanceWS.on("close", () => {
        console.log("Reconnecting Binance...");
        setTimeout(connectBinanceWS, 5000);
    });
}

setInterval(() => {
    PRODUCTS.forEach(pair => {
        if (orderbookStore[pair]) {
            broadcast({ type: "orderBook", pair, ...orderbookStore[pair], ts: Date.now() });
        }
    });
}, 200);

wss.on("connection", ws => {
    ws.subscriptions = new Set();
    ws.userId = null;

    ws.on("message", async raw => {
        try {
            const data = JSON.parse(raw.toString());

            if (data.type === "auth" && data.token) {
                const userId = verifyWsAuthToken(data.token);
                if (!userId) {
                    ws.send(JSON.stringify({ type: "authError", error: "UNAUTHORIZED" }));
                    return;
                }
                registerUserWebSocket(userId, ws);
                ws.send(JSON.stringify({ type: "authOk" }));
                return;
            }

            if (data.type === "subscribe" && PRODUCTS.includes(data.pair)) {
                ws.subscriptions.add(data.pair);
                const granularity = normalizeGranularity(data.timeframe || 60);

                await withHistoryLock(data.pair, granularity, async () => {
                    await ensureHistoryLength(data.pair, granularity, INITIAL_HISTORY_CANDLES);
                });

                const fullHistory = (historyStore[data.pair] && historyStore[data.pair][granularity])
                    ? historyStore[data.pair][granularity]
                    : [];

                const initialData = fullHistory.slice(-INITIAL_HISTORY_CANDLES);
                ws.send(JSON.stringify({
                    type: "history",
                    pair: data.pair,
                    data: initialData,
                    timeframe: granularity
                }));

                if (latestPrice[data.pair]) ws.send(JSON.stringify({ type: "price", pair: data.pair, price: latestPrice[data.pair], ts: Date.now() }));
                if (orderbookStore[data.pair]) ws.send(JSON.stringify({ type: "orderBook", pair: data.pair, ...orderbookStore[data.pair] }));
                if (tradesStore[data.pair]) ws.send(JSON.stringify({ type: "trades", pair: data.pair, trades: tradesStore[data.pair].slice(-20) }));
            }

            if (data.type === "loadMore" && PRODUCTS.includes(data.pair)) {
                const granularity = normalizeGranularity(data.timeframe || 60);
                const oldestTime = data.until;

                console.log(`📥 loadMore request: ${data.pair} @ ${granularity}s, before ${new Date(oldestTime * 1000).toISOString()}`);

                await withHistoryLock(data.pair, granularity, async () => {
                    await ensureHistoryBefore(data.pair, granularity, oldestTime);
                });

                const fullHistory = (historyStore[data.pair] && historyStore[data.pair][granularity])
                    ? historyStore[data.pair][granularity]
                    : [];

                const olderCandles = fullHistory.filter(c => c.time < oldestTime);
                const chunk = olderCandles.slice(-COINBASE_MAX_CANDLES_PER_REQUEST);

                console.log(`📤 Found ${chunk.length} older candles to send back`);

                ws.send(JSON.stringify({
                    type: "moreHistory",
                    pair: data.pair,
                    data: chunk,
                    timeframe: granularity
                }));
            }

        } catch (e) { console.error(e); }
    });

    ws.on("close", () => {
        unregisterUserWebSocket(ws);
    });

    ws.on("error", () => {
        unregisterUserWebSocket(ws);
    });
});

const MAIN_SERVER_URL = "https://gitlabebanayahuyna-project-1.onrender.com";

cron.schedule("*/10 * * * *", async () => {
    try {
        await fetch(`${MAIN_SERVER_URL}/api/health`);
    } catch (e) { }
});

cron.schedule("*/1 * * * *", async () => {
    for (const p of PRODUCTS) {
        for (const tf of TIMEFRAMES) {
            await withHistoryLock(p, tf, async () => {
                await refreshLatestHistory(p, tf);
            });
        }
    }
});

async function init() {
    for (const p of PRODUCTS) {
        for (const tf of TIMEFRAMES) {
            await withHistoryLock(p, tf, async () => {
                await ensureHistoryLength(p, tf, INITIAL_HISTORY_CANDLES);
            });
        }
    }
    connectBinanceWS();
    const PORT = process.env.PORT || 3000;
    server.listen(PORT, () => console.log(`🚀 PriceServer running on port ${PORT}`));
}

init();
