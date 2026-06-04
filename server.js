import dotenv from "dotenv";
dotenv.config();
import express from "express";
import crypto from "crypto";
import cors from "cors";
import { Pool } from "pg";
import { validate } from '@telegram-apps/init-data-node';
import rateLimit from 'express-rate-limit';
import cron from "node-cron";
import {
    normalizeMarginMode,
    buildAccountSnapshot,
} from "./margin-utils.js";

const app = express();

app.set('trust proxy', 1);

app.use(cors({
    origin: true,
    credentials: true
}));
app.use(express.json());
app.use(express.static("public"));

const limiter = rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 100,
    standardHeaders: true,
    legacyHeaders: false,
    message: { ok: false, error: "TOO_MANY_REQUESTS" }
});

app.use('/api/', limiter);

const PRICE_SERVER_URL = "https://gitlabebanayahuyna-project.onrender.com";

cron.schedule("*/10 * * * *", async () => {
    console.log("⏰ Anti-Sleep: Pinging Price Server...");
    try {
        const response = await fetch(`${PRICE_SERVER_URL}/health`);
        if (response.ok) console.log("✅ Price Server is awake");
        else console.log("⚠️ Price Server responded with " + response.status);
    } catch (e) {
        console.error("❌ Anti-Sleep Error:", e.message);
    }
});

const DATABASE_URL = process.env.DATABASE_URL;

console.log("=== ENV CHECK ===");
console.log("BOT_TOKEN set:", !!process.env.BOT_TOKEN);
console.log("ADSGRAM_SECRET set:", !!process.env.ADSGRAM_SECRET);
console.log("Using provided NeonDB connection string");
console.log("==================");

if (!process.env.BOT_TOKEN) {
    console.warn("⚠️  BOT_TOKEN not set! Signature verification will fail.");
}

if (!process.env.ADSGRAM_SECRET) {
    console.warn("⚠️  ADSGRAM_SECRET not set! Ad reward endpoint will reject all requests.");
}

const db = new Pool({
    connectionString: DATABASE_URL,
    ssl: {
        rejectUnauthorized: false
    }
});

const BOT_USERNAME = process.env.BOT_USERNAME || "";
const WEBAPP_SHORT_NAME = process.env.WEBAPP_SHORT_NAME || "";

const REFERRAL_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";

const AD_REWARD_AMOUNT = 1;
const DAILY_AD_LIMIT = 5;
const VP_TO_USD_RATE = 0.005;
const MAX_TP_PER_POSITION = 3;
const MAX_SL_PER_POSITION = 3;
const MIN_PARTIAL_PERCENT = 10;
const MAX_PARTIAL_PERCENT = 100;
const SUPPORTED_MARKET_PAIRS = ["BTC-USDT", "ETH-USDT", "SOL-USDT", "BNB-USDT", "XRP-USDT", "DOGE-USDT", "ADA-USDT", "LINK-USDT", "SUI-USDT", "XAUT-USDT"];

// ======================== VAULT CONSTANTS ========================
const VAULT_FRACTION = 0.5;              // 50% P&L трейдера идёт из/в пул
const VAULT_LOCK_APY = 0.15;             // 15% годовых для заблокированных
const VAULT_UNLOCK_APY = 0.07;           // 7% годовых для незаблокированных
const VAULT_LOCK_PERIOD_DAYS = 30;       // Период блокировки в днях
const MAX_VAULT_DRAIN_PER_TRADE = 0.1;   // Макс 10% пула за одну сделку

function makeReferralCode(len = 8) {
    const bytes = crypto.randomBytes(len);
    let out = "";
    for (let i = 0; i < len; i++) {
        out += REFERRAL_ALPHABET[bytes[i] % REFERRAL_ALPHABET.length];
    }
    return out;
}

async function generateUniqueReferralCode() {
    for (let attempt = 0; attempt < 20; attempt++) {
        const code = makeReferralCode(8);
        const check = await db.query("SELECT 1 FROM users WHERE referral_code = $1 LIMIT 1", [code]);
        if (!check.rows.length) return code;
    }
    for (let attempt = 0; attempt < 20; attempt++) {
        const code = makeReferralCode(12);
        const check = await db.query("SELECT 1 FROM users WHERE referral_code = $1 LIMIT 1", [code]);
        if (!check.rows.length) return code;
    }
    throw new Error("REFERRAL_CODE_GENERATION_FAILED");
}

function buildReferralLink(code) {
    if (BOT_USERNAME && WEBAPP_SHORT_NAME) {
        return `https://t.me/${BOT_USERNAME}/${WEBAPP_SHORT_NAME}?startapp=${encodeURIComponent(code)}`;
    }
    if (BOT_USERNAME) {
        return `https://t.me/${BOT_USERNAME}?start=${encodeURIComponent(code)}`;
    }
    return code;
}

function getTodayDateUTC() {
    const now = new Date();
    return now.toISOString().split('T')[0];
}

function normalizePair(p) {
    return String(p || "").trim().replace("/", "-").toUpperCase();
}

function normalizeSupportedMarketPairs(pairs) {
    const allowed = new Set(SUPPORTED_MARKET_PAIRS);
    const out = [];
    for (const pair of Array.isArray(pairs) ? pairs : []) {
        const normalized = normalizePair(pair);
        if (allowed.has(normalized) && !out.includes(normalized)) out.push(normalized);
    }
    return out;
}

function checkAndResetDailyAds(user) {
    const today = getTodayDateUTC();
    const lastResetDate = user.ad_views_reset_date ? user.ad_views_reset_date.toISOString().split('T')[0] : null;

    if (lastResetDate !== today) {
        return {
            needsReset: true,
            dailyAdViews: 0,
            newResetDate: today
        };
    }

    return {
        needsReset: false,
        dailyAdViews: Number(user.daily_ad_views) || 0,
        newResetDate: lastResetDate
    };
}

db.connect()
    .then(client => {
        console.log("✅ Successfully connected to NeonDB (PostgreSQL)");
        client.release();
    })
    .catch(err => {
        console.error("❌ Failed to connect to database:", err.message);
        console.error("Full error:", err);
    });

function checkTelegramAuthInitData(initData) {
    try {
        console.log("🔍 Validating initData with official @telegram-apps/init-data-node library...");
        validate(initData, process.env.BOT_TOKEN);
        console.log("✅ initData signature VALID (library confirmed)!");
        return true;
    } catch (err) {
        console.error("❌ initData validation FAILED:", err.message);
        return false;
    }
}

const COOKIE_NAME = "tg_session";
const AUTH_SECRET = process.env.COOKIE_SECRET || process.env.BOT_TOKEN || crypto.randomBytes(32).toString("hex");
const WS_TOKEN_TTL_MS = 5 * 60 * 1000;

if (!process.env.COOKIE_SECRET && !process.env.BOT_TOKEN) {
    console.warn("⚠️ COOKIE_SECRET/BOT_TOKEN not set; generated ephemeral auth secret for this process.");
}

function getAuthSecret() {
    return AUTH_SECRET;
}

function parseCookies(req) {
    const cookieHeader = req.headers.cookie || "";
    return Object.fromEntries(
        cookieHeader
            .split(";")
            .map(c => c.trim().split("="))
            .filter(p => p.length === 2)
    );
}

function safeEqual(a, b) {
    const left = Buffer.from(String(a || ""), "hex");
    const right = Buffer.from(String(b || ""), "hex");
    return left.length === right.length && crypto.timingSafeEqual(left, right);
}

function makeSessionCookieValue(userId) {
    const secret = getAuthSecret();
    const mac = crypto.createHmac("sha256", secret).update(String(userId)).digest("hex");
    return `${userId}:${mac}`;
}

function verifySessionCookieValue(val) {
    if (!val || typeof val !== "string") return false;
    const [userId, mac] = val.split(":");
    if (!userId || !mac) return false;
    const secret = getAuthSecret();
    const expected = crypto.createHmac("sha256", secret).update(String(userId)).digest("hex");
    return safeEqual(mac, expected) ? userId : false;
}

function getBearerSessionToken(req) {
    const auth = req.headers.authorization || "";
    const match = String(auth).match(/^Bearer\s+(.+)$/i);
    if (match?.[1]) return match[1].trim();
    const fallback = req.headers["x-session-token"];
    return fallback ? String(fallback).trim() : "";
}

function makeWsAuthToken(userId) {
    const expiresAt = Date.now() + WS_TOKEN_TTL_MS;
    const payload = `${userId}:${expiresAt}`;
    const mac = crypto.createHmac("sha256", getAuthSecret()).update(payload).digest("hex");
    return `${payload}:${mac}`;
}

async function getAuthenticatedUserId(req) {
    const cookies = parseCookies(req);
    const userId = verifySessionCookieValue(cookies[COOKIE_NAME]) || verifySessionCookieValue(getBearerSessionToken(req));
    return userId ? String(userId) : null;
}

async function getAuthenticatedUser(req) {
    const userId = await getAuthenticatedUserId(req);
    if (!userId) throw new Error("NO_SESSION");

    const res = await db.query("SELECT user_id, balance FROM users WHERE user_id = $1", [userId]);
    if (!res.rows.length) throw new Error("NO_USER");

    return res.rows[0];
}

function sendAuthError(res, err) {
    if (err.message === "NO_SESSION") {
        return res.status(401).json({ ok: false, error: "UNAUTHORIZED" });
    }
    if (err.message === "NO_USER") {
        return res.status(404).json({ ok: false, error: "NO_USER" });
    }
    return null;
}

function getClientIp(req) {
    const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
    return ip ? ip.split(',')[0].trim() : ip;
}

async function fetchUserPositions(userId) {
    const positionsRes = await db.query("SELECT * FROM positions WHERE user_id = $1 ORDER BY created_at ASC", [userId]);
    return positionsRes.rows;
}

function normalizeAlertPair(pair) {
    if (!pair) return '';
    return String(pair).trim().replace('/', '-').toUpperCase();
}

function normalizeAlertDirection(direction) {
    return String(direction || 'ABOVE').trim().toUpperCase() === 'BELOW' ? 'BELOW' : 'ABOVE';
}

async function syncPriceAlertsToWorker(pair = null) {
    try {
        const payload = pair ? { pair: normalizeAlertPair(pair) } : {};
        await fetch(`${PRICE_SERVER_URL}/internal/price-alerts/refresh`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
    } catch (e) {
        console.warn('⚠️ Failed to sync price alerts with worker:', e.message);
    }
}

async function getUserPriceAlerts(userId, onlyActive = true) {
    const query = onlyActive
      ? "SELECT * FROM price_alerts WHERE user_id = $1 AND status = $2 ORDER BY created_at ASC"
      : "SELECT * FROM price_alerts WHERE user_id = $1 ORDER BY created_at DESC";
    const params = onlyActive ? [String(userId), 'ACTIVE'] : [String(userId)];
    const res = await db.query(query, params);
    return res.rows;
}

async function fetchUserAccountSnapshot(userId, priceMap = {}) {
    const userRes = await db.query("SELECT user_id, balance, margin_mode FROM users WHERE user_id = $1", [userId]);
    const balance = Number(userRes.rows[0]?.balance || 0);
    const positions = await fetchUserPositions(userId);
    const snapshot = buildAccountSnapshot({ balance, positions, priceMap });
    return {
        marginMode: normalizeMarginMode(userRes.rows[0]?.margin_mode),
        ...snapshot,
    };
}

async function initDB() {
    try {
        console.log("🔄 Recreating/Checking DB tables...");

        await db.query(`
      CREATE TABLE IF NOT EXISTS users (
        user_id TEXT PRIMARY KEY,
        first_name TEXT,
        username TEXT,
        photo_url TEXT,
        balance NUMERIC NOT NULL DEFAULT 1000,
        margin_mode TEXT NOT NULL DEFAULT 'isolated',
        last_ip TEXT,
        referral_code TEXT,
        invited_by TEXT,
        invited_at TIMESTAMP,
        ad_views_count INTEGER NOT NULL DEFAULT 0,
        daily_ad_views INTEGER NOT NULL DEFAULT 0,
        ad_views_reset_date DATE,
        last_ad_view TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `);

        try { await db.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS last_ip TEXT`); } catch(e) {}
        try { await db.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS margin_mode TEXT NOT NULL DEFAULT 'isolated'`); } catch(e) {}
        try { await db.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS referral_code TEXT`); } catch(e) {}
        try { await db.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS invited_by TEXT`); } catch(e) {}
        try { await db.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS invited_at TIMESTAMP`); } catch(e) {}
        try { await db.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS ad_views_count INTEGER NOT NULL DEFAULT 0`); } catch(e) {}
        try { await db.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS last_ad_view TIMESTAMP`); } catch(e) {}
        try { await db.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS daily_ad_views INTEGER NOT NULL DEFAULT 0`); } catch(e) {}
        try { await db.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS ad_views_reset_date DATE`); } catch(e) {}

        await db.query(`CREATE UNIQUE INDEX IF NOT EXISTS users_referral_code_uidx ON users(referral_code) WHERE referral_code IS NOT NULL;`);
        await db.query(`CREATE INDEX IF NOT EXISTS users_invited_by_idx ON users(invited_by);`);

        await db.query(`
      CREATE TABLE IF NOT EXISTS positions (
        id BIGSERIAL PRIMARY KEY,
        user_id TEXT REFERENCES users(user_id) ON DELETE CASCADE,
        pair TEXT NOT NULL DEFAULT 'BTC-USDT',
        type TEXT NOT NULL,
        entry_price NUMERIC NOT NULL,
        margin NUMERIC NOT NULL,
        leverage INT NOT NULL,
        size NUMERIC NOT NULL,
        warning_sent BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `);

        try { await db.query(`ALTER TABLE positions ADD COLUMN IF NOT EXISTS pair TEXT DEFAULT 'BTC-USDT'`); } catch(e) {}
        try { await db.query(`ALTER TABLE positions ADD COLUMN IF NOT EXISTS warning_sent BOOLEAN DEFAULT FALSE`); } catch(e) {}

        await db.query(`
      CREATE TABLE IF NOT EXISTS trades_history (
        id BIGSERIAL PRIMARY KEY,
        user_id TEXT REFERENCES users(user_id) ON DELETE CASCADE,
        pair TEXT NOT NULL,
        type TEXT NOT NULL,
        entry_price NUMERIC NOT NULL,
        exit_price NUMERIC NOT NULL,
        size NUMERIC NOT NULL,
        leverage INT NOT NULL,
        pnl NUMERIC NOT NULL,
        commission NUMERIC DEFAULT 0,
        closed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `);

        try { await db.query(`ALTER TABLE trades_history ADD COLUMN IF NOT EXISTS commission NUMERIC DEFAULT 0`); } catch(e) {}

        await db.query(`
      CREATE TABLE IF NOT EXISTS tp_sl_orders (
        id BIGSERIAL PRIMARY KEY,
        position_id BIGINT NOT NULL REFERENCES positions(id) ON DELETE CASCADE,
        user_id TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
        pair TEXT NOT NULL,
        order_type TEXT NOT NULL CHECK (order_type IN ('TP', 'SL')),
        trigger_price NUMERIC NOT NULL,
        size_percent NUMERIC NOT NULL DEFAULT 100 CHECK (size_percent >= 10 AND size_percent <= 100),
        size_amount NUMERIC NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'TRIGGERED', 'CANCELLED')),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        triggered_at TIMESTAMP
      );
    `);

        // Миграция: добавить и гарантировать правильные ограничения для size_amount
        try {
            // Проверим, существует ли колонка
            const checkCol = await db.query(`
                SELECT column_name, is_nullable, column_default FROM information_schema.columns 
                WHERE table_name = 'tp_sl_orders' AND column_name = 'size_amount'
            `);
            
            if (checkCol.rows.length === 0) {
                // Колонка не существует - добавляем с полными ограничениями
                await db.query(`ALTER TABLE tp_sl_orders ADD COLUMN size_amount NUMERIC NOT NULL DEFAULT 0`);
                console.log("✅ Created size_amount column with NOT NULL and DEFAULT 0");
            } else {
                const col = checkCol.rows[0];
                // Если колонка nullable, обновляем её
                if (col.is_nullable === 'YES') {
                    console.log("🔧 Fixing size_amount column constraints...");
                    // Обновляем NULL значения
                    await db.query(`UPDATE tp_sl_orders SET size_amount = 0 WHERE size_amount IS NULL`);
                    // Добавляем NOT NULL
                    await db.query(`ALTER TABLE tp_sl_orders ALTER COLUMN size_amount SET NOT NULL`);
                    // Гарантируем DEFAULT
                    await db.query(`ALTER TABLE tp_sl_orders ALTER COLUMN size_amount SET DEFAULT 0`);
                    console.log("✅ Fixed size_amount column constraints");
                }
                // Если нет DEFAULT, добавляем
                if (!col.column_default || !col.column_default.includes('0')) {
                    console.log("🔧 Adding DEFAULT to size_amount...");
                    await db.query(`ALTER TABLE tp_sl_orders ALTER COLUMN size_amount SET DEFAULT 0`);
                    console.log("✅ Added DEFAULT 0 to size_amount");
                }
            }
        } catch(e) {
            console.error("⚠️ Warning during size_amount migration:", e.message);
        }

        await db.query(`CREATE INDEX IF NOT EXISTS tp_sl_orders_position_idx ON tp_sl_orders(position_id) WHERE status = 'ACTIVE';`);
        await db.query(`CREATE INDEX IF NOT EXISTS tp_sl_orders_status_idx ON tp_sl_orders(status) WHERE status = 'ACTIVE';`);
        await db.query(`CREATE INDEX IF NOT EXISTS tp_sl_orders_user_idx ON tp_sl_orders(user_id);`);

        // ======================== LIMIT ORDERS TABLE ========================
        await db.query(`
            CREATE TABLE IF NOT EXISTS limit_orders (
                id BIGSERIAL PRIMARY KEY,
                user_id TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
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
        await db.query(`CREATE INDEX IF NOT EXISTS limit_orders_user_status_idx ON limit_orders(user_id, status);`);
        await db.query(`CREATE INDEX IF NOT EXISTS limit_orders_status_idx ON limit_orders(status) WHERE status = 'PENDING';`);

        await db.query(`
            CREATE TABLE IF NOT EXISTS price_alerts (
                id BIGSERIAL PRIMARY KEY,
                user_id TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
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

        await db.query(`
            CREATE TABLE IF NOT EXISTS user_market_favorites (
                user_id TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                pair TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, pair)
            );
        `);
        await db.query(`CREATE INDEX IF NOT EXISTS user_market_favorites_user_idx ON user_market_favorites(user_id);`);
        console.log("✅ Market favorites table ready");

        // ======================== VAULT / LIQUIDITY POOL TABLES ========================

        await db.query(`
          CREATE TABLE IF NOT EXISTS vault_pool (
            id SERIAL PRIMARY KEY,
            total_balance NUMERIC NOT NULL DEFAULT 0,
            total_locked_balance NUMERIC NOT NULL DEFAULT 0,
            total_unlocked_balance NUMERIC NOT NULL DEFAULT 0,
            cumulative_pnl NUMERIC NOT NULL DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
          );
        `);

        await db.query(`
          CREATE TABLE IF NOT EXISTS vault_deposits (
            id BIGSERIAL PRIMARY KEY,
            user_id TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
            amount NUMERIC NOT NULL,
            lock_type TEXT NOT NULL CHECK (lock_type IN ('locked', 'unlocked')),
            lock_until TIMESTAMP,
            share_percent NUMERIC NOT NULL DEFAULT 0,
            earned_pnl NUMERIC NOT NULL DEFAULT 0,
            earned_interest NUMERIC NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'withdrawn')),
            deposited_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            withdrawn_at TIMESTAMP
          );
        `);

        await db.query(`
          CREATE TABLE IF NOT EXISTS vault_pnl_history (
            id BIGSERIAL PRIMARY KEY,
            delta NUMERIC NOT NULL,
            reason TEXT NOT NULL,
            trader_user_id TEXT,
            position_id BIGINT,
            fraction_applied NUMERIC NOT NULL DEFAULT 0.5,
            pool_balance_before NUMERIC NOT NULL,
            pool_balance_after NUMERIC NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
          );
        `);

        // Миграция: добавляем новые колонки если их нет
        await db.query(`ALTER TABLE vault_pool ADD COLUMN IF NOT EXISTS total_shares NUMERIC DEFAULT 0;`);
        await db.query(`ALTER TABLE vault_deposits ADD COLUMN IF NOT EXISTS shares NUMERIC DEFAULT 0;`);

        // Инициализация пула если его нет
        const poolCheck = await db.query("SELECT id FROM vault_pool LIMIT 1");
        if (!poolCheck.rows.length) {
          await db.query(
            "INSERT INTO vault_pool (total_balance, total_locked_balance, total_unlocked_balance) VALUES (0, 0, 0)"
          );
          console.log("✅ Vault pool initialized");
        }

        console.log("✅ DB tables ready (including vault tables)!");

        try {
            const missing = await db.query("SELECT user_id FROM users WHERE referral_code IS NULL");
            if (missing.rows.length) {
                console.log(`🔁 Backfill referral_code: ${missing.rows.length} users`);
                for (const row of missing.rows) {
                    const code = await generateUniqueReferralCode();
                    await db.query("UPDATE users SET referral_code = $1 WHERE user_id = $2 AND referral_code IS NULL", [code, row.user_id]);
                }
                console.log("✅ Backfill referral_code done");
            }
        } catch (e) {
            console.error("⚠️ Backfill referral_code failed:", e.message);
        }
    } catch (err) {
        console.error("❌ Error recreating tables:", err.message);
    }
}
await initDB();

async function upsertUserFromObj(userObj, ipAddress, startParamRaw) {
    const userId = String(userObj.id);
    console.log(`📝 Upserting user ${userId} (${userObj.first_name || "No name"}). IP: ${ipAddress}`);

    const startParam = startParamRaw ? String(startParamRaw).trim() : "";

    try {
        await db.query("BEGIN");

        const existingRes = await db.query(
            "SELECT user_id, referral_code, invited_by FROM users WHERE user_id = $1 FOR UPDATE",
            [userId]
        );

        let referralCode = existingRes.rows[0]?.referral_code || null;
        let invitedBy = existingRes.rows[0]?.invited_by || null;
        let invitedAt = null;

        if (!referralCode) {
            referralCode = await generateUniqueReferralCode();
        }

        if (!invitedBy && startParam) {
            const inviterRes = await db.query(
                "SELECT user_id FROM users WHERE referral_code = $1 LIMIT 1",
                [startParam]
            );
            const inviterId = inviterRes.rows[0]?.user_id || null;
            if (inviterId && inviterId !== userId) {
                invitedBy = inviterId;
                invitedAt = new Date();
            }
        }

        if (!existingRes.rows.length) {
            await db.query(`
        INSERT INTO users (user_id, first_name, username, photo_url, last_ip, referral_code, invited_by, invited_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      `, [
                userId,
                userObj.first_name || null,
                userObj.username || null,
                userObj.photo_url || null,
                ipAddress,
                referralCode,
                invitedBy,
                invitedAt
            ]);
        } else {
            await db.query(`
        UPDATE users SET
          first_name = $2,
          username = $3,
          photo_url = $4,
          last_ip = $5,
          referral_code = $6,
          invited_by = COALESCE(users.invited_by, $7),
          invited_at = COALESCE(users.invited_at, $8),
          updated_at = CURRENT_TIMESTAMP
        WHERE user_id = $1
      `, [
                userId,
                userObj.first_name || null,
                userObj.username || null,
                userObj.photo_url || null,
                ipAddress,
                referralCode,
                invitedBy,
                invitedAt
            ]);
        }

        await db.query("COMMIT");

        const res = await db.query(
            "SELECT user_id, first_name, username, photo_url, balance, margin_mode, referral_code, invited_by, invited_at, ad_views_count, daily_ad_views, ad_views_reset_date FROM users WHERE user_id = $1",
            [userId]
        );
        return res.rows[0];
    } catch (err) {
        try { await db.query("ROLLBACK"); } catch (e) {}
        console.error(`❌ Error saving user ${userId}:`, err.message);
        throw err;
    }
}

app.use((req, res, next) => {
    const ip = getClientIp(req);
    console.log(`\n📡 [${new Date().toISOString()}] ${req.method} ${req.path} [IP: ${ip}]`);
    if (req.body && Object.keys(req.body).length > 0) console.log("Body:", req.body);
    next();
});

app.get("/auth/telegram", async (req, res) => {
    res.json({msg: "Endpoint exists"});
});

app.post("/api/init", async (req, res) => {
    console.log("\n🚀 /api/init called!");
    const ip = getClientIp(req);

    try {
        const { initData, referralCode: referralCodeFromBody } = req.body;
        let userRow;

        if (initData) {
            const sigValid = checkTelegramAuthInitData(initData);

            if (!sigValid && process.env.DEV_ALLOW_BYPASS !== "1") {
                console.log("❌ Signature invalid and no bypass — rejecting");
                return res.status(403).json({ ok: false, error: "INVALID_SIGNATURE" });
            }

            const params = new URLSearchParams(initData);
            params.delete("signature");
            const rawUser = params.get("user");
            if (!rawUser) return res.status(400).json({ ok: false, error: "NO_USER" });

            const startParam = params.get("start_param") || referralCodeFromBody || "";

            let userObj;
            try {
                userObj = JSON.parse(rawUser);
            } catch (e) {
                return res.status(400).json({ ok: false, error: "INVALID_USER_JSON" });
            }

            userRow = await upsertUserFromObj(userObj, ip, startParam);
        } else {
            const userId = await getAuthenticatedUserId(req);

            if (!userId) return res.status(401).json({ ok: false, error: "NO_SESSION" });

            const ures = await db.query(
                "SELECT user_id, first_name, username, photo_url, balance, margin_mode, ad_views_count, daily_ad_views, ad_views_reset_date FROM users WHERE user_id = $1",
                [userId]
            );
            if (!ures.rows.length) return res.status(404).json({ ok: false, error: "NO_USER" });
            userRow = ures.rows[0];
        }

        const dailyStatus = checkAndResetDailyAds(userRow);
        if (dailyStatus.needsReset) {
            await db.query(
                "UPDATE users SET daily_ad_views = 0, ad_views_reset_date = $1 WHERE user_id = $2",
                [dailyStatus.newResetDate, userRow.user_id]
            );
            userRow.daily_ad_views = 0;
            userRow.ad_views_reset_date = dailyStatus.newResetDate;
        }

        const positionsRes = await db.query(
            "SELECT * FROM positions WHERE user_id = $1 ORDER BY created_at ASC",
            [userRow.user_id]
        );

        const tpSlRes = await db.query(
            "SELECT * FROM tp_sl_orders WHERE user_id = $1 AND status = 'ACTIVE' ORDER BY created_at ASC",
            [userRow.user_id]
        );

        const limitOrdersRes = await db.query(
            "SELECT * FROM limit_orders WHERE user_id = $1 AND status = 'PENDING' ORDER BY created_at DESC",
            [userRow.user_id]
        );

        const marketFavoritesRes = await db.query(
            "SELECT pair FROM user_market_favorites WHERE user_id = $1 ORDER BY created_at ASC",
            [userRow.user_id]
        );

        const cookieVal = makeSessionCookieValue(userRow.user_id);
        const isSecure = req.headers["x-forwarded-proto"] === "https" || req.protocol === "https";

        const sameSite = isSecure ? "SameSite=None" : "SameSite=Lax";
        const cookieParts = [`${COOKIE_NAME}=${cookieVal}`, `Path=/`, `HttpOnly`, sameSite, `Max-Age=${60 * 60 * 24 * 30}`];
        if (isSecure) cookieParts.push("Secure");
        res.setHeader("Set-Cookie", cookieParts.join("; "));

        const account = buildAccountSnapshot({ balance: Number(userRow.balance || 0), positions: positionsRes.rows, priceMap: {} });

        res.json({
            ok: true,
            user: {
                ...userRow,
                margin_mode: normalizeMarginMode(userRow.margin_mode),
                daily_ad_views: dailyStatus.dailyAdViews,
                daily_ad_limit: DAILY_AD_LIMIT,
                vp_to_usd_rate: VP_TO_USD_RATE
            },
            positions: positionsRes.rows,
            tpSlOrders: tpSlRes.rows,
            limitOrders: limitOrdersRes.rows,
            marketFavorites: marketFavoritesRes.rows.map(row => row.pair),
            account,
            sessionToken: cookieVal,
            wsAuthToken: makeWsAuthToken(userRow.user_id)
        });

    } catch (err) {
        console.error("💥 UNHANDLED ERROR in /api/init:", err);
        res.status(500).json({ ok: false, error: "SERVER_ERROR" });
    }
});

app.get("/api/account/summary", async (req, res) => {
    try {
        const userId = await getAuthenticatedUserId(req);
        if (!userId) return res.status(401).json({ ok: false, error: "UNAUTHORIZED" });

        const account = await fetchUserAccountSnapshot(userId, {});
        res.json({ ok: true, account });
    } catch (err) {
        console.error("Error fetching account summary:", err);
        res.status(500).json({ ok: false, error: "SERVER_ERROR" });
    }
});

app.get("/api/user/referrals", async (req, res) => {
    try {
        const userId = await getAuthenticatedUserId(req);
        if (!userId) return res.status(401).json({ ok: false, error: "UNAUTHORIZED" });

        const userRes = await db.query(
            "SELECT user_id, referral_code FROM users WHERE user_id = $1",
            [userId]
        );
        if (!userRes.rows.length) return res.status(404).json({ ok: false, error: "NO_USER" });

        let referralCode = userRes.rows[0].referral_code;
        if (!referralCode) {
            referralCode = await generateUniqueReferralCode();
            await db.query("UPDATE users SET referral_code = $1 WHERE user_id = $2 AND referral_code IS NULL", [referralCode, userId]);
        }

        const invitedRes = await db.query(
            `SELECT user_id, first_name, username, photo_url, invited_at, created_at
             FROM users
             WHERE invited_by = $1
             ORDER BY invited_at DESC NULLS LAST, created_at DESC
             LIMIT 50`,
            [userId]
        );

        const countRes = await db.query(
            "SELECT COUNT(*)::int AS cnt FROM users WHERE invited_by = $1",
            [userId]
        );

        res.json({
            ok: true,
            referralCode,
            referralLink: buildReferralLink(referralCode),
            invitedCount: countRes.rows[0]?.cnt || 0,
            invited: invitedRes.rows
        });
    } catch (err) {
        console.error("Error fetching referrals:", err);
        res.status(500).json({ ok: false, error: "SERVER_ERROR" });
    }
});

app.get("/api/user/history", async (req, res) => {
    try {
        const userId = await getAuthenticatedUserId(req);
        if (!userId) return res.status(401).json({ ok: false, error: "UNAUTHORIZED" });

        const historyRes = await db.query(
            "SELECT * FROM trades_history WHERE user_id = $1 ORDER BY closed_at DESC LIMIT 50",
            [userId]
        );

        res.json({ ok: true, history: historyRes.rows });
    } catch (err) {
        console.error("Error fetching history:", err);
        res.status(500).json({ ok: false, error: "SERVER_ERROR" });
    }
});

app.get("/api/user/market-favorites", async (req, res) => {
    try {
        const userId = await getAuthenticatedUserId(req);
        if (!userId) return res.status(401).json({ ok: false, error: "UNAUTHORIZED" });

        const favRes = await db.query(
            "SELECT pair FROM user_market_favorites WHERE user_id = $1 ORDER BY created_at ASC",
            [userId]
        );

        res.json({ ok: true, favorites: favRes.rows.map(row => row.pair) });
    } catch (err) {
        console.error("Error fetching market favorites:", err);
        res.status(500).json({ ok: false, error: "SERVER_ERROR" });
    }
});

app.post("/api/user/market-favorites", async (req, res) => {
    let client;
    try {
        const userId = await getAuthenticatedUserId(req);
        if (!userId) {
            return res.status(401).json({ ok: false, error: "UNAUTHORIZED" });
        }

        const favorites = normalizeSupportedMarketPairs(req.body?.favorites);

        client = await db.connect();
        await client.query("BEGIN");
        await client.query("DELETE FROM user_market_favorites WHERE user_id = $1", [userId]);
        for (const pair of favorites) {
            await client.query(
                "INSERT INTO user_market_favorites (user_id, pair) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                [userId, pair]
            );
        }
        await client.query("COMMIT");

        res.json({ ok: true, favorites });
    } catch (err) {
        if (client) {
            try { await client.query("ROLLBACK"); } catch (_) {}
        }
        console.error("Error saving market favorites:", err);
        res.status(500).json({ ok: false, error: "SERVER_ERROR" });
    } finally {
        if (client) client.release();
    }
});

app.get("/api/adsgram/reward", async (req, res) => {
    console.log("\n🎬 /api/adsgram/reward called!");
    console.log("Query params:", req.query);

    try {
        const { userid, secret } = req.query;

        if (!userid) {
            console.log("❌ Missing userid parameter");
            return res.status(400).json({ ok: false, error: "MISSING_USERID" });
        }

        if (!secret) {
            console.log("❌ Missing secret parameter");
            return res.status(400).json({ ok: false, error: "MISSING_SECRET" });
        }

        const expectedSecret = process.env.ADSGRAM_SECRET;
        if (!expectedSecret) {
            console.error("❌ ADSGRAM_SECRET not configured on server");
            return res.status(500).json({ ok: false, error: "SERVER_CONFIG_ERROR" });
        }

        if (secret !== expectedSecret) {
            console.log("❌ Invalid secret provided");
            return res.status(403).json({ ok: false, error: "INVALID_SECRET" });
        }

        const userId = String(userid).trim();

        const userCheck = await db.query(
            "SELECT user_id, balance, ad_views_count, daily_ad_views, ad_views_reset_date FROM users WHERE user_id = $1",
            [userId]
        );

        if (!userCheck.rows.length) {
            console.log(`❌ User ${userId} not found in database`);
            return res.status(404).json({ ok: false, error: "USER_NOT_FOUND" });
        }

        const user = userCheck.rows[0];
        const dailyStatus = checkAndResetDailyAds(user);

        if (dailyStatus.needsReset) {
            await db.query(
                "UPDATE users SET daily_ad_views = 0, ad_views_reset_date = $1 WHERE user_id = $2",
                [dailyStatus.newResetDate, userId]
            );
            user.daily_ad_views = 0;
        }

        const currentDailyViews = dailyStatus.dailyAdViews;

        if (currentDailyViews >= DAILY_AD_LIMIT) {
            console.log(`⚠️ User ${userId} reached daily ad limit (${currentDailyViews}/${DAILY_AD_LIMIT})`);
            return res.status(429).json({
                ok: false,
                error: "DAILY_LIMIT_REACHED",
                dailyAdViews: currentDailyViews,
                dailyAdLimit: DAILY_AD_LIMIT,
                message: `Daily limit of ${DAILY_AD_LIMIT} ads reached. Try again tomorrow!`
            });
        }

        await db.query(`
            UPDATE users
            SET balance = balance + $1,
                ad_views_count = ad_views_count + 1,
                daily_ad_views = daily_ad_views + 1,
                ad_views_reset_date = COALESCE(ad_views_reset_date, CURRENT_DATE),
                last_ad_view = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE user_id = $2
        `, [AD_REWARD_AMOUNT, userId]);

        const updatedUser = await db.query(
            "SELECT balance, ad_views_count, daily_ad_views FROM users WHERE user_id = $1",
            [userId]
        );

        const newDailyViews = Number(updatedUser.rows[0].daily_ad_views);
        const remainingToday = DAILY_AD_LIMIT - newDailyViews;

        console.log(`✅ Ad reward granted to user ${userId}: +${AD_REWARD_AMOUNT} VP`);
        console.log(`   New balance: ${updatedUser.rows[0].balance}, Daily views: ${newDailyViews}/${DAILY_AD_LIMIT}, Remaining: ${remainingToday}`);

        res.json({
            ok: true,
            reward: AD_REWARD_AMOUNT,
            newBalance: Number(updatedUser.rows[0].balance),
            totalViews: Number(updatedUser.rows[0].ad_views_count),
            dailyAdViews: newDailyViews,
            dailyAdLimit: DAILY_AD_LIMIT,
            remainingToday: remainingToday
        });

    } catch (err) {
        console.error("💥 Error in /api/adsgram/reward:", err);
        res.status(500).json({ ok: false, error: "SERVER_ERROR" });
    }
});

app.get("/api/user/ad-stats", async (req, res) => {
    try {
        const userId = await getAuthenticatedUserId(req);
        if (!userId) return res.status(401).json({ ok: false, error: "UNAUTHORIZED" });

        const userRes = await db.query(
            "SELECT ad_views_count, daily_ad_views, ad_views_reset_date, last_ad_view, balance FROM users WHERE user_id = $1",
            [userId]
        );

        if (!userRes.rows.length) {
            return res.status(404).json({ ok: false, error: "USER_NOT_FOUND" });
        }

        const user = userRes.rows[0];
        const dailyStatus = checkAndResetDailyAds(user);

        if (dailyStatus.needsReset) {
            await db.query(
                "UPDATE users SET daily_ad_views = 0, ad_views_reset_date = $1 WHERE user_id = $2",
                [dailyStatus.newResetDate, userId]
            );
        }

        const currentDailyViews = dailyStatus.dailyAdViews;
        const remainingToday = DAILY_AD_LIMIT - currentDailyViews;

        res.json({
            ok: true,
            adViewsCount: Number(user.ad_views_count) || 0,
            dailyAdViews: currentDailyViews,
            dailyAdLimit: DAILY_AD_LIMIT,
            remainingToday: Math.max(0, remainingToday),
            lastAdView: user.last_ad_view,
            balance: Number(user.balance),
            vpToUsdRate: VP_TO_USD_RATE
        });
    } catch (err) {
        console.error("Error fetching ad stats:", err);
        res.status(500).json({ ok: false, error: "SERVER_ERROR" });
    }
});

app.post("/api/order/open", async (req, res) => {
  // Получаем соединение из пула для транзакции
  const client = await db.connect();

  try {
    const user = await getAuthenticatedUser(req);
    // Добавляем idempotencyKey (генерируется на фронте) для защиты от дублей
    const { pair, type, size, leverage, entryPrice, idempotencyKey } = req.body;

    if (!pair || !type || !size || !leverage || !entryPrice) {
      return res.status(400).json({ ok: false, error: "MISSING_FIELDS" });
    }

    // 1. НАЧАЛО ТРАНЗАКЦИИ
    await client.query("BEGIN");

    // Блокируем строку пользователя для обновления (защита от race condition)
    const userRes = await client.query("SELECT balance FROM users WHERE user_id = $1 FOR UPDATE", [user.user_id]);
    const currentBalance = Number(userRes.rows[0].balance);
    const margin = Number(size) / Number(leverage);

    if (margin > currentBalance) {
      await client.query("ROLLBACK");
      return res.status(400).json({ ok: false, error: "INSUFFICIENT_BALANCE" });
    }

    // Списываем баланс
    await client.query(
      "UPDATE users SET balance = balance - $1 WHERE user_id = $2",
      [margin, user.user_id]
    );

    // Создаем позицию
    const posRes = await client.query(`
      INSERT INTO positions (user_id, pair, type, entry_price, margin, leverage, size)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      RETURNING *
    `, [user.user_id, pair, type, entryPrice, margin, leverage, size]);

    // 2. ФИКСАЦИЯ ТРАНЗАКЦИИ
    await client.query("COMMIT");

    res.json({
      ok: true,
      position: posRes.rows[0],
      newBalance: currentBalance - margin
    });

  } catch (err) {
    // В случае любой ошибки отменяем все изменения
    await client.query("ROLLBACK");
    if (sendAuthError(res, err)) return;
    console.error("Error opening position:", err.message);
    res.status(500).json({ ok: false, error: err.message });
  } finally {
    // Освобождаем клиент обратно в пул
    client.release();
  }
});

app.post("/api/order/close", async (req, res) => {
    try {
        const user = await getAuthenticatedUser(req);
        const { positionId, closePrice } = req.body;

        if (!positionId || !closePrice) return res.status(400).json({ ok: false, error: "MISSING_FIELDS" });

        const posRes = await db.query(
            "SELECT * FROM positions WHERE id = $1 AND user_id = $2",
            [positionId, user.user_id]
        );

        if (!posRes.rows.length) return res.status(404).json({ ok: false, error: "POSITION_NOT_FOUND" });
        const pos = posRes.rows[0];

        const cPrice = Number(closePrice);
        const ePrice = Number(pos.entry_price);
        const pSize = Number(pos.size);
        const pMargin = Number(pos.margin);

        const priceChangePct = (cPrice - ePrice) / ePrice;
        let pnl = priceChangePct * pSize;
        if (pos.type === "SHORT") pnl = -pnl;

        const commission = pSize * 0.0003;

        let totalReturn = pMargin + pnl - commission;

        let isLiquidated = false;
        if (totalReturn <= 0) {
            isLiquidated = true;
            totalReturn = 0;
            pnl = commission - pMargin;
        }

        let client = null;
        try {
            client = await db.connect();
            await client.query("BEGIN");

            // Применяем P&L к пулу
            const vaultDelta = await applyTraderPnlToVault(
                client, 
                pnl, 
                'trader_close', 
                user.user_id, 
                positionId
            );

            if (totalReturn > 0) {
                await client.query("UPDATE users SET balance = balance + $1 WHERE user_id = $2", [totalReturn, user.user_id]);
            }

            const finalCommission = commission;

            await client.query(`
              INSERT INTO trades_history (user_id, pair, type, entry_price, exit_price, size, leverage, pnl, commission)
              VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            `, [user.user_id, pos.pair || 'BTC-USDT', pos.type, ePrice, cPrice, pSize, pos.leverage, pnl, finalCommission]);

            await client.query("DELETE FROM positions WHERE id = $1", [positionId]);

            await client.query("COMMIT");

            const newBalRes = await db.query("SELECT balance FROM users WHERE user_id = $1", [user.user_id]);

            console.log(`✅ ${isLiquidated ? 'LIQUIDATED' : 'CLOSED'} | PnL: ${pnl.toFixed(2)} | Vault delta: ${vaultDelta.toFixed(2)}`);

            res.json({
                ok: true,
                pnl: Number(pnl.toFixed(2)),
                commission: Number(finalCommission.toFixed(2)),
                liquidated: isLiquidated,
                newBalance: Number(newBalRes.rows[0].balance)
            });

        } catch (err) {
            if (client) {
                try { await client.query("ROLLBACK"); } catch (e) {}
            }
            if (sendAuthError(res, err)) return;
            console.error("❌ Ошибка закрытия позиции:", err.message);
            res.status(500).json({ ok: false, error: err.message });
        } finally {
            if (client) {
                try { client.release(); } catch (e) {}
            }
        }
        return;
        
        } catch (err) {
        res.status(500).json({ ok: false, error: err.message });
    }
});

// ======================== TP/SL ENDPOINTS ========================

// ======================== VAULT HELPER FUNCTIONS ========================

async function getVaultPool(client = db) {
  const res = await client.query("SELECT * FROM vault_pool LIMIT 1");
  return res.rows[0] || {
    total_balance: 0,
    total_shares: 0,
    total_locked_balance: 0,
    total_unlocked_balance: 0,
    cumulative_pnl: 0
  };
}

// Применить P&L трейдера к пулу (share-based, Hyperliquid модель)
async function applyTraderPnlToVault(client, traderPnl, reason, traderId, positionId) {
  const pool = await getVaultPool(client);
  const poolBalance = Number(pool.total_balance);

  if (poolBalance <= 0) return 0; // Пул пуст

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

  // Обновляем только total_balance — total_shares НЕ трогаем (это core share-model)
  await client.query(`
    UPDATE vault_pool SET
      total_balance = $1,
      cumulative_pnl = cumulative_pnl + $2,
      updated_at = CURRENT_TIMESTAMP
    WHERE id = 1
  `, [newBalance, lpDelta]);

  // Логируем в историю
  await client.query(`
    INSERT INTO vault_pnl_history
      (delta, reason, trader_user_id, position_id, fraction_applied, pool_balance_before, pool_balance_after)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
  `, [lpDelta, reason, traderId, positionId, VAULT_FRACTION, poolBalance, newBalance]);

  return lpDelta;
}


app.post("/api/tp-sl/create", async (req, res) => {
    let client = null;
    try {
        const user = await getAuthenticatedUser(req);
        const { positionId, orderType, triggerPrice, sizePercent } = req.body;

        if (!positionId || !orderType || !triggerPrice) {
            return res.status(400).json({ ok: false, error: "MISSING_FIELDS" });
        }

        const normalizedType = String(orderType).toUpperCase();
        if (normalizedType !== 'TP' && normalizedType !== 'SL') {
            return res.status(400).json({ ok: false, error: "INVALID_ORDER_TYPE" });
        }

        const trigPrice = Number(triggerPrice);
        if (isNaN(trigPrice) || trigPrice <= 0) {
            return res.status(400).json({ ok: false, error: "INVALID_TRIGGER_PRICE" });
        }

        const percent = Number(sizePercent) || 100;
        if (percent < MIN_PARTIAL_PERCENT || percent > MAX_PARTIAL_PERCENT) {
            return res.status(400).json({ ok: false, error: `SIZE_PERCENT_MUST_BE_${MIN_PARTIAL_PERCENT}_TO_${MAX_PARTIAL_PERCENT}` });
        }

        // Правильное получение клиента из пула
        client = await db.connect();

        await client.query("BEGIN");

        const posRes = await client.query(
            "SELECT * FROM positions WHERE id = $1 AND user_id = $2 FOR UPDATE",
            [positionId, user.user_id]
        );

        if (!posRes.rows.length) {
            await client.query("ROLLBACK");
            return res.status(404).json({ ok: false, error: "POSITION_NOT_FOUND" });
        }

        const pos = posRes.rows[0];
        const entryPrice = Number(pos.entry_price);
        const posSize = Number(pos.size);
        const posType = pos.type.toUpperCase();

        // Валидация TP: для LONG должен быть выше entry, для SHORT — ниже
        if (normalizedType === 'TP') {
            if (posType === 'LONG' && trigPrice <= entryPrice) {
                await client.query("ROLLBACK");
                return res.status(400).json({ ok: false, error: "TP_MUST_BE_ABOVE_ENTRY_FOR_LONG" });
            }
            if (posType === 'SHORT' && trigPrice >= entryPrice) {
                await client.query("ROLLBACK");
                return res.status(400).json({ ok: false, error: "TP_MUST_BE_BELOW_ENTRY_FOR_SHORT" });
            }
        }

        // Валидация SL: для LONG должен быть ниже entry, для SHORT — выше
        if (normalizedType === 'SL') {
            if (posType === 'LONG' && trigPrice >= entryPrice) {
                await client.query("ROLLBACK");
                return res.status(400).json({ ok: false, error: "SL_MUST_BE_BELOW_ENTRY_FOR_LONG" });
            }
            if (posType === 'SHORT' && trigPrice <= entryPrice) {
                await client.query("ROLLBACK");
                return res.status(400).json({ ok: false, error: "SL_MUST_BE_ABOVE_ENTRY_FOR_SHORT" });
            }
        }

        const existingOrders = await client.query(
            "SELECT * FROM tp_sl_orders WHERE position_id = $1 AND status = 'ACTIVE'",
            [positionId]
        );

        const tpCount = existingOrders.rows.filter(o => o.order_type === 'TP').length;
        const slCount = existingOrders.rows.filter(o => o.order_type === 'SL').length;

        if (normalizedType === 'TP' && tpCount >= MAX_TP_PER_POSITION) {
            await client.query("ROLLBACK");
            return res.status(400).json({ ok: false, error: `MAX_${MAX_TP_PER_POSITION}_TP_ORDERS_REACHED` });
        }

        if (normalizedType === 'SL' && slCount >= MAX_SL_PER_POSITION) {
            await client.query("ROLLBACK");
            return res.status(400).json({ ok: false, error: `MAX_${MAX_SL_PER_POSITION}_SL_ORDERS_REACHED` });
        }

        const sameTypeOrders = existingOrders.rows.filter(o => o.order_type === normalizedType);
        const usedPercent = sameTypeOrders.reduce((sum, o) => sum + Number(o.size_percent), 0);
        const availablePercent = 100 - usedPercent;

        if (percent > availablePercent) {
            await client.query("ROLLBACK");
            return res.status(400).json({
                ok: false,
                error: "EXCEEDS_AVAILABLE_VOLUME",
                availablePercent: Math.floor(availablePercent),
                usedPercent: Math.ceil(usedPercent)
            });
        }

        const duplicatePrice = sameTypeOrders.find(o => Math.abs(Number(o.trigger_price) - trigPrice) < 0.0001);
        if (duplicatePrice) {
            await client.query("ROLLBACK");
            return res.status(400).json({ ok: false, error: "DUPLICATE_TRIGGER_PRICE" });
        }

        // Вычисляем size_amount - абсолютный размер позиции для закрытия
        let sizeAmount = 0;
        if (posSize && !isNaN(posSize) && posSize > 0 && !isNaN(percent) && percent > 0) {
            sizeAmount = (Number(posSize) * Number(percent)) / 100;
            if (isNaN(sizeAmount) || !isFinite(sizeAmount)) {
                sizeAmount = 0;
            }
        }
        // Гарантированно не передаём NULL - используем 0 как fallback
        sizeAmount = Math.max(0, Number(sizeAmount) || 0);

        const orderRes = await client.query(`
            INSERT INTO tp_sl_orders (position_id, user_id, pair, order_type, trigger_price, size_percent, size_amount, status)
            VALUES ($1, $2, $3, $4, $5, $6, COALESCE($7, 0), 'ACTIVE')
            RETURNING *
        `, [positionId, user.user_id, pos.pair, normalizedType, trigPrice, percent, sizeAmount]);

        await client.query("COMMIT");

        const allOrders = await db.query(
            "SELECT * FROM tp_sl_orders WHERE position_id = $1 AND status = 'ACTIVE' ORDER BY created_at ASC",
            [positionId]
        );

        console.log(`✅ ${normalizedType} order created for position ${positionId}: price=${trigPrice}, size=${percent}% (${sizeAmount.toFixed(2)} VP)`);

        res.json({
            ok: true,
            order: orderRes.rows[0],
            allOrders: allOrders.rows,
            tpCount: allOrders.rows.filter(o => o.order_type === 'TP').length,
            slCount: allOrders.rows.filter(o => o.order_type === 'SL').length
        });

    } catch (err) {
        if (client) {
            try { await client.query("ROLLBACK"); } catch (e) {}
        }
        if (sendAuthError(res, err)) return;
        console.error("❌ Error creating TP/SL:", err.message);
        res.status(500).json({ ok: false, error: err.message });
    } finally {
        if (client) {
            try { client.release(); } catch (e) {}
        }
    }
});

app.post("/api/tp-sl/delete", async (req, res) => {
    try {
        const user = await getAuthenticatedUser(req);
        const { orderId } = req.body;

        if (!orderId) {
            return res.status(400).json({ ok: false, error: "MISSING_ORDER_ID" });
        }

        const orderRes = await db.query(
            "SELECT * FROM tp_sl_orders WHERE id = $1 AND user_id = $2 AND status = 'ACTIVE'",
            [orderId, user.user_id]
        );

        if (!orderRes.rows.length) {
            return res.status(404).json({ ok: false, error: "ORDER_NOT_FOUND" });
        }

        const order = orderRes.rows[0];

        await db.query(
            "UPDATE tp_sl_orders SET status = 'CANCELLED' WHERE id = $1",
            [orderId]
        );

        const allOrders = await db.query(
            "SELECT * FROM tp_sl_orders WHERE position_id = $1 AND status = 'ACTIVE' ORDER BY created_at ASC",
            [order.position_id]
        );

        console.log(`✅ ${order.order_type} order ${orderId} cancelled for position ${order.position_id}`);

        res.json({
            ok: true,
            deletedOrderId: orderId,
            allOrders: allOrders.rows,
            tpCount: allOrders.rows.filter(o => o.order_type === 'TP').length,
            slCount: allOrders.rows.filter(o => o.order_type === 'SL').length
        });

    } catch (err) {
        if (sendAuthError(res, err)) return;
        console.error("❌ Error deleting TP/SL:", err.message);
        res.status(500).json({ ok: false, error: err.message });
    }
});

app.get("/api/tp-sl/list", async (req, res) => {
    try {
        const userId = await getAuthenticatedUserId(req);
        if (!userId) return res.status(401).json({ ok: false, error: "UNAUTHORIZED" });

        const positionId = req.query.positionId;

        let ordersRes;
        if (positionId) {
            ordersRes = await db.query(
                "SELECT * FROM tp_sl_orders WHERE position_id = $1 AND user_id = $2 AND status = 'ACTIVE' ORDER BY created_at ASC",
                [positionId, userId]
            );
        } else {
            ordersRes = await db.query(
                "SELECT * FROM tp_sl_orders WHERE user_id = $1 AND status = 'ACTIVE' ORDER BY created_at ASC",
                [userId]
            );
        }

        const orders = ordersRes.rows;
        const tpOrders = orders.filter(o => o.order_type === 'TP');
        const slOrders = orders.filter(o => o.order_type === 'SL');

        res.json({
            ok: true,
            orders,
            tpCount: tpOrders.length,
            slCount: slOrders.length,
            tpUsedPercent: tpOrders.reduce((s, o) => s + Number(o.size_percent), 0),
            slUsedPercent: slOrders.reduce((s, o) => s + Number(o.size_percent), 0)
        });

    } catch (err) {
        console.error("Error fetching TP/SL orders:", err);
        res.status(500).json({ ok: false, error: "SERVER_ERROR" });
    }
});

// ======================== LIMIT ORDER ENDPOINTS ========================

app.post("/api/limit-order/create", async (req, res) => {
    const client = await db.connect();
    try {
        const user = await getAuthenticatedUser(req);
        const { pair, type, limitPrice, margin, leverage, size } = req.body;

        if (!pair || !type || !limitPrice || !margin || !leverage || !size) {
            return res.status(400).json({ ok: false, error: "MISSING_FIELDS" });
        }

        const normalizedType = String(type).toUpperCase();
        if (normalizedType !== 'LONG' && normalizedType !== 'SHORT') {
            return res.status(400).json({ ok: false, error: "INVALID_TYPE" });
        }

        const lPrice = Number(limitPrice);
        const mAmount = Number(margin);
        const sAmount = Number(size);
        const lev = Number(leverage);

        if (lPrice <= 0 || mAmount <= 0 || sAmount <= 0 || lev < 1) {
            return res.status(400).json({ ok: false, error: "INVALID_VALUES" });
        }

        await client.query("BEGIN");

        const userRes = await client.query(
            "SELECT balance FROM users WHERE user_id = $1 FOR UPDATE",
            [user.user_id]
        );
        const currentBalance = Number(userRes.rows[0].balance);

        if (mAmount > currentBalance) {
            await client.query("ROLLBACK");
            return res.status(400).json({ ok: false, error: "INSUFFICIENT_BALANCE" });
        }

        // Deduct margin
        await client.query(
            "UPDATE users SET balance = balance - $1 WHERE user_id = $2",
            [mAmount, user.user_id]
        );

        const orderRes = await client.query(`
            INSERT INTO limit_orders (user_id, pair, type, limit_price, margin, leverage, size, status)
            VALUES ($1, $2, $3, $4, $5, $6, $7, 'PENDING')
            RETURNING *
        `, [user.user_id, pair, normalizedType, lPrice, mAmount, lev, sAmount]);

        await client.query("COMMIT");

        const newBalRes = await client.query("SELECT balance FROM users WHERE user_id = $1", [user.user_id]);

        console.log(`✅ Limit order created: ${normalizedType} ${pair} @ ${lPrice}, margin=${mAmount}`);

        res.json({
            ok: true,
            order: orderRes.rows[0],
            newBalance: Number(newBalRes.rows[0].balance)
        });
    } catch (err) {
        try { await client.query("ROLLBACK"); } catch(e) {}
        if (sendAuthError(res, err)) return;
        console.error("Error creating limit order:", err.message);
        res.status(500).json({ ok: false, error: err.message });
    } finally {
        client.release();
    }
});

app.post("/api/limit-order/cancel", async (req, res) => {
    const client = await db.connect();
    try {
        const user = await getAuthenticatedUser(req);
        const { orderId } = req.body;

        if (!orderId) return res.status(400).json({ ok: false, error: "MISSING_ORDER_ID" });

        await client.query("BEGIN");

        const orderRes = await client.query(
            "SELECT * FROM limit_orders WHERE id = $1 AND user_id = $2 AND status = 'PENDING' FOR UPDATE",
            [orderId, user.user_id]
        );

        if (!orderRes.rows.length) {
            await client.query("ROLLBACK");
            return res.status(404).json({ ok: false, error: "ORDER_NOT_FOUND" });
        }

        const order = orderRes.rows[0];

        // Refund margin
        await client.query(
            "UPDATE users SET balance = balance + $1 WHERE user_id = $2",
            [Number(order.margin), user.user_id]
        );

        await client.query(
            "UPDATE limit_orders SET status = 'CANCELLED', cancelled_at = CURRENT_TIMESTAMP WHERE id = $1",
            [orderId]
        );

        await client.query("COMMIT");

        const newBalRes = await client.query("SELECT balance FROM users WHERE user_id = $1", [user.user_id]);

        console.log(`✅ Limit order ${orderId} cancelled, margin refunded: ${order.margin}`);

        res.json({
            ok: true,
            cancelledOrderId: orderId,
            newBalance: Number(newBalRes.rows[0].balance)
        });
    } catch (err) {
        try { await client.query("ROLLBACK"); } catch(e) {}
        if (sendAuthError(res, err)) return;
        console.error("Error cancelling limit order:", err.message);
        res.status(500).json({ ok: false, error: err.message });
    } finally {
        client.release();
    }
});

app.get("/api/limit-order/list", async (req, res) => {
    try {
        const userId = await getAuthenticatedUserId(req);
        if (!userId) return res.status(401).json({ ok: false, error: "UNAUTHORIZED" });

        const ordersRes = await db.query(
            "SELECT * FROM limit_orders WHERE user_id = $1 AND status = 'PENDING' ORDER BY created_at DESC",
            [userId]
        );

        res.json({ ok: true, orders: ordersRes.rows });
    } catch (err) {
        console.error("Error listing limit orders:", err);
        res.status(500).json({ ok: false, error: "SERVER_ERROR" });
    }
});

app.get("/api/vault/info", async (req, res) => {
  try {
    const userId = await getAuthenticatedUserId(req);
    if (!userId) return res.status(401).json({ ok: false, error: "UNAUTHORIZED" });

    const pool = await getVaultPool();
    const totalBalance = Number(pool.total_balance) || 0;
    const totalLocked = Number(pool.total_locked_balance) || 0;
    const totalUnlocked = Number(pool.total_unlocked_balance) || 0;

    // Депозиты пользователя
    const userDeps = await db.query(
      "SELECT * FROM vault_deposits WHERE user_id = $1 AND status = 'active' ORDER BY deposited_at DESC",
      [userId]
    );

    // Статистика пула
    const statsRes = await db.query(`
      SELECT 
        COUNT(*)::int as provider_count,
        SUM(amount)::numeric as total_deposited
      FROM vault_deposits WHERE status = 'active'
    `);

    // PnL за 24ч
    const pnlRes = await db.query(`
      SELECT COALESCE(SUM(delta), 0)::numeric as pnl_24h
      FROM vault_pnl_history
      WHERE created_at > NOW() - INTERVAL '24 hours'
    `);

    // Рассчитываем статистику для каждого депозита (share-based)
    const totalShares = Number(pool.total_shares) || 0;
    const depositsWithStats = userDeps.rows.map(dep => {
      const depAmount = Number(dep.amount);
      const depShares = Number(dep.shares) || 0;

      const shareInPool = totalShares > 0 ? depShares / totalShares : 0;
      const currentValue = Number((shareInPool * totalBalance).toFixed(4));
      const pnl = Number((currentValue - depAmount).toFixed(4));

      const now = new Date();
      const lockUntil = dep.lock_until ? new Date(dep.lock_until) : null;
      const canWithdraw = dep.lock_type === 'unlocked' || (lockUntil && lockUntil <= now);

      return {
        ...dep,
        currentValue,
        pnl,
        sharePercent: Number((shareInPool * 100).toFixed(4)),
        canWithdraw,
        daysLocked: lockUntil ? Math.max(0, Math.ceil((lockUntil - now) / (1000 * 60 * 60 * 24))) : 0
      };
    });

    const userTotalDeposited = depositsWithStats.reduce((s, d) => s + Number(d.amount), 0);
    const userTotalPnl = depositsWithStats.reduce((s, d) => s + d.pnl, 0);

    res.json({
      ok: true,
      pool: {
        totalBalance,
        totalLocked,
        totalUnlocked,
        totalShares,
        sharePrice: totalShares > 0 ? Number((totalBalance / totalShares).toFixed(6)) : 1,
        providerCount: Number(statsRes.rows[0].provider_count) || 0,
        pnl24h: Number(pnlRes.rows[0].pnl_24h) || 0,
        cumulativePnl: Number(pool.cumulative_pnl) || 0,
        lockPeriodDays: VAULT_LOCK_PERIOD_DAYS,
        vaultFraction: VAULT_FRACTION * 100
      },
      userDeposits: depositsWithStats,
      userSummary: {
        totalDeposited: Number(userTotalDeposited.toFixed(2)),
        totalPnl: Number(userTotalPnl.toFixed(4))
      }
    });
  } catch (err) {
    console.error("Error fetching vault info:", err);
    res.status(500).json({ ok: false, error: "SERVER_ERROR" });
  }
});

app.post("/api/vault/deposit", async (req, res) => {
  let client = null;
  try {
    const user = await getAuthenticatedUser(req);
    const { amount, lockType } = req.body;

    if (!amount || !lockType) {
      return res.status(400).json({ ok: false, error: "MISSING_FIELDS" });
    }
    if (lockType !== 'locked' && lockType !== 'unlocked') {
      return res.status(400).json({ ok: false, error: "INVALID_LOCK_TYPE" });
    }

    const depositAmount = Number(amount);
    if (isNaN(depositAmount) || depositAmount < 10) {
      return res.status(400).json({ ok: false, error: "MIN_DEPOSIT_10_VP" });
    }
    if (depositAmount > Number(user.balance)) {
      return res.status(400).json({ ok: false, error: "INSUFFICIENT_BALANCE" });
    }

    client = await db.connect();
    await client.query("BEGIN");

    // Списываем с баланса
    await client.query(
      "UPDATE users SET balance = balance - $1 WHERE user_id = $2",
      [depositAmount, user.user_id]
    );

    const lockUntil = lockType === 'locked'
      ? new Date(Date.now() + VAULT_LOCK_PERIOD_DAYS * 24 * 60 * 60 * 1000)
      : null;

    // Создаём депозит с расчётом доли (shares)
    const pool = await getVaultPool(client);
    const poolBalance = Number(pool.total_balance);
    const poolShares = Number(pool.total_shares);

    let shares;
    if (poolShares === 0 || poolBalance === 0) {
      shares = depositAmount;
    } else {
      shares = (depositAmount / poolBalance) * poolShares;
    }

    const depRes = await client.query(`
      INSERT INTO vault_deposits (user_id, amount, shares, lock_type, lock_until, status)
      VALUES ($1, $2, $3, $4, $5, 'active')
      RETURNING *
    `, [user.user_id, depositAmount, shares, lockType, lockUntil]);

    // Обновляем пул
    await client.query(`
      UPDATE vault_pool SET
        total_balance = total_balance + $1,
        total_shares  = total_shares  + $2,
        total_locked_balance   = total_locked_balance   + $3,
        total_unlocked_balance = total_unlocked_balance + $4,
        updated_at = CURRENT_TIMESTAMP
      WHERE id = 1
    `, [
      depositAmount,
      shares,
      lockType === 'locked'   ? depositAmount : 0,
      lockType === 'unlocked' ? depositAmount : 0
    ]);

    await client.query("COMMIT");

    const newBalRes = await db.query("SELECT balance FROM users WHERE user_id = $1", [user.user_id]);
    const poolAfter = await getVaultPool();  // ← переименована

    console.log(`✅ Vault deposit: user=${user.user_id}, amount=${depositAmount}, type=${lockType}`);

    res.json({
      ok: true,
      deposit: depRes.rows[0],
      newBalance: Number(newBalRes.rows[0].balance),
      poolBalance: Number(poolAfter.total_balance)
    });

  } catch (err) {
    if (client) {
      try { await client.query("ROLLBACK"); } catch(e) {}
    }
    if (sendAuthError(res, err)) return;
    console.error("Error depositing to vault:", err);
    res.status(500).json({ ok: false, error: err.message });
  } finally {
    if (client) {
      try { client.release(); } catch(e) {}
    }
  }
});

app.post("/api/vault/withdraw", async (req, res) => {
  let client = null;
  try {
    const user = await getAuthenticatedUser(req);
    const { depositId } = req.body;

    if (!depositId) {
      return res.status(400).json({ ok: false, error: "MISSING_DEPOSIT_ID" });
    }

    client = await db.connect();
    await client.query("BEGIN");

    const depRes = await client.query(
      "SELECT * FROM vault_deposits WHERE id = $1 AND user_id = $2 AND status = 'active' FOR UPDATE",
      [depositId, user.user_id]
    );

    if (!depRes.rows.length) {
      await client.query("ROLLBACK");
      return res.status(404).json({ ok: false, error: "DEPOSIT_NOT_FOUND" });
    }

    const dep = depRes.rows[0];
    const now = new Date();

    // Проверка блокировки
    if (dep.lock_type === 'locked' && dep.lock_until && new Date(dep.lock_until) > now) {
      await client.query("ROLLBACK");
      const daysRemaining = Math.ceil((new Date(dep.lock_until) - now) / (1000 * 60 * 60 * 24));
      return res.status(400).json({
        ok: false,
        error: "LOCKED",
        lockUntil: dep.lock_until,
        daysRemaining: daysRemaining
      });
    }

    // Рассчитываем сумму вывода по доле shares
    const pool = await getVaultPool(client);
    const poolBalance = Number(pool.total_balance);
    const poolShares = Number(pool.total_shares);

    const depShares = Number(dep.shares) || 0;
    const shareRatio = poolShares > 0 ? depShares / poolShares : 0;
    const withdrawAmount = Number((poolBalance * shareRatio).toFixed(8));

    // Возвращаем средства пользователю
    await client.query(
      "UPDATE users SET balance = balance + $1 WHERE user_id = $2",
      [withdrawAmount, user.user_id]
    );

    // Обновляем пул: вычитаем balance и shares, корректируем locked/unlocked
    await client.query(`
      UPDATE vault_pool SET
        total_balance          = total_balance          - $1,
        total_shares           = total_shares           - $2,
        total_locked_balance   = total_locked_balance   - $3,
        total_unlocked_balance = total_unlocked_balance - $4,
        updated_at = CURRENT_TIMESTAMP
      WHERE id = 1
    `, [
      withdrawAmount,
      depShares,
      dep.lock_type === 'locked'   ? Number(dep.amount) : 0,
      dep.lock_type === 'unlocked' ? Number(dep.amount) : 0
    ]);

    // Помечаем депозит как withdrawn
    await client.query(
      "UPDATE vault_deposits SET status = 'withdrawn', withdrawn_at = CURRENT_TIMESTAMP WHERE id = $1",
      [depositId]
    );

    await client.query("COMMIT");

    const newBalRes = await db.query("SELECT balance FROM users WHERE user_id = $1", [user.user_id]);

    console.log(`✅ Vault withdrawal: user=${user.user_id}, deposited=${dep.amount}, withdrawn=${withdrawAmount}, shares=${depShares}`);

    res.json({
      ok: true,
      withdrawn: Number(withdrawAmount.toFixed(4)),
      originalDeposit: Number(dep.amount),
      pnl: Number((withdrawAmount - Number(dep.amount)).toFixed(4)),
      newBalance: Number(newBalRes.rows[0].balance)
    });

  } catch (err) {
    if (client) {
      try { await client.query("ROLLBACK"); } catch(e) {}
    }
    if (sendAuthError(res, err)) return;
    console.error("Error withdrawing from vault:", err);
    res.status(500).json({ ok: false, error: err.message });
  } finally {
    if (client) {
      try { client.release(); } catch(e) {}
    }
  }
});


app.get("/api/price-alerts/list", async (req, res) => {
    try {
        const userId = await getAuthenticatedUserId(req);
        if (!userId) return res.status(401).json({ ok: false, error: 'UNAUTHORIZED' });
        const alerts = await getUserPriceAlerts(userId, true);
        res.json({ ok: true, alerts });
    } catch (err) {
        if (sendAuthError(res, err)) return;
        console.error('Error fetching price alerts:', err.message);
        res.status(500).json({ ok: false, error: err.message || 'SERVER_ERROR' });
    }
});

app.post("/api/price-alerts/create", async (req, res) => {
    try {
        const user = await getAuthenticatedUser(req);
        const pair = normalizeAlertPair(req.body?.pair);
        const triggerPrice = Number(req.body?.triggerPrice);
        const currentPrice = Number(req.body?.currentPrice);
        const providedDirection = req.body?.direction ? normalizeAlertDirection(req.body?.direction) : null;
        const direction = Number.isFinite(currentPrice) && currentPrice > 0
            ? (currentPrice > triggerPrice ? 'BELOW' : 'ABOVE')
            : providedDirection;

        if (!pair) return res.status(400).json({ ok: false, error: 'INVALID_PAIR' });
        if (!Number.isFinite(triggerPrice) || triggerPrice <= 0) return res.status(400).json({ ok: false, error: 'INVALID_PRICE' });
        if (!['ABOVE', 'BELOW'].includes(direction)) return res.status(400).json({ ok: false, error: 'CURRENT_PRICE_REQUIRED' });

        const existing = await db.query(
            `SELECT 1 FROM price_alerts
             WHERE user_id = $1 AND pair = $2 AND direction = $3 AND status = 'ACTIVE' AND ABS(trigger_price - $4) < 0.00000001
             LIMIT 1`,
            [user.user_id, pair, direction, triggerPrice]
        );
        if (existing.rows.length) {
            return res.status(409).json({ ok: false, error: 'ALERT_EXISTS' });
        }

        const insertRes = await db.query(
            `INSERT INTO price_alerts (user_id, pair, trigger_price, direction)
             VALUES ($1, $2, $3, $4)
             RETURNING *`,
            [user.user_id, pair, triggerPrice, direction]
        );

        await syncPriceAlertsToWorker(pair);
        res.json({ ok: true, alert: insertRes.rows[0] });
    } catch (err) {
        if (sendAuthError(res, err)) return;
        console.error('Error creating price alert:', err.message);
        res.status(500).json({ ok: false, error: err.message || 'SERVER_ERROR' });
    }
});

app.post("/api/price-alerts/delete", async (req, res) => {
    try {
        const user = await getAuthenticatedUser(req);
        const alertId = Number(req.body?.alertId);
        if (!Number.isFinite(alertId) || alertId <= 0) {
            return res.status(400).json({ ok: false, error: 'INVALID_ALERT_ID' });
        }

        const existing = await db.query(
            `SELECT id, pair FROM price_alerts WHERE id = $1 AND user_id = $2 AND status = 'ACTIVE' LIMIT 1`,
            [alertId, user.user_id]
        );
        if (!existing.rows.length) {
            return res.status(404).json({ ok: false, error: 'ALERT_NOT_FOUND' });
        }

        await db.query(
            `UPDATE price_alerts SET status = 'CANCELLED', cancelled_at = CURRENT_TIMESTAMP WHERE id = $1`,
            [alertId]
        );
        await syncPriceAlertsToWorker(existing.rows[0].pair);
        res.json({ ok: true });
    } catch (err) {
        if (sendAuthError(res, err)) return;
        console.error('Error deleting price alert:', err.message);
        res.status(500).json({ ok: false, error: err.message || 'SERVER_ERROR' });
    }
});

app.post("/api/user/margin-mode", async (req, res) => {
    try {
        const user = await getAuthenticatedUser(req);
        const requested = normalizeMarginMode(req.body?.marginMode);
        const currentRes = await db.query("SELECT margin_mode FROM users WHERE user_id = $1", [user.user_id]);
        const current = normalizeMarginMode(currentRes.rows[0]?.margin_mode);

        if (requested === current) {
            return res.json({ ok: true, marginMode: current });
        }

        const counts = await db.query(`SELECT
            (SELECT COUNT(*)::int FROM positions WHERE user_id = $1) AS positions_count,
            (SELECT COUNT(*)::int FROM limit_orders WHERE user_id = $1 AND status = 'PENDING') AS pending_orders_count`, [user.user_id]);
        const openPositions = Number(counts.rows[0]?.positions_count || 0);
        const pendingOrders = Number(counts.rows[0]?.pending_orders_count || 0);
        if (openPositions > 0 || pendingOrders > 0) {
            return res.status(409).json({ ok: false, error: "OPEN_POSITIONS_EXIST", openPositions, pendingOrders });
        }

        await db.query("UPDATE users SET margin_mode = $1, updated_at = CURRENT_TIMESTAMP WHERE user_id = $2", [requested, user.user_id]);
        res.json({ ok: true, marginMode: requested });
    } catch (err) {
        console.error("Error switching margin mode:", err.message);
        res.status(500).json({ ok: false, error: "SERVER_ERROR" });
    }
});

app.get("/api/health", (req, res) => res.json({
    ok: true,
    service: "api",
    entrypoint: "server.js",
    uptime: process.uptime()
}));

app.use("/api", (req, res) => {
    res.status(404).json({
        ok: false,
        error: "API_ROUTE_NOT_FOUND",
        method: req.method,
        path: req.originalUrl,
        service: "api",
        entrypoint: "server.js"
    });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));
