/**
 * Poller de depósitos on-chain (worker server, SIN seed) — versión STANDALONE para un repo PÚBLICO (minutos GH
 * ilimitados) + disparo confiable por pg_cron→repository_dispatch.
 *
 * Idéntico al `btc-wallet/poller.mjs` salvo la CAPA DB: en vez de PostgREST con la `service_role` key, se conecta
 * **directo a Postgres con el rol ACOTADO `btc_poller`** (grants mínimos: leer addresses + tocar
 * `btc_pending_deposits`; NADA de webhook_secret / email / resto de la BD). Por eso este repo puede ser público:
 * el único secreto es un connection string a un rol con permisos recortados.
 *
 * SEEDLESS: solo lecturas públicas de Spark (`getUtxosForDepositAddress`) + Telegram. No toca material de derivación.
 *
 *   DATABASE_URL=postgres://btc_poller:…  SPARK_NETWORK=MAINNET  TELEGRAM_BOT_TOKEN=…  node poller.mjs --once
 */
import { SparkReadonlyClient } from "@buildonspark/spark-sdk";
import pg from "pg";

const { DATABASE_URL, TELEGRAM_BOT_TOKEN, SUPABASE_CA_CERT } = process.env;
const NETWORK = process.env.SPARK_NETWORK === "REGTEST" ? "REGTEST" : "MAINNET";
const WINDOW_S = Number(process.env.BTC_POLL_WINDOW_SECONDS ?? 1800);
const TICK_MS = Number(process.env.BTC_POLL_TICK_MS ?? 60_000);
// Columna de la deposit address por red. Constante CONTROLADA (whitelist abajo) → seguro interpolarla.
const DEPOSIT_COL = NETWORK === "REGTEST" ? "static_deposit_address_regtest" : "static_deposit_address";
const CONCURRENCY = Number(process.env.BTC_POLL_CONCURRENCY ?? 25);
const ONCE = process.argv.includes("--once");

if (!DATABASE_URL) {
  console.error("[poller] falta DATABASE_URL (connection string del rol btc_poller)");
  process.exit(1);
}

// ── Postgres directo con el rol ACOTADO. TLS SIEMPRE verificado (anti-MITM): si se pasa la CA de Supabase
// (`SUPABASE_CA_CERT`, descargable del dashboard) verifica contra ella; si no, contra el trust store del sistema.
// NUNCA se desactiva la verificación. Si el cert no valida, la conexión FALLA (fail-closed), no sigue insegura.
const db = new pg.Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: true, ...(SUPABASE_CA_CERT ? { ca: SUPABASE_CA_CERT } : {}) },
  max: 4,
});
const q = async (text, params) => (await db.query(text, params)).rows;

// ── Telegram (Bot API) ──
async function tgSend(chatId, text) {
  if (!TELEGRAM_BOT_TOKEN || !chatId) return;
  try {
    await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ chat_id: chatId, text }),
    });
  } catch (e) {
    console.error("[poller] telegram falló:", e.message);
  }
}
async function chatIdFor(address) {
  const rows = await q("select telegram_chat_id from public.users where address = $1", [address.toLowerCase()]).catch(() => []);
  return rows[0]?.telegram_chat_id ?? null;
}

// ── Stagger por buckets (modo daemon) ──
const TICK_S = Math.max(1, Math.round(TICK_MS / 1000));
const NUM_TICKS = Math.max(1, Math.floor(WINDOW_S / TICK_S));
const bucket = (address) => {
  let h = 0;
  for (let i = 0; i < address.length; i++) h = (h * 31 + address.charCodeAt(i)) >>> 0;
  return h % NUM_TICKS;
};
const currentBucket = () => Math.floor((Math.floor(Date.now() / 1000) % WINDOW_S) / TICK_S) % NUM_TICKS;

// ── Procesa UNA address: getUtxos → diff vs marker → insert nuevos (+TG) / delete reclamados ──
async function processAddress(ro, address, deposit) {
  if (!deposit) return;
  let current;
  try {
    const res = await ro.getUtxosForDepositAddress({ depositAddress: deposit, excludeClaimed: true });
    current = new Set(res.utxos.map((u) => `${u.txid}:${u.vout}`));
  } catch (e) {
    console.error(`[poller] getUtxos ${address} falló:`, e.message);
    return;
  }
  const markers = await q("select txid, vout from public.btc_pending_deposits where address = $1 and network = $2", [address, NETWORK]);
  const seen = new Set(markers.map((m) => `${m.txid}:${m.vout}`));

  const fresh = [...current].filter((k) => !seen.has(k));
  if (fresh.length) {
    for (const k of fresh) {
      const [txid, vout] = k.split(":");
      await q(
        "insert into public.btc_pending_deposits (address, network, txid, vout) values ($1,$2,$3,$4) on conflict do nothing",
        [address, NETWORK, txid, Number(vout)],
      );
    }
    const chatId = await chatIdFor(address);
    for (let i = 0; i < fresh.length; i++) {
      await tgSend(chatId, "🟠 Bitcoin deposit received on-chain. Open PANA to claim it into Spark.");
    }
    console.log(`[poller] ${address}: ${fresh.length} depósito(s) nuevo(s) → aviso`);
  }

  const gone = [...seen].filter((k) => !current.has(k));
  for (const k of gone) {
    const [txid, vout] = k.split(":");
    await q("delete from public.btc_pending_deposits where network = $1 and txid = $2 and vout = $3", [NETWORK, txid, Number(vout)])
      .catch((e) => console.error("[poller] delete falló:", e.message));
  }
}

const ro = SparkReadonlyClient.createPublic({ network: NETWORK });

// ── Modo always-on (host 24/7): un tick por minuto procesa SOLO el bucket que toca (staggered) ──
async function tick() {
  const b = currentBucket();
  const rows = await q(`select address, ${DEPOSIT_COL} as deposit from public.spark_wallets where ${DEPOSIT_COL} is not null`);
  const due = rows.filter((r) => bucket(r.address) === b);
  for (const r of due) await processAddress(ro, r.address, r.deposit);
}

// ── Modo --once (cron / GitHub Actions): UN barrido de TODAS las direcciones con pool de concurrencia ──
async function sweepAll() {
  const rows = await q(`select address, ${DEPOSIT_COL} as deposit from public.spark_wallets where ${DEPOSIT_COL} is not null`);
  let next = 0;
  const worker = async () => {
    while (next < rows.length) {
      const r = rows[next++];
      try {
        await processAddress(ro, r.address, r.deposit);
      } catch (e) {
        console.error(`[poller] ${r.address} falló:`, e.message);
      }
    }
  };
  await Promise.all(Array.from({ length: Math.min(CONCURRENCY, rows.length || 1) }, worker));
  console.log(`[poller] barrido completo: ${rows.length} direcciones (conc=${CONCURRENCY})`);
}

if (ONCE) {
  console.log(`[poller] --once · network=${NETWORK} · conc=${CONCURRENCY}`);
  await sweepAll().catch((e) => {
    console.error("[poller] barrido falló:", e.message);
    process.exitCode = 1;
  });
  await db.end();
  process.exit(process.exitCode ?? 0);
} else {
  console.log(`[poller] BTC on-chain watcher · network=${NETWORK} · ventana=${WINDOW_S}s · tick=${TICK_MS}ms · buckets=${NUM_TICKS}`);
  await tick().catch((e) => console.error("[poller] tick inicial:", e.message));
  setInterval(() => tick().catch((e) => console.error("[poller] tick:", e.message)), TICK_MS);
}
