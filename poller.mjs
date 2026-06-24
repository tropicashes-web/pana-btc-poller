/**
 * Poller de depósitos on-chain (worker server, SIN seed) — versión STANDALONE para un repo PÚBLICO (minutos GH
 * ilimitados) + disparo confiable por pg_cron→repository_dispatch.
 *
 * Idéntico al `btc-wallet/poller.mjs` salvo la CAPA DB: en vez de PostgREST con la `service_role` key, se conecta
 * **directo a Postgres con el rol ACOTADO `btc_poller`** (grants mínimos: leer addresses + tocar
 * `btc_pending_deposits`/`spark_seen_transfers`; NADA de webhook_secret / email / resto de la BD). Por eso este repo
 * puede ser público: el único secreto es un connection string a un rol con permisos recortados.
 *
 * SEEDLESS: solo lecturas públicas de Spark + Telegram. No toca material de derivación.
 *
 * DOS barridos INDEPENDIENTES (cada uno con su pg_cron→repository_dispatch, cadencias distintas):
 *   `--once`   → depósitos on-chain  (`getUtxosForDepositAddress`, dispatch `btc-poll`, ~30min)
 *   `--spark`  → Spark→Spark entrante (`getPendingTransfers`, dispatch `spark-poll`, ~1min)
 *
 *   DATABASE_URL=postgres://btc_poller:…  SPARK_NETWORK=MAINNET  TELEGRAM_BOT_TOKEN=…  node poller.mjs --once [--spark]
 */
import { SparkReadonlyClient } from "@buildonspark/spark-sdk";
import pg from "pg";

const { DATABASE_URL, TELEGRAM_BOT_TOKEN, SUPABASE_CA_CERT } = process.env;
const NETWORK = process.env.SPARK_NETWORK === "REGTEST" ? "REGTEST" : "MAINNET";
const WINDOW_S = Number(process.env.BTC_POLL_WINDOW_SECONDS ?? 1800);
const TICK_MS = Number(process.env.BTC_POLL_TICK_MS ?? 60_000);
// Columna de la deposit address por red. Constante CONTROLADA (whitelist abajo) → seguro interpolarla.
const DEPOSIT_COL = NETWORK === "REGTEST" ? "static_deposit_address_regtest" : "static_deposit_address";
// Columnas del barrido Spark→Spark por red (mismo criterio whitelist que DEPOSIT_COL → seguro interpolarlas).
const SPARK_COL = NETWORK === "REGTEST" ? "spark_address_regtest" : "spark_address";
const IDENTITY_COL = NETWORK === "REGTEST" ? "identity_pubkey_regtest" : "identity_pubkey";
const CONCURRENCY = Number(process.env.BTC_POLL_CONCURRENCY ?? 25);
const ONCE = process.argv.includes("--once");
const SPARK = process.argv.includes("--spark"); // enruta a sweepAllSpark (dispatch `spark-poll`); el on-chain queda intacto

// TransferType (proto) → categoría (igual que btc-wallet/src/spark.mjs): 0=LN · 1/3=on-chain · 2/30/40=Spark.
const TYPE_CAT = { 0: "Lightning", 1: "On-chain", 2: "Spark", 3: "On-chain", 30: "Spark", 40: "Spark" };
const bytesToHex = (u8) => Array.from(u8 ?? [], (b) => b.toString(16).padStart(2, "0")).join("");

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
// Como chatIdFor pero también trae el toggle `notify_incoming` (el sweep Spark SÍ lo respeta; default ON).
async function chatPrefFor(address) {
  const rows = await q("select telegram_chat_id, notify_incoming from public.users where address = $1", [address.toLowerCase()]).catch(() => []);
  return { chatId: rows[0]?.telegram_chat_id ?? null, notifyIncoming: rows[0]?.notify_incoming ?? true };
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

// ── Spark→Spark entrante: getPendingTransfers (seedless) → filtra Spark + no-self → dedup → aviso UNA vez ──
async function processSparkAddress(ro, address, sparkAddr, identityHex) {
  if (!sparkAddr) return;
  let pend;
  try {
    pend = await ro.getPendingTransfers(sparkAddr); // entrantes PENDIENTES (antes de que el receptor los reclame)
  } catch (e) {
    console.error(`[poller] getPendingTransfers ${address} falló:`, e.message);
    return;
  }
  const incoming = (pend ?? []).filter((t) => {
    if (TYPE_CAT[t.type] !== "Spark") return false; // solo Spark→Spark (LN tiene webhook; on-chain tiene su sweep)
    if (identityHex && bytesToHex(t.senderIdentityPublicKey).toLowerCase() === identityHex) return false; // self-send
    return true;
  });
  if (!incoming.length) return;
  let pref = null;
  for (const t of incoming) {
    const ins = await q(
      "insert into public.spark_seen_transfers (transfer_id, network) values ($1,$2) on conflict do nothing returning transfer_id",
      [t.id, NETWORK],
    ).catch((e) => {
      console.error("[poller] dedup insert falló:", e.message);
      return [{ transfer_id: null }]; // ante fallo de dedup, NO re-avisa en loop: tratamos como ya-visto
    });
    if (!ins.length) continue; // ya avisado (conflict)
    if (pref === null) pref = await chatPrefFor(address);
    const sats = Number(t.totalValue);
    if (pref.notifyIncoming && pref.chatId) {
      await tgSend(pref.chatId, `🟣 Received ${sats.toLocaleString("en-US")} sats via Spark`);
    }
    console.log(`[poller] ${address}: Spark transfer ${t.id} (${sats} sats) → ${pref.chatId ? "aviso" : "sin chat"}`);
  }
}

// ── --spark (dispatch `spark-poll`): UN barrido de TODAS las spark addresses con pool de concurrencia ──
async function sweepAllSpark() {
  const rows = await q(
    `select address, ${SPARK_COL} as spark, ${IDENTITY_COL} as identity from public.spark_wallets where ${SPARK_COL} is not null`,
  );
  let next = 0;
  const worker = async () => {
    while (next < rows.length) {
      const r = rows[next++];
      try {
        await processSparkAddress(ro, r.address, r.spark, r.identity ? String(r.identity).toLowerCase() : null);
      } catch (e) {
        console.error(`[poller] spark ${r.address} falló:`, e.message);
      }
    }
  };
  await Promise.all(Array.from({ length: Math.min(CONCURRENCY, rows.length || 1) }, worker));
  console.log(`[poller] barrido Spark completo: ${rows.length} direcciones (conc=${CONCURRENCY})`);
}

if (SPARK) {
  // Dispatch `spark-poll` (cada ~1 min): barrido Spark→Spark, separado del on-chain. Siempre one-shot.
  console.log(`[poller] --spark (Spark→Spark) · network=${NETWORK} · conc=${CONCURRENCY}`);
  await sweepAllSpark().catch((e) => {
    console.error("[poller] barrido Spark falló:", e.message);
    process.exitCode = 1;
  });
  await db.end();
  process.exit(process.exitCode ?? 0);
} else if (ONCE) {
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
