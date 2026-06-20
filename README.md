# PANA · BTC on-chain poller (standalone)

Worker **seedless** que vigila las deposit addresses BTC de los usuarios y avisa por Telegram cuando llega un
depósito on-chain sin reclamar. Pensado para vivir en un **repo PÚBLICO** (minutos de GitHub Actions ilimitados,
gratis) y dispararse de forma **confiable** vía `pg_cron → repository_dispatch` (no el `schedule:` best-effort).

## Por qué es seguro tenerlo público
- **Sin secretos en el código** — todo va por GitHub Secrets encriptados.
- **Seedless** — solo hace lecturas públicas de Spark (`getUtxosForDepositAddress`). NO toca el seed ni material de
  derivación (sig_B, HKDF, etc.).
- **Rol Postgres ACOTADO** (`btc_poller`) — el connection string es de un rol con grants mínimos: leer addresses +
  `telegram_chat_id`, y SELECT/INSERT/DELETE en `btc_pending_deposits`. **NO** puede leer `webhook_secret`, `email`,
  perfil, ni el resto de la BD. Peor caso si se filtrara: avisos falsos, NO pérdida de fondos.

## Setup

### 1. Rol `btc_poller` (ya creado en la BD) → ponerle password
En el dashboard de Supabase → SQL editor:
```sql
alter role btc_poller with password '<una-password-fuerte>';
```

### 2. Connection string (`DATABASE_URL`)
⚠️ **GitHub Actions runners son IPv4-only.** La conexión DIRECTA (`db.<ref>.supabase.co`, IPv6) **NO** sirve en CI →
usar el **Session Pooler** (IPv4). En el dashboard: *Settings → Database → Connection string → Session pooler*, y
reemplazar el rol/password por los de `btc_poller`:
```
postgresql://btc_poller.<project-ref>:<password>@aws-0-<region>.pooler.supabase.com:5432/postgres
```
(En tu máquina local sí podés usar la conexión directa o el pooler — el límite IPv4 es solo de los runners.)

### 3. TLS (anti-MITM)
La conexión **siempre verifica el certificado**. Descargá la CA de Supabase (*Settings → Database → SSL
configuration → download certificate*) y guardá su contenido PEM como el secret `SUPABASE_CA_CERT`. Si tu setup ya
valida contra el trust store del sistema, podés omitirla — pero **nunca** se desactiva la verificación.

### 4. Secrets del repo (Settings → Secrets and variables → Actions)
- `DATABASE_URL` — el del paso 2.
- `SUPABASE_CA_CERT` — la CA del paso 3 (recomendado).
- `TELEGRAM_BOT_TOKEN` — el bot de PANA.
- Variable `SPARK_NETWORK` = `MAINNET` (o `REGTEST` para probar).

### 5. Disparo confiable (pg_cron)
Lo arma PANA en Supabase: `pg_cron` cada 30 min → `repository_dispatch` (`event_type: btc-poll`) a este repo.
Requiere un PAT fine-grained (Contents: Read & Write sobre este repo) guardado en Supabase Vault.

## Probar
```bash
npm install
DATABASE_URL='postgresql://btc_poller…' SPARK_NETWORK=MAINNET TELEGRAM_BOT_TOKEN='…' node poller.mjs --once
```
`--once` = un barrido y salir (lo que usa el cron). Sin `--once` = daemon (loop con bucketing escalonado).
