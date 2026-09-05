/* ============================================================
   PORT · BİLDİRİM WORKER  ·  V-13.2 (cron iz kaydı /runlog · pay penceresi 1 saat · damga iş bitince)
   ------------------------------------------------------------
   Veriyi DOĞRUDAN KV'den okur (worker-to-worker HTTP yok).
   "port" (uygulama) ve "portfolio-sync" worker'larına DOKUNMAZ.

   KURULUM (port-notify worker'ında)
   1) Edit code → bu dosyanın TAMAMINI yapıştır → Deploy.
   2) Bindings sekmesi → Add → KV namespace:
        Variable name = PORTFOLIO
        KV namespace  = portfolio-kv     (sync worker'ın kullandığı KV)
   3) Settings → Variables and Secrets → Add → Secret:
        WA_PHONE   = numaran, ülke koduyla, + ve boşluksuz (ör. 905321234567)
        WA_APIKEY  = CallMeBot apikey'in
        TEST_KEY   = test parolan (ör. port2026)
      (KV kullanınca SYNC_URL / SYNC_PW GEREKMEZ. İstersen HTTP yedeği için
       yine de ekleyebilirsin — KV yoksa ona düşer.)
   4) Deploy. Triggers → Cron: her 15 dakikada bir, TEK cron yeter (yildiz/15 bosluk yildiz x4)

   TEST:
     Ham teşhis (veri kaynağını gösterir):  /?probe=1&key=port2026
     Sadece göster (WA'ya göndermez):        /?debug=daily&key=port2026
     Gerçek gönderim:                        /?test=daily&key=port2026
   ============================================================ */

export default {
  /* V-9.9: TEK cron yeter — her 15 dakikada bir. Hangi işin sırası geldiğine İstanbul saatine
     bakarak burada karar veriyoruz (cron sayısı sınırına takılmamak için birleştirildi).
       her 15 dk : fiyat/uyarı kontrolü
       00:00     : gece anlık görüntüsü (snapshot) — uygulamayı açmasan da kayıt oluşur
       06:00     : bilanço (earnings) tarihlerini tazele
       07:30     : sabah WhatsApp özeti (yalnız Pzt–Cum)
       19:00 Paz : haftalık WhatsApp özeti
     Eski cron'lar (0 16 * * *, 0 16 * * SUN) dursa da zararsız; dağıtıcı yine saate bakar. */
  async scheduled(event, env, ctx) {
    ctx.waitUntil(dispatch(env).catch(()=>{}));
  },

  async fetch(request, env) {
    const url = new URL(request.url);
    // V6.2: CORS preflight (uygulama farklı origin'den POST/GET atıyor)
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Access-Control-Max-Age": "86400"
      } });
    }
    /* V-9.8: FRED PROXY — tarayıcı FRED'e doğrudan gidemiyor (CORS yok).
       Uygulama buraya sorar, worker FRED'i çağırıp CORS başlığıyla döner.
       Anahtar KV'deki senkron durumundan (apiKeys.fred) okunur; URL'de anahtar taşınmaz. */
    /* V-9.9: elle tetikleme uçları — /snapshot, /earnings, /morning, /weekly (hepsi key korumalı) */
    if (url.pathname === "/snapshot" || url.pathname === "/earnings" || url.pathname === "/morning" || url.pathname === "/weekly" || url.pathname === "/pay" || url.pathname === "/dispatch" || url.pathname === "/runlog") {
      const k = url.searchParams.get("key") || "";
      const exp = env.TEST_KEY || env.SYNC_PW || "";
      if (!exp || k !== exp) return txt("unauthorized", 401);
      const dry = url.searchParams.get("dry") === "1";
      try {
        if (url.pathname === "/snapshot") return txt("SNAPSHOT · " + await writeSnapshot(env));
        if (url.pathname === "/earnings") return txt("EARNINGS · " + await refreshEarnings(env));
        if (url.pathname === "/morning")  return txt(await buildMorning(env, !dry));
        if (url.pathname === "/weekly")   return txt(await buildWeeklyMsg(env, !dry));
        if (url.pathname === "/pay")      return txt(await buildPayMsg(env, !dry));
        /* V-10.0: /dispatch artık gerçek cron gibi davranır (saat kontrollü).
           Tek işi zorla çalıştırmak için ?job=push|snapshot|earnings|morning|weekly ekle.
           Hepsini birden zorlamak Cloudflare'in çağrı başına 50 subrequest sınırını aşıyordu. */
        if (url.pathname === "/dispatch") return txt("DISPATCH · " + await dispatch(env, url.searchParams.get("job") || "", dry));
        if (url.pathname === "/runlog")   return txt("RUNLOG\n" + await runLogDump(env, +(url.searchParams.get("days") || 3)));
      } catch (e) { return txt("HATA [" + url.pathname.slice(1).toUpperCase() + "]: " + errStr(e), 500); }
    }
    /* V-11.2 (#9): MAKRO TAKVİM — uygulama içi görünüm için FRED yayın takvimi (gerçek veri).
       Tarayıcı FRED'e doğrudan gidemiyor (CORS yok); worker çekip CORS başlığıyla döner. */
    /* V-11.3: SEC EDGAR Form 4 — kümelenmiş içeriden alım taraması.
       Tarayıcı SEC'e doğrudan gidemiyor (CORS + zorunlu User-Agent); worker çeker.
       ?syms=AAPL,MSFT → o sembolleri TARAR ve KV'ye işler (çağrı başına en fazla 4 sembol,
       50 subrequest sınırı için). ?cached=1 → hiç ağa çıkmadan KV'deki son sonucu döner. */
    /* V-11.5: FINRA kısa pozisyon (Equity Short Interest) — ayda iki kez yayınlanan resmî veri.
       Tarayıcı api.finra.org'a doğrudan gidemiyor (CORS); worker POST atıp CORS başlığıyla döner.
       ?syms=A,B → yalnız o sembolleri getirir · ?scan=1 → days-to-cover eşiğini geçenleri tarar.
       Anahtar gerektirmeyen açık uçtur; FINRA kimlik isterse hata metni olduğu gibi görünür. */
    if (url.pathname === "/short") {
      const k = url.searchParams.get("key") || "";
      const exp = env.TEST_KEY || env.SYNC_PW || "";
      if (!exp || k !== exp) return txt("unauthorized", 401);
      try { return txt(JSON.stringify(await finraShort(env, url.searchParams)), 200, "application/json; charset=utf-8"); }
      catch (e) { return txt(JSON.stringify({ error: errStr(e) }), 200, "application/json; charset=utf-8"); }
    }
    /* V-12.4: AI PROXY — tarayıcı doğrudan Anthropic'e gidemiyor (NetworkError). */
    if (url.pathname === "/ai") {
      const k = url.searchParams.get("key") || "";
      const exp = env.TEST_KEY || env.SYNC_PW || "";
      if (!exp || k !== exp) return txt("unauthorized", 401);
      try { return await aiProxy(env, request, url); }
      catch (e) { return txt(JSON.stringify({ error: errStr(e) }), 200, "application/json; charset=utf-8"); }
    }

    /* V-11.9: ENDEKS DAHİL ETME ARBİTRAJI — S&P 500'e girme adayları (FMP stable). */
    if (url.pathname === "/index") {
      const k = url.searchParams.get("key") || "";
      const exp = env.TEST_KEY || env.SYNC_PW || "";
      if (!exp || k !== exp) return txt("unauthorized", 401);
      try { return txt(JSON.stringify(await idxScan(env, url.searchParams)), 200, "application/json; charset=utf-8"); }
      catch (e) { return txt(JSON.stringify({ ok: false, error: errStr(e) }), 200, "application/json; charset=utf-8"); }
    }
    if (url.pathname === "/insider") {
      const k = url.searchParams.get("key") || "";
      const exp = env.TEST_KEY || env.SYNC_PW || "";
      if (!exp || k !== exp) return txt("unauthorized", 401);
      try {
        if (url.searchParams.get("diag") === "1") {
          /* Neden boş döndüğünü göstermek için: ham durum, kaç sembol eşleşti, örnek. */
          const t = (url.searchParams.get("syms") || "NVDA,TSLA,AAPL").toUpperCase().split(/[^A-Z0-9.\-]+/).filter(Boolean);
          try { await env.PORTFOLIO.delete("sec:cik"); } catch (e) {}
          const d = {};
          const mp = await cikMap(env, t, d);
          return txt(JSON.stringify({ ok: true, diag: d, asked: t, resolved: t.map(x => x + "=" + (mp[x] || "—")) }, null, 1), 200, "application/json");
        }
        if (url.searchParams.get("cached") === "1") {
          return txt(JSON.stringify({ ok: true, cached: true, rows: await insiderCacheRows(env) }), 200, "application/json");
        }
        const win = Math.min(120, Math.max(7, parseInt(url.searchParams.get("win") || "45", 10) || 45));
        let syms = (url.searchParams.get("syms") || "").toUpperCase().split(/[^A-Z0-9.\-]+/).filter(Boolean);
        if (!syms.length) {
          const st = await fetchState(env); const keys = (st && st.keys) || {};
          syms = insiderUniverse(keys);
        }
        const done = await insiderScan(env, syms.slice(0, INSIDER_BATCH), win);
        return txt(JSON.stringify({ ok: true, win, scanned: done.scanned, skipped: done.skipped, rows: done.rows }), 200, "application/json");
      } catch (e) { return txt(JSON.stringify({ error: errStr(e) }), 500, "application/json"); }
    }
    if (url.pathname === "/maccal") {
      const k = url.searchParams.get("key") || "";
      const exp = env.TEST_KEY || env.SYNC_PW || "";
      if (!exp || k !== exp) return txt("unauthorized", 401);
      const days = Math.min(120, Math.max(7, parseInt(url.searchParams.get("days") || "45", 10) || 45));
      const back = Math.min(30, Math.max(0, parseInt(url.searchParams.get("back") || "0", 10) || 0));
      try {
        const st = await fetchState(env);
        const keys = (st && st.keys) || {};
        /* uzun pencerede haftalık yayınlar (jobless claims) 3'ten fazla çıkar → tekrar sınırı
           pencere uzunluğuna göre ölçeklenir; yalnız GERÇEKTEN günlük gürültü elenir. */
        const rows = await fredCalendar(env, keys, days, back, Math.max(4, Math.round(days * 0.5)), 60);
        return txt(JSON.stringify({ ok: true, at: new Date().toISOString(), days, rows }), 200, "application/json");
      } catch (e) { return txt(JSON.stringify({ error: errStr(e) }), 500, "application/json"); }
    }
    if (url.pathname === "/fred") {
      const k = url.searchParams.get("key") || "";
      const exp = env.TEST_KEY || env.SYNC_PW || "";
      if (!exp || k !== exp) return txt("unauthorized", 401);
      const series = (url.searchParams.get("series") || "").replace(/[^A-Za-z0-9_]/g, "");
      if (!series) return txt("series gerekli", 400);
      try {
        const st = await fetchState(env);
        const fredKey = ((st && st.keys && st.keys.apiKeys && st.keys.apiKeys.fred) || env.FRED_KEY || "").trim();
        if (!fredKey) return txt(JSON.stringify({ error: "FRED anahtarı yok (uygulamada Ayarlar → FRED, sonra Kaydet)" }), 400, "application/json");
        const limit = Math.min(500, Math.max(1, parseInt(url.searchParams.get("limit") || "14", 10) || 14));
        const u = `https://api.stlouisfed.org/fred/series/observations?series_id=${series}&api_key=${encodeURIComponent(fredKey)}&file_type=json&sort_order=desc&limit=${limit}`;
        const r = await fetch(u, { headers: ua() });
        const body = await r.text();
        return new Response(body, { status: r.status, headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
          "Cache-Control": "public, max-age=1800"
        } });
      } catch (e) { return txt(JSON.stringify({ error: errStr(e) }), 500, "application/json"); }
    }
    // V6.2: Native Web Push rotaları (path bazlı, key korumalı)
    if (url.pathname === "/subscribe" || url.pathname === "/unsubscribe" || url.pathname === "/pushtest" || url.pathname === "/pushcheck" || url.pathname === "/mailtest") {
      const k = url.searchParams.get("key") || "";
      const exp = env.TEST_KEY || env.SYNC_PW || "";
      if (!exp || k !== exp) return txt("unauthorized", 401);
      try {
        if (url.pathname === "/subscribe") { const sub = await request.json(); await pushSubStore(env, sub); return txt("OK · abone kaydedildi"); }
        if (url.pathname === "/unsubscribe") { const b = await request.json().catch(()=>({})); await pushSubRemove(env, b.endpoint); return txt("OK · abonelik silindi"); }
        if (url.pathname === "/pushtest") { const n = await sendPushAll(env, { title: "PORT · Test", body: "Push çalışıyor ✅", tag: "test", url: "/" }); return txt("OK · " + n + " cihaza gönderildi"); }
        if (url.pathname === "/pushcheck") { const r = await runPushChecks(env); return txt("PUSH CHECK ·\n" + r); }
        if (url.pathname === "/mailtest") { const r = await weeklyMail(env); return txt("MAIL · " + r); }
      } catch (e) { return txt("HATA [" + url.pathname.slice(1).toUpperCase() + "]: " + errStr(e), 500); }
    }
    const test = url.searchParams.get("test");
    const debug = url.searchParams.get("debug");
    const probe = url.searchParams.get("probe");
    const prices = url.searchParams.get("prices");
    if (!test && !debug && !probe && !prices)
      return txt("port-notify · canlı.\nTeşhis: /?probe=1&key=...\nFiyat testi: /?prices=1&key=...\nGöster: /?debug=daily&key=...\nGönder: /?test=daily&key=...");

    const key = url.searchParams.get("key") || "";
    const expected = env.TEST_KEY || env.SYNC_PW || "";
    if (!expected || key !== expected) return txt("unauthorized (TEST_KEY yanlış/eksik)", 401);

    if (probe) return txt(await probeText(env));
    if (prices) {
      try {
        const state = await fetchState(env);
        const keys = (state && state.keys) || {};
        const rep = await fetchLivePrices(env, keys);
        const ps = (keys.positions || []).filter(p => !p.manual).slice(0, 8)
          .map(p => `${p.t}=${p.price} (${p.day != null ? (p.day > 0 ? "+" : "") + p.day + "%" : "—"})`);
        return txt("PRICES ·\nKripto güncellenen: " + rep.crypto + " · Hisse güncellenen: " + rep.stock +
          "\nHatalar: " + (rep.fail.length ? rep.fail.join(" | ") : "yok") +
          "\nÖrnek: " + ps.join(" · "));
      } catch (e) { return txt("HATA [PRICES]: " + errStr(e), 500); }
    }

    const kind = ((test || debug) === "weekly") ? "weekly" : "daily";
    const send = !!test;
    try {
      const res = await run(env, kind, send);
      const head = send ? ("OK · WhatsApp'a gönderildi (" + kind + ")") : ("DEBUG · gönderilmedi (" + kind + ")");
      return txt(head + "\n\n" + res.text);
    } catch (e) {
      return txt("HATA [" + (e && e.stage || "?") + "]: " + errStr(e), 500);
    }
  }
};

function txt(s, status = 200, ct) { return new Response(s, { status, headers: { "Content-Type": ct || "text/plain; charset=utf-8", "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Headers": "Content-Type", "Access-Control-Allow-Methods": "GET,POST,OPTIONS" } }); }
/* ============================================================
   V-11.5 · FINRA KISA POZİSYON (Equity Short Interest)
   Resmî, ücretsiz, anahtarsız uç: api.finra.org/data/group/otcMarket/name/EquityShortInterest
   Ayda iki kez yayınlanır. FINRA'nın kendi alanları kullanılır — days-to-cover UYDURULMAZ,
   dosyada yoksa null döner. Float verisi bu dosyada YOK; uygulama tarafında "—" gösterilir.
============================================================ */
/* V-11.8: FINRA "equityShortInterest" veri setini kullanımdan kaldırdı (30 Nisan 2021'den
   sonra yayınlanmıyor, içinde yalnız eski OTC kayıtları var) ve yerine
   "equityShortInterestStandardized" geldi. Önce yenisi denenir, olmazsa eskisine düşülür;
   hangisinin çalıştığı KV'ye yazılır ve /short?diag=1 ile görünür. */
const FINRA_BASE = "https://api.finra.org/data/group/otcMarket/name/";
const FINRA_META = "https://api.finra.org/metadata/group/otcMarket/name/";
const FINRA_SETS = ["equityShortInterestStandardized", "equityShortInterest"];
function finraNum(v) { const n = parseFloat(v); return isFinite(n) ? n : null; }
function finraRow(r) {
  const dtc = finraNum(r.daysToCoverQuantity != null ? r.daysToCoverQuantity : r.daysToCover);
  const cur = finraNum(r.currentShortPositionQuantity != null ? r.currentShortPositionQuantity : r.currentShortShareNumber);
  const prv = finraNum(r.previousShortPositionQuantity != null ? r.previousShortPositionQuantity : r.previousShortShareNumber);
  const adv = finraNum(r.averageDailyVolumeQuantity != null ? r.averageDailyVolumeQuantity : r.averageShortShareNumber);
  return {
    sym: (r.symbolCode || r.issueSymbolIdentifier || "").trim().toUpperCase(),
    name: (r.issueName || "").trim(),
    date: (r.settlementDate || "").slice(0, 10),
    cur, prev: prv, adv,
    dtc: dtc != null ? dtc : ((cur != null && adv > 0) ? cur / adv : null),   // dosyada yoksa aynı formülle türetilir
    chgPct: finraNum(r.changePercent != null ? r.changePercent : r.percentageChangefromPreviousShort),
    mkt: (r.marketClassCode || r.marketCategoryCode || "").trim()
  };
}
/* V-11.7: FINRA eşleşme olmayan sorguya 200 + BOŞ GÖVDE dönüyor.
   Bunu hata saymak yanlıştı — boş gövde "kayıt yok" demektir. */
async function finraPost(set, body, meta) {
  const r = await fetch(FINRA_BASE + set, {
    method: "POST",
    headers: { "Content-Type": "application/json", "Accept": "application/json" },
    body: JSON.stringify(body)
  });
  const t = await r.text();
  if (meta) { meta.status = r.status; meta.len = t.length; meta.head = t.slice(0, 200); }
  if (!r.ok) throw new Error("FINRA " + r.status + ": " + t.slice(0, 200));
  if (!t || !t.trim()) return [];
  let j; try { j = JSON.parse(t); } catch (e) { throw new Error("FINRA yanıtı JSON değil (" + t.length + " bayt): " + t.slice(0, 160)); }
  return Array.isArray(j) ? j : (j && Array.isArray(j.data) ? j.data : []);
}
/* Veri seti gerçekten erişilebilir mi? Filtresiz tek satır ister — tanı içindir. */
async function finraProbe() {
  const out = {};
  for (const set of FINRA_SETS) {
    const o = { set };
    try {
      const r = await fetch(FINRA_META + set, { headers: { "Accept": "application/json" } });
      const t = await r.text();
      o.meta = { status: r.status, head: t.slice(0, 400) };
    } catch (e) { o.meta = { error: errStr(e) }; }
    try {
      const meta = {};
      const rows = await finraPost(set, { limit: 1 }, meta);
      o.rows = rows.length; o.status = meta.status; o.len = meta.len;
      if (rows.length) { o.sampleDate = (rows[0].settlementDate || "").slice(0, 10); o.fields = Object.keys(rows[0]); }
    } catch (e) { o.error = errStr(e); }
    (out.sets = out.sets || []).push(o);
  }
  return out;
}
/* Aday tarihler tutmazsa: son 90 günde HERHANGİ bir kayıt var mı? Sıralama kullanmadan,
   tarih aralığı filtresiyle tek satır ister; dönen satırın tarihi geçerli bir yayın tarihidir. */
async function finraRangeHint(set) {
  const to = new Date(Date.now() + 3 * 3600 * 1000);
  const from = new Date(to.getTime() - 90 * 86400000);
  const rows = await finraPost(set, {
    limit: 1,
    dateRangeFilters: [{ fieldName: "settlementDate", startDate: finraIso(from), endDate: finraIso(to) }]
  });
  return rows.length ? (rows[0].settlementDate || "").slice(0, 10) : null;
}
/* V-11.6: FINRA "Sorting is allowed only if all partition keys are specified" diyerek
   sıralamalı sorguyu reddediyor. Bu yüzden son yayın tarihi SORULARAK değil, TAKVİMDEN
   üretilip denenerek bulunuyor: kısa pozisyon her ayın 15'i ve son günü için raporlanır,
   hafta sonuna denk gelirse bir önceki iş gününe kayar. Yayın, mutabakat tarihinden
   ~8 iş günü sonra çıktığı için en yeni aday henüz boş dönebilir — sırayla geriye gidilir. */
function finraIso(d) { return d.toISOString().slice(0, 10); }
function finraBizBack(d) {
  const x = new Date(d.getTime());
  while (x.getUTCDay() === 0 || x.getUTCDay() === 6) x.setUTCDate(x.getUTCDate() - 1);
  return x;
}
function finraCandidates(months) {
  const now = new Date(Date.now() + 3 * 3600 * 1000);
  const today = finraIso(now);
  let y = now.getUTCFullYear(), m = now.getUTCMonth();
  const out = [];
  for (let i = 0; i < (months || 3); i++) {
    out.push(finraIso(finraBizBack(new Date(Date.UTC(y, m + 1, 0)))));   // ay sonu
    out.push(finraIso(finraBizBack(new Date(Date.UTC(y, m, 15)))));      // ay ortası
    m--; if (m < 0) { m = 11; y--; }
  }
  return [...new Set(out)].filter(d => d <= today).sort().reverse();
}
async function finraLatest(env, diag) {
  const c = await env.PORTFOLIO.get("short:latest").catch(() => null);
  if (c && !diag) { try { return JSON.parse(c); } catch (e) {} }
  const tried = [];
  for (const set of FINRA_SETS) {
    for (const d of finraCandidates(3)) {
      const meta = {};
      let rows = [];
      try {
        rows = await finraPost(set, {
          limit: 1,
          compareFilters: [{ compareType: "EQUAL", fieldName: "settlementDate", fieldValue: d }]
        }, meta);
      } catch (e) { tried.push({ set, date: d, error: errStr(e) }); continue; }
      tried.push({ set, date: d, rows: rows.length });
      if (rows.length) {
        const hit = { set, date: d };
        if (diag) diag.tried = tried;
        await env.PORTFOLIO.put("short:latest", JSON.stringify(hit), { expirationTtl: 43200 }).catch(() => {});
        return hit;
      }
    }
    /* aday tarihler tutmadı — aralık ipucu dene */
    try {
      const h = await finraRangeHint(set);
      tried.push({ set, rangeHint: h });
      if (h) {
        const hit = { set, date: h };
        if (diag) diag.tried = tried;
        await env.PORTFOLIO.put("short:latest", JSON.stringify(hit), { expirationTtl: 43200 }).catch(() => {});
        return hit;
      }
    } catch (e) { tried.push({ set, rangeHintError: errStr(e) }); }
  }
  if (diag) diag.tried = tried;
  throw new Error("FINRA'nın iki veri setinde de güncel kayıt bulunamadı — /short?diag=1 ile ham yanıta bak.");
}
async function finraShort(env, q) {
  if (q.get("diag") === "1") {
    const diag = { candidates: finraCandidates(3), probe: await finraProbe() };
    try { diag.latest = await finraLatest(env, diag); }
    catch (e) { diag.error = errStr(e); }
    return diag;
  }
  const forced = (q.get("date") || "").trim();
  const L = forced ? { set: FINRA_SETS[0], date: forced } : await finraLatest(env);
  const date = L.date, set = L.set;
  const syms = (q.get("syms") || "").split(",").map(x => x.trim().toUpperCase()).filter(Boolean).slice(0, 60);
  const base = { compareFilters: [{ compareType: "EQUAL", fieldName: "settlementDate", fieldValue: date }] };
  if (syms.length) {
    const body = Object.assign({ limit: 5000 }, base);
    body.compareFilters = base.compareFilters.slice();
    const rows = (await finraPost(set, body)).map(finraRow);
    const want = new Set(syms);
    const hit = rows.filter(r => want.has(r.sym));
    return { date, set, mode: "syms", asked: syms.length, found: hit.length, rows: hit,
             missing: syms.filter(s => !hit.some(r => r.sym === s)) };
  }
  /* tarama: days-to-cover eşiğini geçenler — sıralamayı FINRA yapar, biz ilk N'i alırız */
  const minDtc = parseFloat(q.get("minDtc") || "5") || 5;
  const body = Object.assign({ limit: 3000 }, base);
  const all = (await finraPost(set, body)).map(finraRow);
  const rows = all.filter(r => r.dtc != null && r.dtc >= minDtc && r.cur > 0)
                  .sort((a, b) => b.dtc - a.dtc).slice(0, 40);
  return { date, set, mode: "scan", minDtc, scanned: all.length, rows };
}
function errStr(e) {
  if (!e) return "(boş hata)";
  if (typeof e === "string") return e;
  const p = [];
  if (e.name) p.push(e.name);
  if (e.message) p.push(e.message);
  if (e.stack) p.push("@ " + String(e.stack).split("\n")[0]);
  if (!p.length) { try { p.push(JSON.stringify(e)); } catch (_) {} }
  return p.join(" · ") || "(mesajsız)";
}
function stageErr(stage, e) { const err = (e instanceof Error) ? e : new Error(errStr(e)); err.stage = stage; return err; }

/* ---------- ham teşhis ---------- */
async function probeText(env) {
  const out = ["PROBE ·", "KV binding (PORTFOLIO): " + (env.PORTFOLIO ? "VAR" : "YOK")];
  if (env.PORTFOLIO) {
    try {
      const l = await env.PORTFOLIO.list();
      out.push("KV anahtarları: " + (l.keys.map(k => k.name).join(", ") || "(boş)"));
      const raw = await pickKV(env);
      if (raw) {
        const j = JSON.parse(raw);
        const nk = j.keys ? Object.keys(j.keys).length : 0;
        const ps = (j.keys && Array.isArray(j.keys.positions)) ? j.keys.positions : [];
        out.push("Okunan blob: updatedAt=" + (j.updatedAt || "?") + " · anahtar sayısı=" + nk + " · pozisyon=" + ps.length);
        // 36 vs 31 uyumu: aktif / kapalı(toz) dökümü
        const isClosed = p => (num(p.qty) <= 1e-6) || (p.price > 0 && Math.abs(p.qty) * p.price < 0.5);
        const closed = ps.filter(isClosed);
        const val = p => p.lev ? p.qty * (p.price - p.cost) : p.qty * p.price;
        out.push("Aktif: " + (ps.length - closed.length) + " · Kapalı/toz: " + closed.length);
        out.push("Kapalı/toz olanlar: " + (closed.map(p => `${p.t}(${(+p.qty).toFixed(4)}@${p.price}=$${Math.round(val(p))})`).join(", ") || "—"));
        // P.8: sabah mesajındaki grup %'lerin neden 0.00 çıktığını görmek için son 5 kayıt
        const hist = Array.isArray(j.keys.history) ? j.keys.history.slice().sort((a, b) => a.date < b.date ? -1 : 1) : [];
        out.push("Son 5 history kaydı:");
        hist.slice(-5).forEach(h => {
          const g = h.groups || {};
          const gs = Object.keys(g).map(k => k + "=" + Math.round(num(g[k].val))).join(" · ") || "grup yok";
          out.push(`  ${h.date} · total=${Math.round(num(h.total))} · ${gs} · src=${h.src || "?"}`);
        });
      } else out.push("KV'den veri okunamadı (boş).");
    } catch (e) { out.push("KV hata: " + errStr(e)); }
  }
  out.push("SYNC_URL: " + (env.SYNC_URL ? env.SYNC_URL : "(yok)"));
  out.push("WA_PHONE: " + (env.WA_PHONE ? "VAR" : "YOK") + " · WA_APIKEY: " + (env.WA_APIKEY ? "VAR" : "YOK"));
  return out.join("\n");
}

/* ---------- ana akış ---------- */
async function run(env, kind, send) {
  let state;
  try { state = await fetchState(env); } catch (e) { throw stageErr("STATE", e); }
  const keys = (state && state.keys) || {};
  // V6.2: canlı fiyat — özet/uyarı son sync yerine anlık fiyatla
  let priceRep = null;
  try {
    priceRep = await fetchLivePrices(env, keys);
    if (priceRep && (priceRep.crypto + priceRep.stock) > 0) keys.priceUpd = new Date().toISOString();
  } catch (e) { priceRep = { crypto: 0, stock: 0, fail: ["canlı fiyat: " + errStr(e)] }; }
  let text;
  try {
    const s = computeSummary(keys);
    text = (kind === "weekly") ? buildWeekly(s, keys, state) : buildDaily(s, state);
  } catch (e) { throw stageErr("HESAP", e); }
  if (send) { try { await sendWA(env, text); } catch (e) { throw stageErr("WHATSAPP", e); } }
  return { text, priceRep };
}

async function pickKV(env) {
  let raw = await env.PORTFOLIO.get("state");
  if (!raw) {
    const l = await env.PORTFOLIO.list();
    if (l.keys && l.keys.length) raw = await env.PORTFOLIO.get(l.keys[0].name);
  }
  return raw;
}

async function fetchState(env) {
  // 1) KV varsa doğrudan oradan oku (en sağlam yol)
  if (env.PORTFOLIO) {
    const raw = await pickKV(env);
    if (!raw) throw new Error("KV bağlı ama portfolio-kv boş — sync yapılmış mı?");
    let j; try { j = JSON.parse(raw); } catch (e) { throw new Error("KV verisi JSON değil: " + errStr(e)); }
    if (!j || !j.keys) throw new Error("KV verisinde 'keys' yok");
    return j;
  }
  // 2) KV yoksa HTTP yedeği (SYNC_URL/SYNC_PW ile)
  const base = (env.SYNC_URL || "").trim().replace(/\/+$/, "");
  if (!base) throw new Error("Ne KV (PORTFOLIO) ne SYNC_URL tanımlı — biri gerekli");
  if (!/^https?:\/\//i.test(base)) throw new Error("SYNC_URL 'https://' ile başlamalı");
  let r; try { r = await fetch(base + "/state", { headers: { "X-Pw": env.SYNC_PW || "" } }); }
  catch (e) { throw new Error("/state'e ulaşılamadı (worker→worker engeli olabilir; KV kullan): " + errStr(e)); }
  if (!r.ok) throw new Error("/state " + r.status + (r.status === 401 ? " (SYNC_PW yanlış?)" : ""));
  let j; try { j = await r.json(); } catch (e) { throw new Error("/state JSON değil: " + errStr(e)); }
  if (!j || !j.keys) throw new Error("/state boş/keys yok");
  return j;
}

/* ---------- hesap (uygulamayla aynı formüller) ---------- */
function num(v) { const n = parseFloat(v); return isFinite(n) ? n : 0; }
function computeSummary(k) {
  const positions = Array.isArray(k.positions) ? k.positions : [];
  const exchCash = k.exchCash || {};
  const history = Array.isArray(k.history) ? k.history : [];
  const val = p => p.lev ? p.qty * (p.price - p.cost) : p.qty * p.price;
  const cost = p => p.lev ? 0 : p.qty * p.cost;
  const mkt = p => p.qty * p.price;
  const isClosed = p => (num(p.qty) <= 1e-6) || (p.price > 0 && Math.abs(p.qty) * p.price < 0.5);
  const cashTotal = Object.values(exchCash).reduce((a, v) => a + num(v), 0);
  const posTotal = positions.reduce((a, p) => a + num(val(p)), 0);
  const costTotal = positions.reduce((a, p) => a + num(cost(p)), 0);
  const grand = cashTotal + posTotal;
  const totalPL = posTotal - costTotal;
  const totalPLp = costTotal ? totalPL / costTotal * 100 : 0;
  const todayPL = positions.reduce((a, p) => { const d = num(p.day); return a + num(mkt(p)) * (d / 100) / (1 + d / 100); }, 0);
  const todayPLp = grand ? todayPL / grand * 100 : 0;
  const lookback = days => {
    if (history.length < 2) return null;
    const hist = history.slice().sort((a, b) => a.date < b.date ? -1 : 1);
    const cut = new Date(); cut.setUTCDate(cut.getUTCDate() - days);
    const cutStr = cut.toISOString().slice(0, 10);
    let ref = null; for (const h of hist) { if (h.date <= cutStr) ref = h; }
    if (!ref) ref = hist[0];
    const abs = grand - num(ref.total);
    return { abs, pct: ref.total ? abs / num(ref.total) * 100 : 0 };
  };
  const live = positions.filter(p => !isClosed(p) && p.day != null);
  const byDay = live.slice().sort((a, b) => num(b.day) - num(a.day));
  return { grand, cashTotal, posTotal, costTotal, totalPL, totalPLp, todayPL, todayPLp,
    lb7: lookback(7), lb30: lookback(30),
    gainers: byDay.filter(p => num(p.day) > 0).slice(0, 3),
    losers: byDay.filter(p => num(p.day) < 0).slice(-3).reverse(),
    priceUpd: k.priceUpd || "" };
}

/* ---------- biçim ---------- */
function usd(n) { const neg = n < 0; n = Math.abs(Math.round(n)); return (neg ? "-" : "") + n.toLocaleString("en-US") + " $"; }
function sg(n, d = 2) { return (n >= 0 ? "+" : "") + n.toFixed(d); }
function trTime(iso) { try { const d = iso ? new Date(iso) : new Date(); return d.toLocaleString("tr-TR", { timeZone: "Europe/Istanbul", day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" }); } catch (e) { return "—"; } }
function moversLine(a) { return a.map(p => `${p.t} ${sg(num(p.day), 1)}%`).join(" · ") || "—"; }
function buildDaily(s, state) {
  return ["📊 *PORT · Günlük Özet*", `Toplam: *${usd(s.grand)}*`,
    `Bugün: ${usd(s.todayPL)} (${sg(s.todayPLp)}%)`,
    `Toplam K/Z: ${usd(s.totalPL)} (${sg(s.totalPLp)}%)`,
    (s.lb7 ? `7g: ${usd(s.lb7.abs)} (${sg(s.lb7.pct)}%)` : ""), "",
    `🟢 ${moversLine(s.gainers)}`, `🔴 ${moversLine(s.losers)}`, "",
    `🕗 Fiyatlar: ${trTime(s.priceUpd)} · Sync: ${trTime(state && state.updatedAt)}`
  ].filter(x => x !== "").join("\n");
}
function buildWeekly(s, keys, state) {
  const perf = weekPerformers(keys); const L = ["🗓️ *PORT · Haftalık Özet*", `Toplam: *${usd(s.grand)}*`];
  if (s.lb7) L.push(`Bu hafta: ${usd(s.lb7.abs)} (${sg(s.lb7.pct)}%)`);
  if (s.lb30) L.push(`30g: ${usd(s.lb30.abs)} (${sg(s.lb30.pct)}%)`);
  L.push(`Toplam K/Z: ${usd(s.totalPL)} (${sg(s.totalPLp)}%)`);
  L.push(`Nakit: ${usd(s.cashTotal)} · Pozisyon: ${usd(s.posTotal)}`);
  if (perf) { L.push(""); L.push(`🟢 ${perf.up.map(x => `${x.t} ${sg(x.pct, 1)}%`).join(" · ") || "—"}`); L.push(`🔴 ${perf.down.map(x => `${x.t} ${sg(x.pct, 1)}%`).join(" · ") || "—"}`); }
  L.push(""); L.push(`🕗 ${trTime(state && state.updatedAt)}`); return L.join("\n");
}
function weekPerformers(k) {
  const positions = Array.isArray(k.positions) ? k.positions : [];
  const history = Array.isArray(k.history) ? k.history : [];
  if (history.length < 2) return null;
  const hist = history.slice().sort((a, b) => a.date < b.date ? -1 : 1);
  const cut = new Date(); cut.setUTCDate(cut.getUTCDate() - 7);
  const cutStr = cut.toISOString().slice(0, 10);
  let ref = null; for (const h of hist) { if (h.date <= cutStr && h.prices) ref = h; }
  if (!ref || !ref.prices) return null;
  const out = [];
  for (const p of positions) { const q = num(p.qty); if (q <= 1e-6) continue; const rp = ref.prices[p.t]; if (rp == null || rp <= 0) continue; out.push({ t: p.t, pct: (p.price - rp) / rp * 100 }); }
  out.sort((a, b) => b.pct - a.pct);
  return { up: out.slice(0, 3), down: out.slice(-3).reverse() };
}

/* ---------- CANLI FİYAT (uygulamayla aynı kaynaklar) ---------- */
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function ua() { return { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36" }; }
function setStockPrice(positions, sym, price, dp) {
  positions.forEach(p => { if (!p.cg && p.t && p.t.toUpperCase() === sym.toUpperCase()) {
    p.price = price; if (dp != null && !isNaN(+dp)) p.day = +(+dp).toFixed(2);
    p.srcAt = Date.now(); p.src = "live";   // P.8: cihazlar arası merge fiyatı bununla ayırt ediyor
  } });
}
async function fetchLivePrices(env, keys) {
  const positions = Array.isArray(keys.positions) ? keys.positions : [];
  const rep = { crypto: 0, stock: 0, fail: [] };

  // 1) Kripto — CoinGecko (anahtarsız; CG_KEY varsa demo başlığı)
  const cgIds = [...new Set(positions.filter(p => p.cg).map(p => p.cg))];
  if (cgIds.length) {
    try {
      const h = Object.assign({}, ua());
      if (env.CG_KEY) h["x-cg-demo-api-key"] = env.CG_KEY;
      const r = await fetch("https://api.coingecko.com/api/v3/simple/price?ids=" + encodeURIComponent(cgIds.join(",")) + "&vs_currencies=usd&include_24hr_change=true", { headers: h });
      if (r.ok) {
        const j = await r.json();
        positions.forEach(p => {
          if (p.cg && j[p.cg] && j[p.cg].usd != null) {
            p.price = j[p.cg].usd;
            if (j[p.cg].usd_24h_change != null) p.day = +(+j[p.cg].usd_24h_change).toFixed(2);
            p.srcAt = Date.now(); p.src = "live";   // P.8: cihazlar arası merge fiyatı bununla ayırt ediyor
            rep.crypto++;
          }
        });
      } else rep.fail.push("coingecko " + r.status + (r.status === 429 ? " (CG_KEY gerekebilir)" : ""));
    } catch (e) { rep.fail.push("coingecko: " + errStr(e)); }
  }

  // 2) Hisse — Finnhub (tek tek), Twelvedata yedek
  const fh = (env.FINNHUB || "").trim();
  const stocks = [...new Set(positions.filter(p => !p.cg && !p.manual && p.t).map(p => p.t.toUpperCase()))];
  const failSyms = [];
  if (fh) {
    for (let si = 0; si < stocks.length; si++) {
      const sym = stocks[si];
      try {
        const r = await fetch("https://finnhub.io/api/v1/quote?symbol=" + encodeURIComponent(sym) + "&token=" + encodeURIComponent(fh), { headers: ua() });
        if (r.ok) { const q = await r.json(); if (q && q.c > 0) { setStockPrice(positions, sym, q.c, q.dp); rep.stock++; } else failSyms.push(sym); }
        else {
          failSyms.push(sym);
          // P.8: 429'da kalan semboller hiç denenmeden atlanıyordu — artık hepsi TwelveData yedeğine düşsün diye ekleniyor.
          if (r.status === 429) { failSyms.push(...stocks.slice(si + 1)); rep.fail.push("finnhub 429 (dakika limiti) — kalan " + (stocks.length - si - 1) + " sembol TwelveData'ya düştü"); break; }
        }
      } catch (e) { failSyms.push(sym); }
      await sleep(1100); // Finnhub ~60/dk
    }
  } else { failSyms.push(...stocks); rep.fail.push("FINNHUB secret yok → hisse fiyatı son sync'ten"); }

  const td = (env.TWELVEDATA || "").trim();
  if (td && failSyms.length) {
    try {
      const syms = [...new Set(failSyms)].filter(Boolean);
      const r = await fetch("https://api.twelvedata.com/quote?symbol=" + encodeURIComponent(syms.join(",")) + "&apikey=" + encodeURIComponent(td), { headers: ua() });
      if (r.ok) {
        const j = await r.json();
        syms.forEach(sym => {
          const q = syms.length === 1 ? j : j[sym];
          if (!q || q.status === "error") return;
          const pr = parseFloat(q.close), dp = parseFloat(q.percent_change);
          if (pr > 0) { setStockPrice(positions, sym, pr, dp); rep.stock++; }
        });
      } else rep.fail.push("twelvedata " + r.status);
    } catch (e) { rep.fail.push("twelvedata: " + errStr(e)); }
  }
  return rep;
}

/* ---------- WhatsApp (CallMeBot) ---------- */
/* V-10.2: CallMeBot GET-URL sınırı uzun mesajı kırpıyor.
   Bölme gerekiyorsa ÖNCE AI yorumundan (🤖) ayrılır; hâlâ uzunsa satır sınırlarından
   bölünür. Parça etiketi (1/2) yazılmaz — kullanıcı istemedi. */
const WA_LIM = 800;
function waChunk(text, lim) {
  if (text.length <= lim) return [text];
  const parts = []; let cur = "";
  for (const ln of text.split("\n")) {
    if (cur && (cur.length + 1 + ln.length) > lim) { parts.push(cur); cur = ln; }
    else if (!cur && ln.length > lim) {
      let rest = ln;
      while (rest.length > lim) { const c = rest.lastIndexOf(" ", lim); const at = c > 40 ? c : lim; parts.push(rest.slice(0, at)); rest = rest.slice(at).trim(); }
      cur = rest;
    } else cur = cur ? cur + "\n" + ln : ln;
  }
  if (cur) parts.push(cur);
  return parts;
}
function waParts(text) {
  if (text.length <= WA_LIM) return [text];
  const i = text.search(/\n\*(Günlük|Haftalık) AI Yorum\*/);   // V-10.3: bölme noktası AI başlığı
  let base = i > 0 ? [text.slice(0, i).replace(/\s+$/, ""), text.slice(i + 1)] : [text];
  const out = [];
  base.forEach(b => waChunk(b, WA_LIM).forEach(x => out.push(x)));
  return out;
}
async function sendWA(env, text) {
  const parts = waParts(text);
  let last = "";
  for (let i = 0; i < parts.length; i++) {
    last = await sendWAOne(env, parts[i]);
    if (i < parts.length - 1) await sleep(2500);   // arka arkaya istekte CallMeBot 403 veriyor
  }
  return last;
}
async function sendWAOne(env, text) {
  const phone = (env.WA_PHONE || "").replace(/[^\d]/g, "");
  const apikey = (env.WA_APIKEY || "").trim();
  if (!phone) throw new Error("WA_PHONE secret'i eksik/boş");
  if (!apikey) throw new Error("WA_APIKEY secret'i eksik/boş");
  const u = "https://api.callmebot.com/whatsapp.php?phone=" + encodeURIComponent(phone) + "&text=" + encodeURIComponent(text) + "&apikey=" + encodeURIComponent(apikey);
  const opt = { headers: {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.8"
  } };
  let r; try { r = await fetch(u, opt); } catch (e) { throw new Error("CallMeBot'a ulaşılamadı: " + errStr(e)); }
  // 403/429 → WAF/oran sınırı: kısa bekleyip 1 kez daha dene
  if (r.status === 403 || r.status === 429) {
    await new Promise(res => setTimeout(res, 3000));
    try { r = await fetch(u, opt); } catch (e) { throw new Error("CallMeBot'a ulaşılamadı (2. deneme): " + errStr(e)); }
  }
  const body = await r.text();
  if (r.status === 403) throw new Error("CallMeBot 403 (WAF/oran sınırı — biraz bekleyip tekrar dene; sık test blokluyor)");
  if (!r.ok) throw new Error("CallMeBot " + r.status + ": " + body.slice(0, 200));
  return body;
}

/* ============================================================
   V-11.9 · ENDEKS DAHİL ETME ARBİTRAJI (S&P 500 adayları)
   Kaynak: Financial Modeling Prep "stable" uçları (uygulamadaki FMP anahtarı).
   Kriterler S&P Dow Jones Indices'in yayımladığı EKLEME şartlarıdır:
     · düzeltilmemiş piyasa değeri ≥ 22,7 milyar $ (1 Tem 2025'ten beri geçerli)
     · son çeyrek GAAP net kârı > 0 VE son 4 çeyrek toplamı > 0
     · yıllık dolar hacim / float-ayarlı piyasa değeri ≥ 0,75
     · aylık en az 250.000 hisse işlem (son 6 ay)
     · ABD merkezli, NYSE/Nasdaq'ta, halka açılalı ≥ 12 ay
   ÖLÇÜLEMEYEN: halka açık pay (float) oranı — ücretsiz FMP planında yok.
   Bu kriter puanlanmaz ve PAYDADAN da çıkarılır (eksik veri şirketi cezalandırmaz).
   Likidite oranı float yerine düzeltilmemiş piyasa değeriyle hesaplanır → VEKİL ölçü,
   "~" ile işaretlenir; gerçek oran her zaman bundan yüksektir (alt sınır).
   Uçlar: /index?scan=1 · /index?cached=1 (ağa çıkmaz) · /index?diag=1 (ham yanıt)
============================================================ */
const IDX_FMP = "https://financialmodelingprep.com/stable/";
const IDX_MINCAP = 22.7e9;      // S&P DJI eşiği
const IDX_FALR = 0.75;          // likidite oranı şartı
const IDX_MINVOL = 250000;      // aylık hisse
const IDX_CAPMARGIN = 1.25;     // eşiğin %25 üstü = rahat geçiyor

function idxNum(v) { const n = parseFloat(v); return isFinite(n) ? n : null; }
/* ddPick kalıbı: sağlayıcı alan adını değiştirince rapor sessizce boşalmasın */
function idxPick(o, names) {
  if (!o || typeof o !== "object") return null;
  for (const n of names) { const v = o[n]; if (v !== undefined && v !== null && v !== "") return v; }
  const low = {}; Object.keys(o).forEach(k => { low[k.toLowerCase()] = o[k]; });
  for (const n of names) { const v = low[String(n).toLowerCase()]; if (v !== undefined && v !== null && v !== "") return v; }
  return null;
}
async function idxFmp(key, path, qs, budget, diag) {
  if (budget && !budget.take()) throw new Error("subrequest bütçesi doldu");
  const q = Object.assign({}, qs, { apikey: key });
  const u = IDX_FMP + path + "?" + Object.keys(q).map(k => encodeURIComponent(k) + "=" + encodeURIComponent(q[k])).join("&");
  let r; try { r = await fetch(u, { headers: { "Accept": "application/json" } }); }
  catch (e) { throw new Error("FMP'ye ulaşılamadı (" + path + "): " + errStr(e)); }
  const body = await r.text();
  if (diag) diag.push({ path: path + (qs && qs.symbol ? " " + qs.symbol : "") + (qs && qs.exchange ? " " + qs.exchange : ""), status: r.status, bytes: body.length, head: body.slice(0, 200) });
  if (!r.ok) throw new Error("FMP " + path + " " + r.status + ": " + body.slice(0, 200));
  if (!body.trim()) throw new Error("FMP " + path + " boş gövde döndü");
  let j; try { j = JSON.parse(body); } catch (e) { throw new Error("FMP " + path + " JSON değil (" + body.length + " bayt): " + body.slice(0, 160)); }
  const em = idxPick(j, ["Error Message", "error"]);
  if (em && !Array.isArray(j)) throw new Error("FMP " + path + ": " + em);
  return j;
}

/* V-12.2: UYGUNLUK KAPILARI — S&P 500'ün "olmazsa olmaz" şartları puanla ödünlenmez.
   ABD merkezli olmayan / ADR / halka açılalı 12 aydan yeni şirket endekse GİREMEZ;
   bunlar skorda 15 puan kaybı değil, doğrudan ELEME sebebidir (V-12.1'de listeyi
   TSM, SK hynix ve 2 aylık SPCX gibi hiç aday olamayacak isimler dolduruyordu).
   Kapılar geçildikten sonra puanlanan üç ölçü kalır: kârlılık, likidite, büyüklük marjı. */
const IDX_ADR_RE = /american depositary|depositary (share|receipt)|\bADSs?\b|\bADRs?\b/i;
const IDX_SYM_RE = /^[A-Z]{1,5}$/;   /* nokta/eğik çizgi içerenler birim, warrant veya ikinci sınıf */
/* V-12.3: Nasdaq listesi hisse senedi olmayan enstrümanları da içeriyor ve bunlara İHRAÇÇININ
   piyasa değerini yazıyor — "AT&T %5,350 Notes due 2066" 139 milyar $ görünüyordu (gerçekte 16).
   Tahvil, tercihli pay, ZONES, birim ve warrant adları elenir. */
/* V-12.6: Havuzun tepesi hep aynı yabancı devlerle doluydu (TSM, ASML, HSBC, TM…) ve
   her koşumda 20 profil isteğinin 19'u onlara gidiyordu. Yabancı şirketlerin hukuki eki
   ve "Ordinary Shares" / "New York Registry Shares" ifadeleri profil çekmeden eleniyor. */
const IDX_FOREIGN_RE = /\bplc\b|\bN\.?V\.?$|\bS\.?A\.?$|\bA\.?G\.?$|\bAB\b|\bASA\b|\bOyj\b|\bA\/S\b|\bSpA\b|\bNV\b|ordinary shares|registry shares|\bADS\b/i;
const IDX_INSTR_RE = /notes? due|\bdebenture|preferred|\bZONES\b|\bunits?\b|\bwarrants?\b|\brights?\b|subordinated|\d\s*%|%\s*\d|capital securities/i;
/* Nasdaq "BRK/B" yazıyor, Wikipedia "BRK.B" — üye eşleşmesi kaçmasın */
function idxNormSym(x) { return String(x || "").trim().toUpperCase().replace(/[\/\-]/g, "."); }

/* Ucuz ön eleme: yalnız Nasdaq listesindeki ad ve sembolle, tek bir istek harcamadan */
function idxPreGate(c) {
  if (!IDX_SYM_RE.test(c.sym)) return "sembol biçimi (birim/warrant/ikinci sınıf olabilir)";
  if (IDX_ADR_RE.test(c.name)) return "ADR/depo sertifikası";
  if (IDX_INSTR_RE.test(c.name)) return "hisse senedi değil (tahvil/tercihli/birim)";
  if (IDX_FOREIGN_RE.test(c.name)) return "yabancı şirket (ad ekinden)";
  return null;
}
/* Profil geldikten sonraki kesin kapılar */
function idxGate(c, prof, minCap) {
  if (!prof) return "profil alınamadı";
  /* Nasdaq'ın piyasa değeri güvenilmez (enstrümanlarda ihraççıyı yazıyor) — FMP'ninki esas alınır */
  const mcf = idxNum(idxPick(prof, ["marketCap", "mktCap"]));
  if (mcf !== null) {
    c.mcapSrc = "fmp"; c.mcapNdq = c.mcap; c.mcap = mcf;
    if (mcf < minCap) return "FMP piyasa değeri eşiğin altında (" + Math.round(mcf / 1e9) + " mia $ · Nasdaq " + Math.round((c.mcapNdq || 0) / 1e9) + " mia yazıyordu)";
  }
  if (idxPick(prof, ["isAdr"]) === true) return "ADR — ABD şirketi değil";
  if (idxPick(prof, ["isEtf"]) === true || idxPick(prof, ["isFund"]) === true) return "fon/ETF";
  const ctry = String(idxPick(prof, ["country"]) || "").toUpperCase();
  if (ctry && ctry !== "US") return "merkez ülkesi " + ctry;
  const ipo = String(idxPick(prof, ["ipoDate", "ipo_date"]) || "").slice(0, 10);
  if (/^\d{4}-\d{2}-\d{2}$/.test(ipo)) {
    const d = Math.floor((Date.now() - Date.parse(ipo + "T00:00:00Z")) / 864e5);
    if (d < 365) return "halka açılalı " + d + " gün (12 ay şartı)";
  }
  return null;
}

function idxScore(c, prof, inc, minCap, err) {
  const parts = [];
  const P = (k, label, w, ok, pass, val, note) => parts.push({ k: k, label: label, w: w, ok: ok, pass: pass, val: val, note: note || null });
  const mcap = c.mcap;

  const ea = inc || { q: [], src: null, note: "kârlılık verisi alınmadı" };
  const q = ea.q || [];
  if (q.length >= 4) {
    const last = q[0].val, sum = q.slice(0, 4).reduce((a, b) => a + b.val, 0);
    const drv = q.slice(0, 4).some(x => x.derived);
    P("earn", "Son çeyrek ve 4 çeyrek toplam GAAP kârı > 0", 50, true, (last > 0 && sum > 0),
      { last: last, sum: sum, src: ea.src, upTo: q[0].end, derived: drv },
      (drv ? "~ bir çeyrek yıllıktan türetildi · " : "") + "kaynak: " + (ea.src || "—"));
  } else {
    P("earn", "GAAP kârlılığı", 50, false, null, { n: q.length, src: ea.src },
      q.length ? ("yalnız " + q.length + " çeyrek bulundu · " + (ea.note || "")) : (ea.note || "kârlılık verisi gelmedi"));
  }

  const price = idxNum(idxPick(prof, ["price", "lastPrice"]));
  const avol = idxNum(idxPick(prof, ["averageVolume", "avgVolume", "volAvg", "averageDailyVolume"]));
  if (price !== null && avol !== null && mcap > 0) {
    const falr = (price * avol * 252) / mcap;
    P("liq", "Likidite: yıllık $ hacim / piyasa değeri ≥ 0,75", 30, true, falr >= IDX_FALR, falr,
      "~ float verisi yok, düzeltilmemiş piyasa değeriyle hesaplandı — gerçek oran bundan yüksektir");
  } else P("liq", "Likidite", 30, false, null, null, "fiyat veya ortalama hacim gelmedi");

  P("cap", "Piyasa değeri eşiğin ≥%25 üstünde", 20, true, mcap >= minCap * IDX_CAPMARGIN, mcap, null);

  const ipo = String(idxPick(prof, ["ipoDate", "ipo_date"]) || "").slice(0, 10);
  const days = /^\d{4}-\d{2}-\d{2}$/.test(ipo) ? Math.floor((Date.now() - Date.parse(ipo + "T00:00:00Z")) / 864e5) : null;

  const unmeas = ["Halka açık pay ≥ %50 — ücretsiz kaynakta float verisi yok"];
  if (avol !== null && (avol * 21) < IDX_MINVOL) unmeas.push("Aylık hacim 250 bin hissenin altında görünüyor");

  const den = parts.filter(p => p.ok).reduce((a, p) => a + p.w, 0);
  const got = parts.filter(p => p.ok && p.pass).reduce((a, p) => a + p.w, 0);
  const fails = parts.filter(p => p.ok && !p.pass).map(p => p.label);
  /* Kârlılık S&P 500'ün olmazsa olmaz şartı ve puanın yarısı. Ölçülemediğinde kalan
     kriterleri 100'e ölçeklemek "kârlılığı bilinmeyen şirket %100 uygun" gibi okunuyordu
     (Bloom Energy zarar ederken 100 çıktı). Ölçülemezse skor GÖSTERİLMEZ (§0F). */
  const earnMeasured = parts.some(p => p.k === "earn" && p.ok);
  return {
    sym: c.sym, name: c.name, exch: c.exch, sector: c.sector, mcap: mcap,
    price: price, avol: avol, ipo: ipo || null, ipoDays: days,
    earnOk: q.length >= 4,
    earnSrc: ea.src || null,
    score: (earnMeasured && den > 0) ? Math.round(got * 100 / den) : null,
    scoreNote: earnMeasured ? null : "kârlılık doğrulanamadı — skor verilmedi",
    den: den, parts: parts, fails: fails, unmeasured: unmeas,
    err: (err && err.length) ? err : null
  };
}

/* V-12.0: KAYNAK YOKLAMASI — /index?probe=1
   FMP ücretsiz planı "sp500-constituent" ve muhtemelen "company-screener" uçlarını
   402 ile kapatıyor. Hangi kaynağın worker'dan gerçekten geçtiğini TAHMİN ETMEK yerine
   yokluyoruz: her aday için durum kodu, bayt, ilk 200 karakter ve kaç kayıt çıkarılabildiği.
   Buradan çıkan sonuca göre üye listesi + evren kaynağı seçilecek. */
async function idxTry(name, url, opt, parse) {
  const o = { name: name, url: url.replace(/apikey=[^&]*/, "apikey=***") };
  let r;
  try { r = await fetch(url, opt || {}); }
  catch (e) { o.status = null; o.error = "ulaşılamadı: " + errStr(e); return o; }
  o.status = r.status;
  let body = "";
  try { body = await r.text(); } catch (e) { o.error = "gövde okunamadı: " + errStr(e); return o; }
  o.bytes = body.length;
  o.head = body.slice(0, 200);
  if (parse) { try { o.parsed = parse(body); } catch (e) { o.parsed = "ayrıştırılamadı: " + errStr(e); } }
  return o;
}
/* V-12.1: KAYNAK DEĞİŞİMİ (yoklama sonucuna göre)
   FMP ücretsiz planı: profile ✓ · income-statement ✓ · company-screener ✗402 · sp500-constituent ✗402
   iShares holdings CSV'si worker'a HTML (bot koruması) döndürüyor → kullanılmıyor.
   ÜYE LİSTESİ  → Wikipedia "List of S&P 500 companies" (HTML tablo; yedek: ham wikitext)
   EVREN        → Nasdaq'ın anahtarsız genel tarayıcı JSON'u (api.nasdaq.com/api/screener/stocks)
   Her ikisi de kamuya açık ve mekanik. Sonuç KV'de 24 saat tutulur (liste yılda ~20 kez değişir). */
const IDX_WIKI_HTML = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies";
const IDX_WIKI_RAW = "https://en.wikipedia.org/w/index.php?title=List_of_S%26P_500_companies&action=raw";
const IDX_NDQ = "https://api.nasdaq.com/api/screener/stocks";

function idxSymsFromWikiHtml(html) {
  const i = html.indexOf('id="constituents"');
  if (i < 0) return [];
  const end = html.indexOf("</table>", i);
  const tbl = html.slice(i, end > 0 ? end : i + 300000);
  const out = [];
  const re = /<tr[^>]*>\s*<t[dh][^>]*>\s*(?:<a[^>]*>)?\s*([A-Z][A-Z0-9.\-]{0,5})\s*(?:<\/a>)?\s*<\/t[dh]>/g;
  let m; while ((m = re.exec(tbl))) out.push(m[1]);
  return out;
}
function idxSymsFromWikiRaw(txt) {
  const out = [];
  const re = /^\|\s*(?:\[\s*https?:\/\/\S+\s+)?([A-Z][A-Z0-9.\-]{0,5})\s*\]?\s*$/gm;
  let m; while ((m = re.exec(txt))) out.push(m[1]);
  return out;
}
async function idxMembers(env, diag) {
  let cached = null;
  try { cached = env.PORTFOLIO ? await env.PORTFOLIO.get("sp500:members") : null; } catch (e) {}
  if (cached) {
    try {
      const j = JSON.parse(cached);
      if (j && Array.isArray(j.syms) && j.syms.length >= 400) {
        if (diag) diag.push({ path: "sp500:members (KV)", status: 200, bytes: cached.length, head: j.syms.length + " sembol · " + j.src + " · " + j.at });
        return { syms: new Set(j.syms.map(idxNormSym)), src: j.src + " (önbellek " + j.at + ")" };
      }
    } catch (e) {}
  }
  const tries = [
    { name: "wikipedia:html", url: IDX_WIKI_HTML, fn: idxSymsFromWikiHtml },
    { name: "wikipedia:raw", url: IDX_WIKI_RAW, fn: idxSymsFromWikiRaw }
  ];
  const errs = [];
  for (const t of tries) {
    let body = "";
    try {
      const r = await fetch(t.url, { headers: ua() });
      body = await r.text();
      if (diag) diag.push({ path: t.name, status: r.status, bytes: body.length, head: body.slice(0, 120) });
      if (!r.ok) { errs.push(t.name + " " + r.status); continue; }
    } catch (e) { errs.push(t.name + " ulaşılamadı: " + errStr(e)); continue; }
    let syms = [];
    try { syms = t.fn(body); } catch (e) { errs.push(t.name + " ayrıştırılamadı: " + errStr(e)); continue; }
    const uniq = Array.from(new Set(syms.filter(Boolean)));
    if (diag) diag.push({ path: t.name + " ayrıştırma", status: 200, bytes: uniq.length, head: uniq.slice(0, 8).join(",") });
    /* S&P 500 her zaman ~500 şirkettir; sayı bu aralıkta değilse ayrıştırma bozulmuş demektir */
    if (uniq.length < 450 || uniq.length > 560) { errs.push(t.name + ": " + uniq.length + " sembol çıktı (450-560 beklenir)"); continue; }
    const at = new Date().toISOString().slice(0, 10);
    try { if (env.PORTFOLIO) await env.PORTFOLIO.put("sp500:members", JSON.stringify({ syms: uniq, src: t.name, at: at }), { expirationTtl: 60 * 60 * 24 }); } catch (e) {}
    return { syms: new Set(uniq.map(idxNormSym)), src: t.name };
  }
  throw new Error("S&P 500 üye listesi alınamadı · " + errs.join(" · "));
}

function idxNdqNum(v) {
  const s = String(v == null ? "" : v).replace(/[$,\s]/g, "");
  if (!s || s === "NA" || s === "--") return null;
  const n = parseFloat(s);
  return isFinite(n) ? n : null;
}
async function idxUniverse(minCap, members, diag) {
  const uni = new Map(); const errs = [];
  for (const ex of ["NYSE", "NASDAQ"]) {
    let rows = null;
    /* önce piyasa değeri süzgeciyle (küçük yanıt), boş dönerse filtresiz tam liste */
    for (const q of ["&marketcap=mega%7Clarge&country=United+States", "&marketcap=mega%7Clarge", ""]) {
      const u = IDX_NDQ + "?tableonly=true&limit=6000&offset=0&exchange=" + ex + q;
      let body = "";
      try {
        const r = await fetch(u, { headers: Object.assign({ "Accept": "application/json" }, ua()) });
        body = await r.text();
        if (diag) diag.push({ path: "nasdaq " + ex + (q ? (q.indexOf("country") > 0 ? " (mega|large + ABD)" : " (mega|large)") : " (tümü)"), status: r.status, bytes: body.length, head: body.slice(0, 120) });
        if (!r.ok) { errs.push("nasdaq " + ex + " " + r.status); continue; }
      } catch (e) { errs.push("nasdaq " + ex + " ulaşılamadı: " + errStr(e)); continue; }
      let j; try { j = JSON.parse(body); } catch (e) { errs.push("nasdaq " + ex + " JSON değil (" + body.length + " bayt)"); continue; }
      const rr = j && j.data && (j.data.table ? j.data.table.rows : j.data.rows);
      if (Array.isArray(rr) && rr.length) { rows = rr; break; }
      errs.push("nasdaq " + ex + (q ? " (süzgeçli)" : "") + ": satır yok");
    }
    if (!rows) continue;
    let seen = 0;
    rows.forEach(r => {
      const raw = String(idxPick(r, ["symbol", "Symbol"]) || "").trim().toUpperCase();
      const s = idxNormSym(raw);
      if (!s || members.has(s) || members.has(s.split(".")[0])) return;
      const mc = idxNdqNum(idxPick(r, ["marketCap", "marketcap", "MarketCap"]));
      if (mc === null || mc < minCap) return;
      seen++;
      uni.set(s, { sym: s, name: String(idxPick(r, ["name", "companyName"]) || "").trim(), mcap: mc, exch: ex, sector: "" });
    });
    if (diag) diag.push({ path: "nasdaq " + ex + " süzgeç sonrası", status: 200, bytes: seen, head: "eşiği geçen ve S&P 500 dışı" });
  }
  return { uni: uni, errs: errs };
}

async function idxProbe(env) {
  let keys = {}; try { keys = (await fetchState(env)).keys || {}; } catch (e) {}
  const key = apiKeyOf(env, keys, "fmp");
  const out = [];
  const jrows = b => { const j = JSON.parse(b); const a = Array.isArray(j) ? j : (j && (j.data || j.rows)) || []; return Array.isArray(a) ? (a.length + " kayıt · ilk: " + JSON.stringify(a[0] || null).slice(0, 160)) : ("dizi değil: " + JSON.stringify(j).slice(0, 160)); };

  if (key) {
    const F = "https://financialmodelingprep.com/stable/";
    out.push(await idxTry("fmp:profile", F + "profile?symbol=AAPL&apikey=" + key, null, jrows));
    out.push(await idxTry("fmp:income-statement", F + "income-statement?symbol=AAPL&period=quarter&limit=4&apikey=" + key, null, jrows));
    out.push(await idxTry("fmp:company-screener", F + "company-screener?marketCapMoreThan=22700000000&country=US&exchange=NYSE&isEtf=false&isFund=false&limit=5&apikey=" + key, null, jrows));
    out.push(await idxTry("fmp:sp500-constituent", F + "sp500-constituent?apikey=" + key, null, jrows));
  } else out.push({ name: "fmp", error: "anahtar yok" });

  /* Nasdaq'ın kendi genel tarayıcı JSON'u — anahtarsız, tarayıcı başlığı ister */
  out.push(await idxTry("nasdaq:screener",
    "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=5&offset=0&exchange=NYSE",
    { headers: Object.assign({ "Accept": "application/json" }, ua()) },
    b => { const j = JSON.parse(b); const rows = j && j.data && (j.data.table ? j.data.table.rows : j.data.rows); return Array.isArray(rows) ? (rows.length + " kayıt · ilk: " + JSON.stringify(rows[0]).slice(0, 200)) : ("rows yok: " + JSON.stringify(j).slice(0, 160)); }));

  /* Wikipedia — kullanılan üye listesi kaynağı (yedeği ham wikitext) */
  out.push(await idxTry("wikipedia:html", IDX_WIKI_HTML, { headers: ua() },
    b => idxSymsFromWikiHtml(b).length + " sembol · ilk: " + idxSymsFromWikiHtml(b).slice(0, 8).join(",")));
  out.push(await idxTry("wikipedia:raw", IDX_WIKI_RAW, { headers: ua() },
    b => idxSymsFromWikiRaw(b).length + " sembol · ilk: " + idxSymsFromWikiRaw(b).slice(0, 8).join(",")));

  return { ok: true, ts: Date.now(), note: "kaynak yoklaması — hangi ucun geçtiğini görmek için", probe: out };
}

/* Kapıda elenen ADR / yabancı / enstrüman kararı zamanla değişmez — 30 gün KV'de tutulur,
   sonraki koşumlarda o adaylar için profil isteği harcanmaz. Yaş (12 ay) kararı zamanla
   değiştiği için ÖNBELLEKLENMEZ. */
async function idxDisqLoad(env) {
  try { return JSON.parse((env.PORTFOLIO ? await env.PORTFOLIO.get("idx:disq") : null) || "{}") || {}; } catch (e) { return {}; }
}
async function idxDisqSave(env, map) {
  try { if (env.PORTFOLIO) await env.PORTFOLIO.put("idx:disq", JSON.stringify(map), { expirationTtl: 60 * 60 * 24 * 30 }); } catch (e) {}
}
function idxDisqDurable(why) { return !/halka açılalı|profil alınamadı|bütçe/.test(String(why || "")); }

/* V-12.9 Tanı: /index?facts=SYM → şirketin companyfacts dosyasından KÂR BENZERİ us-gaap
   etiketlerini ve her birinin en yeni dönemini çıkarır. Bloom Energy'nin NetIncomeLoss serisi
   2023'te bitiyor ve denenen beş alternatif de boştu — hangi etikete geçtiğini tahmin etmek
   yerine SEC'e sorduruyoruz. Dosya büyük olabildiği için bu YALNIZ tanıdır, tarama kullanmaz. */
async function idxFactsDiag(env, sym) {
  const s = idxNormSym(sym);
  let cmap = {}; try { cmap = await cikMap(env, [s]) || {}; } catch (e) {}
  const cik = cmap[s];
  if (!cik) return { sym: s, error: "CIK bulunamadı" };
  const u = "https://data.sec.gov/api/xbrl/companyfacts/CIK" + pad10(cik) + ".json";
  let r, body = "";
  try { r = await fetch(u, { headers: secUA(env) }); body = await r.text(); }
  catch (e) { return { sym: s, cik: cik, error: "SEC'e ulaşılamadı: " + errStr(e) }; }
  if (!r.ok) return { sym: s, cik: cik, status: r.status, bytes: body.length, error: "SEC " + r.status };
  let js; try { js = JSON.parse(body); } catch (e) { return { sym: s, cik: cik, bytes: body.length, error: "JSON değil" }; }
  const gaap = (js && js.facts && js.facts["us-gaap"]) || {};
  const hits = [];
  Object.keys(gaap).forEach(tag => {
    if (!/NetIncome|ProfitLoss|IncomeLoss/i.test(tag)) return;
    const un = idxSecUnits(gaap[tag]);
    if (!un.rows.length) return;
    let newest = "", nq = 0;
    un.rows.forEach(x => {
      if (!x || !x.end) return;
      if (x.end > newest) newest = x.end;
      if (x.start && x.form && /^10-[QK]/.test(x.form)) { const d = idxDays(x.start, x.end); if (d >= 80 && d <= 100) nq++; }
    });
    hits.push({ tag: tag, rows: un.rows.length, quarters: nq, newest: newest });
  });
  hits.sort((a, b) => (a.newest < b.newest ? 1 : a.newest > b.newest ? -1 : b.quarters - a.quarters));
  return { sym: s, cik: cik, bytes: body.length, gaapTags: Object.keys(gaap).length, candidates: hits.slice(0, 15) };
}

/* Tanı: /index?members=SYM → sembol üye listesinde var mı, liste nereden geldi */
async function idxMemDiag(env, sym) {
  const s = idxNormSym(sym);
  const mem = await idxMembers(env, null);
  const all = Array.from(mem.syms);
  return {
    sym: s, inList: mem.syms.has(s), rootInList: mem.syms.has(s.split(".")[0]),
    src: mem.src, count: all.length,
    like: all.filter(x => x.indexOf(s.split(".")[0]) === 0).slice(0, 10)
  };
}

/* V-13.1 Tanı: /index?fmp=SYM → Deepdive'ın kullandığı TÜM FMP uçlarını tek tek dener.
   Ücretsiz planda hangi uçların açık olduğunu tahmin etmeden görmek için. */
async function idxFmpDiag(env, sym) {
  const s = idxNormSym(sym);
  let keys = {}; try { keys = (await fetchState(env)).keys || {}; } catch (e) {}
  const key = apiKeyOf(env, keys, "fmp");
  if (!key) return { sym: s, error: "FMP anahtarı yok" };
  const eps = [
    ["profile", { symbol: s }],
    ["income-statement", { symbol: s, period: "quarter", limit: "8" }],
    ["income-statement-annual", { symbol: s, period: "annual", limit: "5" }],
    ["balance-sheet-statement", { symbol: s, period: "quarter", limit: "8" }],
    ["cash-flow-statement", { symbol: s, period: "quarter", limit: "8" }],
    ["ratios-ttm", { symbol: s }],
    ["quote", { symbol: s }],
    ["search-symbol", { query: s, limit: "5" }],
    ["stock-peers", { symbol: s }]
  ];
  const out = [];
  for (const [name, qs] of eps) {
    const path = name === "income-statement-annual" ? "income-statement" : name;
    const q = Object.assign({}, qs, { apikey: key });
    const u = "https://financialmodelingprep.com/stable/" + path + "?" + Object.keys(q).map(k => encodeURIComponent(k) + "=" + encodeURIComponent(q[k])).join("&");
    let r, body = "";
    try { r = await fetch(u, { headers: { Accept: "application/json" } }); body = await r.text(); }
    catch (e) { out.push({ ep: name, error: errStr(e) }); continue; }
    let rows = null; try { const j = JSON.parse(body); rows = Array.isArray(j) ? j.length : (j && typeof j === "object" ? "obje" : null); } catch (e) {}
    out.push({ ep: name, status: r.status, bytes: body.length, rows: rows, head: body.slice(0, 120) });
  }
  return { sym: s, endpoints: out };
}

async function idxScan(env, p) {
  const diag = p.get("diag") === "1" ? [] : null;
  if (p.get("probe") === "1") return await idxProbe(env);
  if (p.get("sec")) return await idxSecDiag(env, p.get("sec"));
  if (p.get("members")) return await idxMemDiag(env, p.get("members"));
  if (p.get("facts")) return await idxFactsDiag(env, p.get("facts"));
  if (p.get("fmp")) return await idxFmpDiag(env, p.get("fmp"));
  if (p.get("cached") === "1") {
    let raw = null; try { raw = env.PORTFOLIO ? await env.PORTFOLIO.get("scan:index") : null; } catch (e) {}
    if (!raw) return { ok: false, error: "önbellekte kayıt yok — önce ?scan=1 çalıştır" };
    try { return JSON.parse(raw); } catch (e) { return { ok: false, error: "önbellek bozuk: " + errStr(e) }; }
  }
  let keys = {}; try { keys = (await fetchState(env)).keys || {}; } catch (e) { keys = {}; }
  const key = apiKeyOf(env, keys, "fmp");
  if (!key) return { ok: false, error: "FMP anahtarı yok (Ayarlar → API → FMP, ya da worker secret FMP_KEY)" };

  const minCap = Math.max(1e9, idxNum(p.get("min")) || IDX_MINCAP);
  const top = Math.min(20, Math.max(1, parseInt(p.get("top") || "15", 10) || 15));
  const budget = subBudget(48);

  let mem;
  try { mem = await idxMembers(env, diag); }
  catch (e) { return { ok: false, error: errStr(e), diag: diag }; }
  const members = mem.syms;

  const un = await idxUniverse(minCap, members, diag);
  const uni = un.uni, uerr = un.errs;
  if (!uni.size) return { ok: false, error: "Eşiği geçen ve S&P 500 dışında olan şirket bulunamadı" + (uerr.length ? " · " + uerr.join(" · ") : ""), diag: diag, members: members.size };

  /* Kademe 0 — ad/sembolden ve önceki koşumların kalıcı kararlarından ücretsiz eleme */
  const disq = await idxDisqLoad(env);
  const pool = []; const rejected = []; let disqHit = 0;
  Array.from(uni.values()).sort((a, b) => b.mcap - a.mcap).forEach(c => {
    const why = idxPreGate(c) || (disq[c.sym] ? disq[c.sym] + " (önceki koşum)" : null);
    if (why) { if (/önceki koşum/.test(why)) disqHit++; if (rejected.length < 60) rejected.push({ sym: c.sym, why: why }); return; }
    pool.push(c);
  });

  /* Kademe 1 — profil (aday başına 1 istek): kesin kapılar burada uygulanır */
  const probeN = Math.min(30, Math.max(top, parseInt(p.get("probeN") || "24", 10) || 24));
  const passed = []; let disqDirty = false;
  for (const c of pool.slice(0, probeN)) {
    let prof = null, perr = null;
    try { const j = await idxFmp(key, "profile", { symbol: c.sym }, budget, diag); prof = Array.isArray(j) ? j[0] : j; }
    catch (e) { perr = "profil: " + errStr(e); }
    const why = idxGate(c, prof, minCap);
    if (why) {
      rejected.push({ sym: c.sym, why: why + (perr ? " · " + perr : "") });
      if (idxDisqDurable(why)) { disq[c.sym] = why; disqDirty = true; }
      continue;
    }
    passed.push({ c: c, prof: prof, err: perr ? [perr] : [] });
    if (passed.length >= top) break;
  }

  /* Kademe 2 — kârlılık: önce SEC XBRL (resmî, kotasız), gelmezse FMP yedeği */
  let cmap = {};
  if (passed.length) {
    if (budget.take()) { try { cmap = await cikMap(env, passed.map(x => x.c.sym)) || {}; } catch (e) {} }
  }
  const rows = [];
  for (const it of passed) {
    let ea = { q: [], src: null, note: null };
    try {
      const sec = await idxSecEarn(env, it.c.sym, cmap[it.c.sym], budget, diag);
      if (sec.q.length) ea = { q: sec.q, src: "SEC XBRL/" + sec.tag, note: null };
      else ea.note = sec.note;
    } catch (e) { ea.note = "SEC: " + errStr(e); }

    if (ea.q.length < 4) {
      try {
        const j = await idxFmp(key, "income-statement", { symbol: it.c.sym, period: "quarter", limit: "4" }, budget, diag);
        const fq = (Array.isArray(j) ? j : []).map(r => ({ end: String(r && r.date || ""), val: idxNum(idxPick(r, ["netIncome", "netincome", "net_income"])), derived: false })).filter(x => x.val !== null);
        /* FMP yedeğinde de güncellik aranır: tarihi okunamayan veya bayat seriyle hüküm verilmez */
        const fresh = fq.length >= 4 && /^\d{4}-\d{2}-\d{2}/.test(fq[0].end) &&
          Math.round((Date.now() - Date.parse(fq[0].end.slice(0, 10) + "T00:00:00Z")) / 864e5) <= IDX_SEC_MAXAGE;
        if (fresh && fq.length > ea.q.length) ea = { q: fq, src: "FMP", note: ea.note };
        else if (fq.length >= 4 && !fresh) ea.note = (ea.note ? ea.note + " · " : "") + "FMP yedeği de güncel değil (" + (fq[0].end || "tarihsiz") + ")";
      } catch (e) { it.err.push("FMP yedeği: " + errStr(e)); }
    }
    rows.push(idxScore(it.c, it.prof, ea, minCap, it.err));
  }
  /* Kârlılığı DOĞRULANANLAR önce: ölçülemeyen kriter paydadan düştüğü için eksik veriyle
     yüksek skor çıkabiliyor; eksik veri sıralamada öne geçmesin. */
  rows.sort((a, b) => (b.earnOk - a.earnOk) || ((b.score === null ? -1 : b.score) - (a.score === null ? -1 : a.score)) || (b.mcap - a.mcap));

  if (disqDirty) await idxDisqSave(env, disq);

  const res = {
    ok: true, ts: Date.now(), asOf: new Date().toISOString().slice(0, 10),
    minCap: minCap, members: members.size, memberSrc: mem.src, universe: uni.size,
    pool: pool.length, disqCached: disqHit, probed: probeN, shown: rows.length, rejected: rejected,
    rule: "S&P DJI ekleme şartları · eşik 22,7 mia $ (1 Tem 2025) · ABD merkezli + 12 ay şartı ELEME kapısıdır",
    warn: uerr.length ? uerr : null, rows: rows
  };
  if (diag) res.diag = diag;
  try { if (env.PORTFOLIO) await env.PORTFOLIO.put("scan:index", JSON.stringify(res), { expirationTtl: 60 * 60 * 24 }); } catch (e) {}
  return res;
}

/* ============================================================
   V-12.4 · KÂRLILIK: SEC XBRL (FMP ücretsiz planı yetmiyor)
   FMP ücretsiz planı "income-statement" ucunu çoğu sembolde 402 ("Special Endpoint")
   ile reddediyor → 50 puanlık ana kriter ölçülemiyordu. Kaynak SEC'e taşındı:
     data.sec.gov/api/xbrl/companyconcept/CIK##########/us-gaap/NetIncomeLoss.json
   Resmî, ücretsiz, anahtarsız (SEC_UA zaten kurulu). FMP yedekte kalır.
   Q4 çoğu şirkette ayrı raporlanmaz — yıllıktan üç çeyrek düşülerek TÜRETİLİR ve
   "~" ile işaretlenir (§0F). Türetilemezse kaç çeyrek bulunduğu yazılır, sayı uydurulmaz.
============================================================ */
/* V-12.7: Bloom Energy'nin NetIncomeLoss serisi 2022'de kesiliyor, ilk üç etiket de boştu.
   Liste genişletildi; güncel seri bulununca döngü kırıldığı için tipik durumda yine tek istek. */
const IDX_SEC_TAGS = ["NetIncomeLoss", "ProfitLoss", "NetIncomeLossAvailableToCommonStockholdersBasic",
  "NetIncomeLossAvailableToCommonStockholdersDiluted",
  "IncomeLossFromContinuingOperationsIncludingPortionAttributableToNoncontrollingInterest",
  "IncomeLossFromContinuingOperations"];
function idxDays(a, b) { return Math.round((Date.parse(b) - Date.parse(a)) / 864e5); }

/* SEC companyconcept yanıtından çeyreklik net kâr serisi çıkar */
/* V-12.8: units altındaki anahtar her zaman "USD" değil (BE'de dizi olmayan bir yapı geldi,
   tanı ucu "units.slice is not a function" ile çöktü). Dizi olan ilk birim seçilir. */
function idxSecUnits(js) {
  const u = (js && js.units) || {};
  if (Array.isArray(u.USD)) return { rows: u.USD, key: "USD", keys: Object.keys(u) };
  const k = Object.keys(u).find(x => Array.isArray(u[x]));
  return { rows: k ? u[k] : [], key: k || null, keys: Object.keys(u) };
}
function idxSecQuarters(js) {
  const arr = idxSecUnits(js).rows;
  const qs = new Map(), ys = new Map();   /* end → kayıt */
  (Array.isArray(arr) ? arr : []).forEach(r => {
    if (!r || !r.start || !r.end || r.val == null) return;
    if (r.form && !/^10-[QK]/.test(r.form)) return;
    const d = idxDays(r.start, r.end);
    const rec = { end: r.end, start: r.start, val: +r.val, form: r.form || "", filed: r.filed || "" };
    if (d >= 80 && d <= 100) { const p = qs.get(r.end); if (!p || (rec.filed > p.filed)) qs.set(r.end, rec); }
    else if (d >= 350 && d <= 380) { const p = ys.get(r.end); if (!p || (rec.filed > p.filed)) ys.set(r.end, rec); }
  });
  const out = Array.from(qs.values()).map(x => ({ end: x.end, val: x.val, derived: false }));

  /* Q4 türetme: yıllık − aynı yılın üç çeyreği (hepsi varsa) */
  Array.from(ys.values()).forEach(y => {
    if (qs.has(y.end)) return;
    const three = Array.from(qs.values()).filter(q => q.end > y.start && q.end < y.end);
    if (three.length !== 3) return;
    const sum = three.reduce((a, b) => a + b.val, 0);
    out.push({ end: y.end, val: y.val - sum, derived: true });
  });
  return out.sort((a, b) => (a.end < b.end ? 1 : a.end > b.end ? -1 : 0));
}

/* V-12.5: Şirketler zaman içinde etiket değiştirebiliyor — SCCO'nun NetIncomeLoss serisi
   2012'de bitiyor, güncel kâr başka etikette. Bu yüzden ilk dolu etikette DURULMAZ:
   seri bayatsa (son çeyrek 400 günden eski) diğer etiketler de denenir ve EN GÜNCEL seri seçilir.
   Bayat seriyle "kârlı" hükmü verilmez — ölçülemedi sayılır (§0F). */
const IDX_SEC_MAXAGE = 400;
async function idxSecEarn(env, sym, cik, budget, diag) {
  if (!cik) return { q: [], note: "CIK bulunamadı (ABD dışı veya yeni kayıt olabilir)" };
  let best = null, note = null;
  for (const tag of IDX_SEC_TAGS) {
    if (budget && !budget.take()) return { q: [], note: "subrequest bütçesi doldu" };
    const u = "https://data.sec.gov/api/xbrl/companyconcept/CIK" + pad10(cik) + "/us-gaap/" + tag + ".json";
    let r, body = "";
    try { r = await fetch(u, { headers: secUA(env) }); body = await r.text(); }
    catch (e) { if (diag) diag.push({ path: "sec " + sym + " " + tag, status: null, bytes: 0, head: errStr(e) }); continue; }
    if (diag) diag.push({ path: "sec " + sym + " " + tag, status: r.status, bytes: body.length, head: body.slice(0, 120) });
    if (r.status === 404) continue;                       /* şirket bu etiketi kullanmıyor */
    if (!r.ok) { note = "SEC " + r.status + " (" + tag + ")"; continue; }
    let js; try { js = JSON.parse(body); } catch (e) { note = "SEC yanıtı JSON değil (" + body.length + " bayt)"; continue; }
    const q = idxSecQuarters(js);
    if (!q.length) continue;
    const age = Math.round((Date.now() - Date.parse(q[0].end + "T00:00:00Z")) / 864e5);
    if (!best || q[0].end > best.q[0].end) best = { q: q, tag: tag, age: age };
    if (best.age <= IDX_SEC_MAXAGE) break;                /* güncel seri bulundu, aramayı sürdürme */
  }
  /* V-13.0: companyconcept ile companyfacts tutarsız olabiliyor — Bloom Energy'de güncel kâr
     ProfitLoss etiketinde (66 çeyrek, 2026-03-31) ama companyconcept aynı etiket için boş
     dönüyor. Küçük uç yetmezse büyük dosyaya düşülür; bu YALNIZ gerektiğinde olur. */
  if (!best || best.age > IDX_SEC_MAXAGE) {
    const f = await idxSecEarnFacts(env, sym, cik, budget, diag);
    if (f.q.length) return f;
    if (best) return { q: [], note: "SEC serisi bayat — son çeyrek " + best.q[0].end + " (" + best.age + " gün önce, " + best.tag + ") · companyfacts de güncel seri vermedi" };
    return { q: [], note: note || "SEC'te çeyreklik net kâr kaydı bulunamadı" };
  }
  return { q: best.q, tag: best.tag, note: null };
}

async function idxSecEarnFacts(env, sym, cik, budget, diag) {
  if (budget && !budget.take()) return { q: [], note: "subrequest bütçesi doldu" };
  const u = "https://data.sec.gov/api/xbrl/companyfacts/CIK" + pad10(cik) + ".json";
  let r, body = "";
  try { r = await fetch(u, { headers: secUA(env) }); body = await r.text(); }
  catch (e) { if (diag) diag.push({ path: "secfacts " + sym, status: null, bytes: 0, head: errStr(e) }); return { q: [], note: "companyfacts alınamadı: " + errStr(e) }; }
  if (diag) diag.push({ path: "secfacts " + sym, status: r.status, bytes: body.length, head: body.slice(0, 80) });
  if (!r.ok) return { q: [], note: "companyfacts SEC " + r.status };
  let js; try { js = JSON.parse(body); } catch (e) { return { q: [], note: "companyfacts JSON değil (" + body.length + " bayt)" }; }
  const gaap = (js && js.facts && js.facts["us-gaap"]) || {};
  for (const tag of IDX_SEC_TAGS) {
    if (!gaap[tag]) continue;
    const q = idxSecQuarters(gaap[tag]);
    if (!q.length) continue;
    const age = Math.round((Date.now() - Date.parse(q[0].end + "T00:00:00Z")) / 864e5);
    if (age <= IDX_SEC_MAXAGE) return { q: q, tag: "facts/" + tag, note: null };
  }
  return { q: [], note: "companyfacts'te güncel çeyreklik kâr bulunamadı" };
}

/* Tanı: /index?sec=SYM → o sembol için ham çeyrek listesi ve hangi etiketin kullanıldığı */
async function idxSecDiag(env, sym) {
  const s = idxNormSym(sym);
  let cmap = {}; try { cmap = await cikMap(env, [s]) || {}; } catch (e) {}
  const diag = [];
  const out = { sym: s, cik: cmap[s] || null, tags: [] };
  if (!out.cik) { out.error = "CIK bulunamadı" + (cikMap._note ? " · " + cikMap._note : ""); return out; }
  for (const tag of IDX_SEC_TAGS) {
    const u = "https://data.sec.gov/api/xbrl/companyconcept/CIK" + pad10(out.cik) + "/us-gaap/" + tag + ".json";
    let r, body = "";
    try { r = await fetch(u, { headers: secUA(env) }); body = await r.text(); }
    catch (e) { out.tags.push({ tag: tag, error: errStr(e) }); continue; }
    if (!r.ok) { out.tags.push({ tag: tag, status: r.status, bytes: body.length }); continue; }
    let js = null; try { js = JSON.parse(body); } catch (e) { out.tags.push({ tag: tag, status: r.status, bytes: body.length, error: "JSON değil" }); continue; }
    const q = idxSecQuarters(js);
    const un = idxSecUnits(js);
    out.tags.push({
      tag: tag, status: r.status, bytes: body.length,
      unitKeys: un.keys.join(",") || "(yok)", unitUsed: un.key,
      rawRows: un.rows.length, quarters: q.length,
      last8: q.slice(0, 8).map(x => x.end + (x.derived ? "~" : "") + ":" + x.val),
      lastRaw: un.rows.slice(-3).map(x => (x && x.start || "?") + ".." + (x && x.end || "?") + " " + (x && x.form || "?")),
      forms: Array.from(new Set(un.rows.slice(-12).map(x => (x && x.form) || "?"))).join(",")
    });
  }
  const r2 = await idxSecEarn(env, s, out.cik, null, diag);
  out.chosen = { tag: r2.tag || null, n: r2.q.length, upTo: r2.q[0] ? r2.q[0].end : null, note: r2.note || null };
  return out;
}

/* ============================================================
   V-12.4 · /ai — Anthropic isteklerinin worker üzerinden geçmesi
   Tarayıcıdan doğrudan api.anthropic.com'a giden istek "NetworkError" veriyordu
   (CORS / uzantı / ağ engeli — HTTP kodu bile dönmüyordu). Worker aynı gövdeyi
   olduğu gibi iletir, yanıtı CORS başlıklarıyla geri verir. Anahtar worker'da kalır.
   GET ?diag=1 → küçük bir yoklama isteği gönderir, ham durumu gösterir.
============================================================ */
async function aiProxy(env, request, url) {
  let keys = {}; try { keys = (await fetchState(env)).keys || {}; } catch (e) {}
  const key = apiKeyOf(env, keys, "apiKey");
  if (!key) return txt(JSON.stringify({ error: "Anthropic anahtarı yok (Ayarlar → API, ya da worker secret ANTHROPIC_KEY)" }), 200, "application/json; charset=utf-8");

  let payload;
  if (request.method === "POST") {
    try { payload = await request.text(); } catch (e) { return txt(JSON.stringify({ error: "gövde okunamadı: " + errStr(e) }), 200, "application/json; charset=utf-8"); }
    if (!payload || !payload.trim()) return txt(JSON.stringify({ error: "boş gövde" }), 200, "application/json; charset=utf-8");
  } else if (url.searchParams.get("diag") === "1") {
    payload = JSON.stringify({ model: url.searchParams.get("model") || "claude-sonnet-4-5", max_tokens: 16, messages: [{ role: "user", content: "ping" }] });
  } else return txt(JSON.stringify({ error: "POST bekleniyor (yoklama için ?diag=1)" }), 200, "application/json; charset=utf-8");

  let r, body = "";
  try {
    r = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-api-key": key, "anthropic-version": "2023-06-01" },
      body: payload
    });
    body = await r.text();
  } catch (e) { return txt(JSON.stringify({ error: "Anthropic'e ulaşılamadı: " + errStr(e) }), 200, "application/json; charset=utf-8"); }

  if (url.searchParams.get("diag") === "1") {
    return txt(JSON.stringify({ ok: r.ok, status: r.status, bytes: body.length, head: body.slice(0, 300) }), 200, "application/json; charset=utf-8");
  }
  return txt(body, r.status, "application/json; charset=utf-8");
}

/* ============================================================
   V6.2 · NATIVE WEB PUSH (VAPID + RFC 8291 aes128gcm)
   Ek secret'lar (port-notify → Settings → Secrets):
     VAPID_PUBLIC = index.html'deki public anahtarın aynısı
     VAPID_JWK    = private JWK (tek satır JSON, d dahil)
   Ek cron (Cron Triggers): her 15 dk (ifade: slash-15 boşluk yıldızlar)
   ============================================================ */
function b64uToBytes(s){ s=String(s||"").replace(/-/g,"+").replace(/_/g,"/"); s+="=".repeat((4-s.length%4)%4); const bin=atob(s); const u=new Uint8Array(bin.length); for(let i=0;i<bin.length;i++)u[i]=bin.charCodeAt(i); return u; }
function bytesToB64u(u){ const b=new Uint8Array(u); let bin=""; for(let i=0;i<b.length;i++)bin+=String.fromCharCode(b[i]); return btoa(bin).replace(/\+/g,"-").replace(/\//g,"_").replace(/=+$/,""); }
function concatU8(){ let n=0; for(const a of arguments)n+=a.length; const o=new Uint8Array(n); let off=0; for(const a of arguments){o.set(a,off);off+=a.length;} return o; }
async function hkdf(salt, ikm, info, len){
  const k=await crypto.subtle.importKey("raw", ikm, "HKDF", false, ["deriveBits"]);
  const bits=await crypto.subtle.deriveBits({name:"HKDF", hash:"SHA-256", salt, info}, k, len*8);
  return new Uint8Array(bits);
}
async function vapidJWT(env, aud){
  const jwk=JSON.parse(env.VAPID_JWK);
  const key=await crypto.subtle.importKey("jwk", jwk, {name:"ECDSA", namedCurve:"P-256"}, false, ["sign"]);
  const enc=new TextEncoder();
  const header=bytesToB64u(enc.encode(JSON.stringify({typ:"JWT", alg:"ES256"})));
  const payload=bytesToB64u(enc.encode(JSON.stringify({aud, exp:Math.floor(Date.now()/1000)+12*3600, sub:"mailto:sakir.unveren@gmail.com"})));
  const sig=await crypto.subtle.sign({name:"ECDSA", hash:"SHA-256"}, key, enc.encode(header+"."+payload));
  return header+"."+payload+"."+bytesToB64u(new Uint8Array(sig));
}
async function encryptPayload(sub, plaintext){
  const uaPub=b64uToBytes(sub.keys.p256dh);      // 65
  const authSecret=b64uToBytes(sub.keys.auth);   // 16
  const asKeys=await crypto.subtle.generateKey({name:"ECDH", namedCurve:"P-256"}, true, ["deriveBits"]);
  const asPub=new Uint8Array(await crypto.subtle.exportKey("raw", asKeys.publicKey)); // 65
  const uaKey=await crypto.subtle.importKey("raw", uaPub, {name:"ECDH", namedCurve:"P-256"}, false, []);
  const ecdh=new Uint8Array(await crypto.subtle.deriveBits({name:"ECDH", public:uaKey}, asKeys.privateKey, 256)); // 32
  const enc=new TextEncoder();
  const keyInfo=concatU8(enc.encode("WebPush: info\0"), uaPub, asPub);
  const ikm=await hkdf(authSecret, ecdh, keyInfo, 32);
  const salt=crypto.getRandomValues(new Uint8Array(16));
  const cek=await hkdf(salt, ikm, enc.encode("Content-Encoding: aes128gcm\0"), 16);
  const nonce=await hkdf(salt, ikm, enc.encode("Content-Encoding: nonce\0"), 12);
  const record=concatU8(new Uint8Array(plaintext), new Uint8Array([2])); // 0x02 = son kayıt
  const aesKey=await crypto.subtle.importKey("raw", cek, {name:"AES-GCM"}, false, ["encrypt"]);
  const ct=new Uint8Array(await crypto.subtle.encrypt({name:"AES-GCM", iv:nonce, tagLength:128}, aesKey, record));
  const rs=new Uint8Array([0,0,16,0]);           // record size 4096
  const idlen=new Uint8Array([asPub.length]);    // 65
  return concatU8(salt, rs, idlen, asPub, ct);
}
async function sendPush(env, sub, payloadObj){
  if(!env.VAPID_JWK||!env.VAPID_PUBLIC) throw new Error("VAPID_JWK/VAPID_PUBLIC secret'i eksik");
  const aud=new URL(sub.endpoint).origin;
  const jwt=await vapidJWT(env, aud);
  const body=await encryptPayload(sub, new TextEncoder().encode(JSON.stringify(payloadObj)));
  const r=await fetch(sub.endpoint, { method:"POST", headers:{
    "Content-Encoding":"aes128gcm", "Content-Type":"application/octet-stream",
    "TTL":"86400", "Authorization":"vapid t="+jwt+", k="+env.VAPID_PUBLIC
  }, body });
  return r.status;
}
async function getSubs(env){ if(!env.PORTFOLIO)return []; try{ const raw=await env.PORTFOLIO.get("push:subs"); return raw?JSON.parse(raw):[]; }catch(e){return [];} }
async function pushSubStore(env, sub){ if(!sub||!sub.endpoint)throw new Error("geçersiz abonelik"); const subs=await getSubs(env); const i=subs.findIndex(s=>s.endpoint===sub.endpoint); if(i>=0)subs[i]=sub; else subs.push(sub); await env.PORTFOLIO.put("push:subs", JSON.stringify(subs)); }
async function pushSubRemove(env, endpoint){ const subs=await getSubs(env); await env.PORTFOLIO.put("push:subs", JSON.stringify(subs.filter(s=>s.endpoint!==endpoint))); }
async function sendPushAll(env, payloadObj){
  const subs=await getSubs(env); let sent=0; const keep=[];
  for(const s of subs){
    try{ const st=await sendPush(env, s, payloadObj); if(st>=200&&st<300){sent++;keep.push(s);} else if(st===404||st===410){/* süresi dolmuş → düş */} else keep.push(s); }
    catch(e){ keep.push(s); }
  }
  if(keep.length!==subs.length) await env.PORTFOLIO.put("push:subs", JSON.stringify(keep));
  return sent;
}
function trDateStr(){ const d=new Date(new Date().toLocaleString("en-US",{timeZone:"Europe/Istanbul"})); return d.getFullYear()+"-"+String(d.getMonth()+1).padStart(2,"0")+"-"+String(d.getDate()).padStart(2,"0"); }
/* Her 15 dk: canlı fiyat → pozisyon gün içi ±%10 → push (gün içinde tekrar etmez). */
async function runPushChecks(env){
  const state=await fetchState(env); const keys=(state&&state.keys)||{};
  try{ await fetchLivePrices(env, keys); }catch(e){}
  const cfg=notifCfgOf(keys);
  const today=trDateStr();
  let alerted={}, sentToday=0;
  try{ const raw=await env.PORTFOLIO.get("push:alerted"); if(raw){ const o=JSON.parse(raw); if(o.date===today){ alerted=o.map||{}; sentToday=+o.count||0; } } }catch(e){}
  const cap=Math.max(1,num(cfg.nfCap)||10);
  const quiet=inQuietHours(cfg);
  let sent=0, lines=[], held=0;
  const push=async(o,important)=>{
    if(sentToday+sent>=cap){ held++; return false; }                    // V-9.9: günlük üst sınır
    if(quiet&&!important){ held++; return false; }                       // V-9.9: sessiz saatler (acil olanlar geçer)
    try{ await sendPushAll(env,o); sent++; return true; }catch(e){ return false; }
  };
  const isClosed=p=>((+p.qty||0)<=1e-6)||((+p.price||0)>0&&Math.abs(+p.qty)*(+p.price)<0.5);
  const grp=p=>p.group||(p.cg?"Crypto":"Stock");
  const positions=(keys.positions||[]).filter(p=>p&&!p.manual&&!isClosed(p));

  /* 1) Gün içi sert hareket — hisse/kripto ve emtia için ayrı eşik */
  for(const p of positions){
    if(p.day==null)continue;
    const d=+p.day; if(!isFinite(d))continue;
    const lim=grp(p)==="Commodity"?num(cfg.nfMoveComm):num(cfg.nfMove);
    if(!lim||Math.abs(d)<lim)continue;
    const kk="mv|"+p.t+"|"+(d>0?"up":"dn");
    if(alerted[kk])continue;
    if(await push({title:`${d>0?"📈":"📉"} ${p.t} ${sg(d,1)}%`,body:`${p.t} bugün ${sg(d,1)}% · fiyat ${p.price}`,tag:"move-"+p.t,url:"/"}))
      { lines.push(p.t+" "+sg(d,1)+"%"); alerted[kk]=1; }
  }

  /* 2) Bugün / yarın bilanço açıklayacak semboller  (V-10.6: 5 günlük kümülatif uyarı kaldırıldı) */
  for(const [dt,lbl] of [[trShift(0),"bugün"],[trShift(1),"yarın"]]){
    const list=earningsOn(keys,dt);
    if(!list.length)continue;
    const kk="earn|"+dt;
    if(alerted[kk])continue;
    if(await push({title:`📅 Bilanço ${lbl}: ${list.length} sembol`,body:list.join(", "),tag:"earn-"+dt,url:"/"},lbl==="bugün"))
      { lines.push("earnings "+lbl+" "+list.length); alerted[kk]=1; }
  }

  /* 3) Bugün ödemesi olanlar — taksitler + abonelikler (ödendi işaretliler atlanır) */
  try{
    const pr=await paymentReminders(env, keys, today, alerted, push);
    if(pr.lines.length) lines=lines.concat(pr.lines);
  }catch(e){}

  /* 4) V-11.2 (#3): pozisyonlar için anlık önemli haber */
  try{
    const nl=await newsPushCheck(env, keys, cfg, push);
    if(nl.length) lines=lines.concat(nl);
  }catch(e){}

  try{ await env.PORTFOLIO.put("push:alerted", JSON.stringify({date:today, map:alerted, count:sentToday+sent})); }catch(e){}
  return `gönderilen: ${sent} (bugün toplam ${sentToday+sent}/${cap})${held?` · ${held} bekletildi${quiet?" (sessiz saat)":""}`:""}`+(lines.length?("\n"+lines.join(" · ")):"");
}
/* finPay: bugün ya da 3 gün sonra vadesi gelen (amt>0) taksitleri tarihe göre gruplayıp push atar. */
async function paymentReminders(env, keys, today, alerted, push){
  const t0=new Date(today+"T00:00:00Z").getTime();
  const dayDiff=ds=>{ const d=new Date((ds+"").slice(0,10)+"T00:00:00Z").getTime(); return isNaN(d)?null:Math.round((d-t0)/86400000); };
  const buckets={};
  /* taksitler / krediler — V-9.9: "ödendi" işaretliler atlanır */
  for(const r of (Array.isArray(keys.finPay)?keys.finPay:[])){
    if(!r||!r.date||r.paid)continue;
    const amt=+r.amt||0; if(amt<=0)continue;
    const diff=dayDiff(r.date); if(diff!==0&&diff!==3)continue;
    const bk=r.date+"|"+diff;
    (buckets[bk]=buckets[bk]||{date:r.date,diff,sum:0,items:[]});
    buckets[bk].sum+=amt; buckets[bk].items.push(((r.bank||"")+" "+(r.product||"")).trim());
  }
  /* abonelikler / hobi / faturalar — V-9.9: finSub de dahil */
  for(const it of (Array.isArray(keys.finSub)?keys.finSub:[])){
    if(!it||it.paid)continue;
    const ds=it.next||""; if(!ds)continue;
    const diff=dayDiff(ds); if(diff!==0&&diff!==3)continue;
    const amt=+it.tl||0;
    const bk=ds+"|"+diff;
    (buckets[bk]=buckets[bk]||{date:ds,diff,sum:0,items:[]});
    buckets[bk].sum+=amt; buckets[bk].items.push(it.name||it.cat||"abonelik");
  }
  let sent=0, lines=[];
  for(const bk in buckets){
    if(alerted["pay|"+bk])continue;
    const b=buckets[bk];
    const when=b.diff===0?"bugün":"3 gün sonra";
    const tl=Math.round(b.sum).toLocaleString("tr-TR")+"₺";
    const names=[...new Set(b.items)].slice(0,4).join(", ");
    const payload={ title:`💳 Ödeme ${when} · ${tl}`, body:`${b.date} · ${b.items.length} kalem · ${names}`, tag:"pay-"+bk, important:true, url:"/" };
    const ok = push ? await push(payload,true) : await sendPushAll(env,payload).then(()=>true).catch(()=>false);
    if(ok){ sent++; lines.push("ödeme "+when+" "+tl); alerted["pay|"+bk]=1; }
  }
  return {sent, lines};
}
/* V-10.4: Her sabah 09:00 (hafta sonu dahil) WhatsApp ödeme özeti.
   Bugün + önümüzdeki 3 günü kapsar. "ödendi" işaretliler atlanır.
   Ödenecek bir şey yoksa mesaj GÖNDERİLMEZ (boş bildirimle rahatsız etmez). */
const AY_ADLARI = ["Ocak","Şubat","Mart","Nisan","Mayıs","Haziran","Temmuz","Ağustos","Eylül","Ekim","Kasım","Aralık"];
/* P.8: uygulamadaki finSubNextDue ile BİREBİR eşitlendi (eskiden worker kendi kuralını
   uyguluyordu — "1'i geçtiyse gelecek ayın 1'i" — bu da ayın 27-30'unda "3 gün sonra",
   1'inde tekrar "bugün" diye yığılmaya, ay ortasında ise hiç görünmemeye sebep oluyordu).
   Aylık: uygulama da belirli gün vermiyor, yalnız "Bu ay" diyor → HER ZAMAN bu ayın 1'i,
   ileri SARILMAZ. Sonuç: hatırlatma yalnız ayın 1'inde (diff=0) düşer, ay içinde tekrarlamaz.
   Haftalık: uygulama da belirli gün vermiyor, her zaman "bu hafta" sayıyor (finSubStatus'ta
   period==="week" hep "yaklaşıyor") → worker da HER GÜN "bugün" kabul eder; eskiden "+7 gün"
   hesaplandığı için fark hep 7 çıkıyor, 0-3 günlük pencereye hiç girmiyordu. */
function subNextDue(it, today) {
  if (it.next) return (it.next + "").slice(0, 10);
  const y = +today.slice(0, 4), m = +today.slice(5, 7) - 1;
  const iso = (yy, mm, dd) => `${yy}-${String(mm + 1).padStart(2, "0")}-${String(dd).padStart(2, "0")}`;
  const tp = (it.type || "").trim();
  if (tp === "Aylık") return iso(y, m, 1);
  if (tp === "Haftalık") return today;
  const dn = (it.donem || "").trim();
  const names = dn.includes(" - ") ? dn.split(" - ").map(x => x.trim()) : [dn];
  const idx = names.map(n => AY_ADLARI.indexOf(n)).filter(i => i >= 0);
  if (!idx.length) return null;
  let best = null;
  idx.forEach(mm => { let d = iso(y, mm, 1); if (d < iso(y, m, 1)) d = iso(y + 1, mm, 1); if (!best || d < best) best = d; });
  return best;
}
async function buildPayMsg(env, send) {
  const st = await fetchState(env); const keys = st.keys || {};
  const today = trShift(0);
  const t0 = new Date(today + "T00:00:00Z").getTime();
  const dayDiff = ds => { const d = new Date((ds + "").slice(0, 10) + "T00:00:00Z").getTime(); return isNaN(d) ? null : Math.round((d - t0) / 86400000); };
  const buckets = {};
  const add = (ds, diff, name, amt) => {
    const b = (buckets[ds] = buckets[ds] || { diff, sum: 0, items: [] });
    b.sum += amt; b.items.push({ name, amt });
  };
  for (const r of (Array.isArray(keys.finPay) ? keys.finPay : [])) {
    if (!r || !r.date || r.paid) continue;
    const amt = +r.amt || 0; if (amt <= 0) continue;
    const diff = dayDiff(r.date); if (diff == null || diff < 0 || diff > 3) continue;
    add((r.date + "").slice(0, 10), diff, ((r.bank || "") + " " + (r.product || "")).trim() || "taksit", amt);
  }
  for (const it of (Array.isArray(keys.finSub) ? keys.finSub : [])) {
    if (!it || it.paid || it.hold) continue;        // V-11.5: uygulamada "beklemede" işaretli abonelik hatırlatılmaz
    const due = subNextDue(it, today);                 // uygulamadaki finSubNextDue ile aynı kural
    if (!due) continue;
    const diff = dayDiff(due); if (diff == null || diff < 0 || diff > 3) continue;
    add(due, diff, it.name || it.cat || "abonelik", +it.tl || 0);
  }
  const dates = Object.keys(buckets).sort();
  if (!dates.length) {                                  // teşhis: veri mi yok, vade mi uzak?
    const np = (Array.isArray(keys.finPay) ? keys.finPay : []).filter(r => r && r.date && !r.paid && (r.date + "").slice(0, 10) >= today).map(r => (r.date + "").slice(0, 10)).sort()[0];
    const ns = (Array.isArray(keys.finSub) ? keys.finSub : []).filter(x => x && !x.paid).map(x => subNextDue(x, today)).filter(x => x && x >= today).sort()[0];
    return `ödeme yok · finPay ${(keys.finPay || []).length} kayıt (en yakın ${np || "—"}) · finSub ${(keys.finSub || []).length} kayıt (en yakın ${ns || "—"})`;
  }
  const tl = v => Math.round(v).toLocaleString("tr-TR") + "₺";
  const L = ["Ödeme Hatırlatma 💳", `${today.slice(8)}.${today.slice(5, 7)}.${today.slice(0, 4)} · bugün ve 3 gün`, ""];
  let toplam = 0;
  for (const ds of dates) {
    const b = buckets[ds];
    toplam += b.sum;
    const when = b.diff === 0 ? "bugün" : b.diff === 1 ? "yarın" : b.diff + " gün sonra";
    L.push(`${ds.slice(8)}.${ds.slice(5, 7)} · ${when} · ${tl(b.sum)}`);
    b.items.slice(0, 8).forEach(x => L.push(`• ${x.name}${x.amt > 0 ? " · " + tl(x.amt) : ""}`));
    if (b.items.length > 8) L.push(`• +${b.items.length - 8} kalem daha`);
    L.push("");
  }
  if (dates.length > 1) L.push(`Toplam · ${tl(toplam)}`);
  const text = L.join("\n").replace(/\n+$/, "");
  /* V-11.5: "sürekli aynı mesaj" düzeltmesi.
     Vadesi 1-3 gün sonra olan kalemler her gün aynı metni üretiyordu. Kural:
     - bugün vadesi dolan (diff 0) kalem varsa HER ZAMAN gönder,
     - yoksa ve metin en son gönderilenle birebir aynıysa gönderme. */
  const hasToday = dates.some(d => buckets[d].diff === 0);
  let sig = 0; for (let i = 0; i < text.length; i++) { sig = (sig * 31 + text.charCodeAt(i)) | 0; }
  sig = String(sig);
  if (send) {
    let last = null;
    try { last = await env.PORTFOLIO.get("pay:lastSig"); } catch (e) {}
    if (!hasToday && last === sig) return "aynı içerik daha önce gönderildi — tekrar gönderilmedi";
    await sendWA(env, text);
    try { await env.PORTFOLIO.put("pay:lastSig", sig, { expirationTtl: 2592000 }); } catch (e) {}
  }
  return text;
}
async function weeklyMail(env){
  if(!env.RESEND_KEY) return "RESEND_KEY yok — mail atlandı";
  const state=await fetchState(env);
  const json=JSON.stringify(state,null,2);
  const b64=btoa(unescape(encodeURIComponent(json)));
  const d=new Date().toLocaleDateString("tr-TR",{timeZone:"Europe/Istanbul"});
  // Uygulamanın "İçe aktar (JSON)" düğmesinin beklediği düz biçim
  const k=(state&&state.keys)||{};
  const flat={positions:k.positions||[],watch:k.watch||[],tx:k.tx||{},history:k.history||[],
              settings:k.settings||{},exportedAt:new Date().toISOString()};
  const b64imp=btoa(unescape(encodeURIComponent(JSON.stringify(flat,null,2))));
  const from=env.MAIL_FROM || "PORT <onboarding@resend.dev>";
  const body={
    from, to:["sakir.unveren@gmail.com"],
    subject:"PORT · Haftalık yedek ("+d+")",
    text:"PORT portföy JSON yedeği ektedir. Tarih: "+d+"\nAnahtar sayısı: "+(state&&state.keys?Object.keys(state.keys).length:0)+
         "\n\nİki ek var:\n· port-…json — bulut/sync ham yedeği (dışa aktar)\n· port-import-…json — uygulamada \"İçe aktar (JSON)\" ile geri yüklenir",
    attachments:[
      { filename:"port-"+d.replace(/\./g,"-")+".json", content:b64 },
      { filename:"port-import-"+d.replace(/\./g,"-")+".json", content:b64imp }
    ]
  };
  const r=await fetch("https://api.resend.com/emails",{
    method:"POST",
    headers:{ "Authorization":"Bearer "+env.RESEND_KEY, "Content-Type":"application/json" },
    body:JSON.stringify(body)
  });
  const t=await r.text();
  if(!r.ok) throw new Error("Resend "+r.status+": "+t.slice(0,200));
  return "gönderildi ("+d+")";
}

/* ============================================================
   V-9.9 · ZAMANLAYICI · SNAPSHOT · EARNINGS · MESAJLAR
   ============================================================ */

/* ---------- İstanbul saati (Türkiye sabit UTC+3) ---------- */
function trParts() {
  const d = new Date(Date.now() + 3 * 3600 * 1000);
  return {
    date: d.toISOString().slice(0, 10),
    hh: d.getUTCHours(), mm: d.getUTCMinutes(),
    dow: d.getUTCDay(),                       // 0 Pazar … 6 Cumartesi
    d
  };
}
function trShift(days) {
  const d = new Date(Date.now() + 3 * 3600 * 1000 + days * 86400000);
  return d.toISOString().slice(0, 10);
}
/* Aynı işi gün içinde bir kez çalıştır (cron penceresi iki kez düşse bile) */
/* V-13.2: damga artık iş BAŞARILI bitince yazılır (önceden çalışmadan önce yazılıyordu,
   hata veren iş o gün bir daha denenmiyordu ve hata hiçbir yere kaydedilmiyordu). */
async function ranToday(env, task, date) {
  try { return !!(await env.PORTFOLIO.get("ran:" + task + ":" + date)); } catch (e) { return false; }
}
async function markToday(env, task, date) {
  try { await env.PORTFOLIO.put("ran:" + task + ":" + date, "1", { expirationTtl: 172800 }); } catch (e) {}
}
/* V-13.2: cron izi — "run:<anahtar>" altında son N satır, 7 gün saklanır. */
async function runLogAdd(env, key, line, keep) {
  try {
    const k = "run:" + key;
    const old = (await env.PORTFOLIO.get(k)) || "";
    const arr = old ? old.split("\n") : [];
    arr.push(line);
    await env.PORTFOLIO.put(k, arr.slice(-(keep || 8)).join("\n"), { expirationTtl: 604800 });
  } catch (e) {}
}
/* V-13.2: /runlog — son günlerin cron koşumları ve iş sonuçları (teşhis). */
async function runLogDump(env, days) {
  const d = Math.max(1, Math.min(7, days || 3));
  const tasks = ["push", "snapshot", "earnings", "morning", "weekly", "pay"];
  const out = [];
  for (let i = 0; i < d; i++) {
    const date = trShift(-i);
    out.push("=== " + date + " ===");
    let cron = ""; try { cron = (await env.PORTFOLIO.get("run:cron:" + date)) || ""; } catch (e) {}
    const cl = cron ? cron.split("\n") : [];
    out.push("cron koşumu: " + cl.length + (cl.length ? " · " + cl.join(" ") : " — HİÇ KOŞMADI"));
    for (const tk of tasks) {
      let v = "", stamp = "";
      try { v = (await env.PORTFOLIO.get("run:" + tk + ":" + date)) || ""; } catch (e) {}
      try { stamp = (await env.PORTFOLIO.get("ran:" + tk + ":" + date)) ? "damga✓" : "damga✗"; } catch (e) {}
      if (v) out.push("  " + tk + " · " + stamp + " · " + v.split("\n").join(" | "));
    }
  }
  let sig = ""; try { sig = (await env.PORTFOLIO.get("pay:lastSig")) || "—"; } catch (e) { sig = "okunamadı"; }
  out.push("pay:lastSig · " + String(sig).slice(0, 160));
  return out.join("\n");
}
async function dispatch(env, job, dry) {
  const t = trParts(), log = [];
  const one = (job || "").trim();                       // V-10.0: tek iş zorlama (test için)
  const send = !dry;
  const hhmm = String(t.hh).padStart(2, "0") + ":" + String(t.mm).padStart(2, "0");
  const cron = !one;                                    // gerçek cron koşumu mu (elle çağrı değil)
  if (cron && !dry) await runLogAdd(env, "cron:" + t.date, hhmm, 96);
  if (!one || one === "push") {
    try { await runPushChecks(env); log.push("push"); }
    catch (e) { log.push("push:HATA " + errStr(e)); if (!dry) await runLogAdd(env, "push:" + t.date, hhmm + " HATA " + errStr(e), 8); }
  }
  const jobs = [];
  const want = n => one ? one === n : false;
  if (want("snapshot") || (!one && t.hh === 0)) jobs.push(["snapshot", () => writeSnapshot(env)]);
  if (want("earnings") || (!one && t.hh === 6)) jobs.push(["earnings", () => refreshEarnings(env)]);
  if (want("morning") || (!one && t.hh === 7 && t.mm >= 30 && t.dow >= 1 && t.dow <= 5)) jobs.push(["morning", () => buildMorning(env, send)]);
  if (want("weekly") || (!one && t.hh === 19 && t.dow === 0)) jobs.push(["weekly", () => buildWeeklyMsg(env, send)]);
  if (want("pay") || (!one && t.hh === 9)) jobs.push(["pay", () => buildPayMsg(env, send)]);   // V-13.2: pencere 09:00-09:59 (cron gecikince 15 dk'lık pencere kaçıyordu; günde bir kez garantisini "ran:" damgası veriyor)
  if (one && !jobs.length && one !== "push") log.push("bilinmeyen job: " + one);
  for (const [name, fn] of jobs) {
    if (cron && await ranToday(env, name, t.date)) { log.push(name + ":atlandı"); continue; }
    let res;
    try { await fn(); res = "ok"; if (cron && !dry) await markToday(env, name, t.date); }
    catch (e) { res = "HATA " + errStr(e); }
    log.push(name + ":" + res);
    if (!dry) await runLogAdd(env, name + ":" + t.date, hhmm + (one ? " [elle] " : " ") + res, 8);
  }
  return t.date + " " + String(t.hh).padStart(2, "0") + ":" + String(t.mm).padStart(2, "0") + " · " + log.join(" · ");
}

/* ---------- anahtarlar: önce worker secret, yoksa uygulamadan senkronlanan ---------- */
function apiKeyOf(env, keys, name) {
  const envMap = { finnhub: "FINNHUB", twelvedata: "TWELVEDATA", fred: "FRED_KEY", apiKey: "ANTHROPIC_KEY", cg: "CG_KEY", fmp: "FMP_KEY" };
  const fromEnv = (env[envMap[name]] || "").trim();
  if (fromEnv) return fromEnv;
  const a = (keys && keys.apiKeys) || {};
  return ((a[name] || "") + "").trim();
}
function notifCfgOf(keys) {
  const d = { nfMove: 10, nfMoveComm: 3, nfQuiet: "00-07", nfCap: 10, nfNews: 1, nfNewsMax: 2 };
  const n = (keys && keys.notify) || {};
  const o = {};
  Object.keys(d).forEach(k => { const v = n[k]; o[k] = (v === undefined || v === null || v === "") ? d[k] : v; });
  return o;
}
function inQuietHours(cfg) {
  const m = String(cfg.nfQuiet || "").match(/(\d{1,2})\s*[-–]\s*(\d{1,2})/);
  if (!m) return false;
  const a = +m[1], b = +m[2], h = trParts().hh;
  return a <= b ? (h >= a && h < b) : (h >= a || h < b);
}

/* ---------- durumu KV'ye geri yaz (worker→worker engelini aşmak için doğrudan KV) ---------- */
async function saveState(env, state) {
  state.updatedAt = new Date().toISOString();
  const body = JSON.stringify(state);
  if (env.PORTFOLIO) {
    let key = "state";
    try { if (!(await env.PORTFOLIO.get("state"))) { const l = await env.PORTFOLIO.list(); if (l.keys && l.keys.length) key = l.keys[0].name; } } catch (e) {}
    await env.PORTFOLIO.put(key, body);
    return "KV";
  }
  const base = (env.SYNC_URL || "").trim().replace(/\/+$/, "");
  if (!base) throw new Error("Ne KV ne SYNC_URL var — yazılamadı");
  const r = await fetch(base + "/state", { method: "PUT", headers: { "Content-Type": "application/json", "X-Pw": env.SYNC_PW || "" }, body });
  if (!r.ok) throw new Error("/state PUT " + r.status);
  return "SYNC";
}

/* ---------- GECE ANLIK GÖRÜNTÜSÜ (00:00) ---------- */
function groupOf(p) { return p.group || (p.cg ? "Crypto" : "Stock"); }
function snapshotRecord(keys, date) {
  const positions = (Array.isArray(keys.positions) ? keys.positions : []).filter(p => p && p.t);
  const watch = Array.isArray(keys.watch) ? keys.watch : [];
  const exchCash = keys.exchCash || {};
  const isClosed = p => num(p.qty) <= 1e-6;
  const val = p => p.lev ? num(p.qty) * (num(p.price) - num(p.cost)) : num(p.qty) * num(p.price);
  const cost = p => p.lev ? 0 : num(p.qty) * num(p.cost);
  const open = positions.filter(p => !isClosed(p));
  const cash = Object.values(exchCash).reduce((a, v) => a + num(v), 0);
  const pos = open.reduce((a, p) => a + val(p), 0);
  const cst = open.reduce((a, p) => a + cost(p), 0);
  const groups = {};
  ["Stock", "Commodity", "Crypto", "Trade"].forEach(g => {
    const ps = open.filter(p => groupOf(p) === g);
    const terms = {};                                   // V-10.3: strateji kırılımı da kaydedilir
    ps.forEach(p => { const k = termOf(p); terms[k] = +((terms[k] || 0) + val(p)).toFixed(2); });
    groups[g] = { val: +ps.reduce((a, p) => a + val(p), 0).toFixed(2), cost: +ps.reduce((a, p) => a + cost(p), 0).toFixed(2), terms };
  });
  const snap = (arr, f) => Object.fromEntries(arr.filter(x => x && x.t && x[f] != null).map(x => [x.t, x[f]]));
  return {
    date,
    total: +(cash + pos).toFixed(2), cash: +cash.toFixed(2), pos: +pos.toFixed(2), cost: +cst.toFixed(2),
    groups,
    prices: snap(positions, "price"), wprices: snap(watch, "price"), qty: snap(positions, "qty"),
    meta: Object.fromEntries(positions.map(p => [p.t, { n: p.n, group: p.group, term: p.term, sec: p.sec, cost: p.cost }])),
    bench: Object.assign({}, keys.bench || {}),
    ts: new Date().toISOString(), src: "worker"
  };
}
async function writeSnapshot(env) {
  const st = await fetchState(env);
  const keys = st.keys || {};
  let rep = null;
  try { rep = await fetchLivePrices(env, keys); } catch (e) {}
  const date = trShift(-1);                       // 00:00'da yazılan kayıt DÜNÜN kapanışıdır
  const rec = snapshotRecord(keys, date);
  // yaz–oku yarışını daralt: en güncel durumu tekrar oku, yalnız history'ye dokun
  const fresh = await fetchState(env);
  const fk = fresh.keys || {};
  const hist = Array.isArray(fk.history) ? fk.history.slice() : [];
  const i = hist.findIndex(h => h && h.date === date);
  if (i >= 0) { if (hist[i] && hist[i].src !== "worker" && num(hist[i].total) > 0) return "zaten var (cihaz kaydı korundu) · " + date; hist[i] = rec; }
  else hist.push(rec);
  hist.sort((a, b) => a.date < b.date ? -1 : 1);
  fk.history = hist.slice(-1500);
  /* P.8: eskiden burada fk.positions = keys.positions ile TÜM pozisyon bütünüyle değiştiriliyordu —
     yorum "yalnız history'ye dokun" dese de bunu bozuyordu: fetchLivePrices'ın kullandığı `keys`
     ilk fetchState'ten (fiyat çekimi ~20 sn sürebiliyor), ikinci "fresh" okuma arada başka bir
     cihazın yaptığı düzenlemeyi (miktar/maliyet/not) yakalasa bile ilk okumanın ESKİ kopyasıyla
     eziliyordu. Artık yalnız fiyat/gün%/kaynak alanları sembol bazında `fresh` üstüne bindiriliyor,
     aradaki düzenlemeler korunuyor. */
  if (rep && (rep.crypto + rep.stock) > 0) {
    const bySym = {};
    (keys.positions || []).forEach(p => { if (p && p.t) bySym[p.t] = p; });
    (fk.positions || []).forEach(p => {
      const src = p && p.t && bySym[p.t];
      if (src && src.srcAt) { p.price = src.price; p.day = src.day; p.srcAt = src.srcAt; p.src = src.src; }
    });
    fk.priceUpd = new Date().toISOString();
  }
  const where = await saveState(env, fresh);
  return `${date} · toplam ${usd(rec.total)} · ${hist.length} kayıt · ${where}` + (rep ? ` · fiyat ${rep.crypto + rep.stock}` : "");
}

/* ---------- BİLANÇO (EARNINGS) TARİHLERİ ---------- */
async function refreshEarnings(env) {
  const st = await fetchState(env);
  const keys = st.keys || {};
  const fh = apiKeyOf(env, keys, "finnhub");
  if (!fh) return "Finnhub anahtarı yok";
  const positions = Array.isArray(keys.positions) ? keys.positions : [];
  const watch = Array.isArray(keys.watch) ? keys.watch : [];
  const items = [...positions, ...watch].filter(x => x && x.t && !x.cg && !x.manual && num(x.qty) !== 0 || (x && x.t && !x.cg && !x.manual));
  const syms = [...new Set(items.map(x => x.t.toUpperCase()))];
  const from = trShift(0), to = trShift(120);
  let ok = 0;
  const map = {};
  for (const sym of syms) {
    try {
      const r = await fetch(`https://finnhub.io/api/v1/calendar/earnings?from=${from}&to=${to}&symbol=${encodeURIComponent(sym)}&token=${encodeURIComponent(fh)}`, { headers: ua() });
      if (r.ok) {
        const j = await r.json();
        const arr = (j && j.earningsCalendar) || [];
        const nx = arr.filter(x => x && x.date).sort((a, b) => a.date < b.date ? -1 : 1)[0];
        if (nx) { map[sym] = { earn: nx.date, earnEps: nx.epsEstimate != null ? nx.epsEstimate : null, earnHour: nx.hour || "" }; ok++; }
      }
    } catch (e) {}
    await sleep(1100);
  }
  const fresh = await fetchState(env);
  const fk = fresh.keys || {};
  [...(fk.positions || []), ...(fk.watch || [])].forEach(x => {
    const m = x && x.t && map[x.t.toUpperCase()];
    if (m) { x.earn = m.earn; x.earnEps = m.earnEps; x.earnHour = m.earnHour; x.earnAt = Date.now(); }
  });
  await saveState(env, fresh);
  return `${ok}/${syms.length} sembol güncellendi`;
}

/* ---------- PİYASA REFERANSLARI (uygulamayla aynı: gerçek → yoksa ETF vekili) ---------- */
/* V-10.4: Piyasa referansları için KV önbelleği. Değer alınabildiğinde saklanır;
   sağlayıcı o an cevap vermezse son bilinen değer "*" işaretiyle gösterilir.
   Uydurma yok — gösterilen sayı gerçekten görülmüş bir değerdir, yalnız bayattır.
   Hiç kayıt yoksa "—" kalır. 3 günden eski kayıt kullanılmaz. */
const MKT_CACHE_KEY = "mkt:refs";
async function mktCacheGet(env) {
  try { const raw = await env.PORTFOLIO.get(MKT_CACHE_KEY); return raw ? JSON.parse(raw) : {}; } catch (e) { return {}; }
}
async function mktCachePut(env, cache) {
  try { await env.PORTFOLIO.put(MKT_CACHE_KEY, JSON.stringify(cache)); } catch (e) {}
}
function mktMerge(out, cache) {
  const now = Date.now(), MAXAGE = 3 * 86400000, next = Object.assign({}, cache);
  Object.keys(out).forEach(k => { if (out[k] && isFinite(out[k].v)) next[k] = { v: out[k].v, d: out[k].d, src: out[k].src, at: now }; });
  Object.keys(next).forEach(k => {
    if (out[k] && isFinite(out[k].v)) return;
    const c = next[k];
    if (!c || !isFinite(c.v) || (now - num(c.at)) > MAXAGE) return;
    out[k] = { v: c.v, d: c.d, src: c.src, stale: true };          // bayat → mkRef "*" koyar
  });
  return next;
}
async function marketRefs(env, keys) {
  const out = {};
  const td = apiKeyOf(env, keys, "twelvedata"), fh = apiKeyOf(env, keys, "finnhub");
  const defs = [
    { k: "SPX", td: "SPX", px: "SPY" }, { k: "NDX", td: "NDX", px: "QQQ" },
    { k: "XAU", td: "XAU/USD", px: "GLD" }, { k: "XAG", td: "XAG/USD", px: "SLV" }
  ];
  for (const d of defs) {
    if (td) {
      try {
        const r = await fetch(`https://api.twelvedata.com/quote?symbol=${encodeURIComponent(d.td)}&apikey=${encodeURIComponent(td)}`, { headers: ua() });
        if (r.ok) { const j = await r.json(); const v = parseFloat(j.close != null ? j.close : j.price);
          if (isFinite(v) && v > 0) { out[d.k] = { v, d: parseFloat(j.percent_change), src: d.k }; continue; } }
      } catch (e) {}
    }
    if (fh) {
      /* V-10.0: Finnhub dakikalık limite takılırsa (429) tek sefer tekrar dener.
         SPX/NDX'in "—" kalmasının sebebi buydu; veri yoksa yine "—" gösterilir, uydurulmaz. */
      for (let a = 0; a < 2; a++) {
        try {
          const r = await fetch(`https://finnhub.io/api/v1/quote?symbol=${d.px}&token=${encodeURIComponent(fh)}`, { headers: ua() });
          if (r.ok) { const q = await r.json(); if (q && q.c > 0) { out[d.k] = { v: q.c, d: q.dp, src: d.px }; break; } }
          else if (r.status !== 429) break;
        } catch (e) { break; }
        if (a === 0) await sleep(1500);
      }
      await sleep(900);
    }
  }
  try {
    const h = Object.assign({}, ua()); const cg = apiKeyOf(env, keys, "cg"); if (cg) h["x-cg-demo-api-key"] = cg;
    const r = await fetch("https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_24hr_change=true", { headers: h });
    if (r.ok) { const j = await r.json(); if (j.bitcoin) out.BTC = { v: j.bitcoin.usd, d: j.bitcoin.usd_24h_change, src: "BTC" }; }
    const r2 = await fetch("https://api.coingecko.com/api/v3/global", { headers: h });
    if (r2.ok) { const j2 = await r2.json(); const g = j2 && j2.data;
      if (g && g.total_market_cap && g.total_market_cap.usd) out.MCAP = { v: g.total_market_cap.usd, d: g.market_cap_change_percentage_24h_usd, src: "CG" }; }
  } catch (e) {}
  try { const c = await mktCacheGet(env); await mktCachePut(env, mktMerge(out, c)); } catch (e) {}
  return out;
}
function mfmt(k, o) {
  if (!o || !isFinite(o.v)) return "—";
  const v = k === "MCAP" ? (o.v >= 1e12 ? (o.v / 1e12).toFixed(2) + "T" : (o.v / 1e9).toFixed(0) + "B")
    : (o.v >= 1000 ? Math.round(o.v).toLocaleString("en-US") : o.v.toFixed(2));
  const d = isFinite(o.d) ? ` (${sg(o.d, 2)}%)` : "";
  const px = (o.src && o.src !== k && k !== "MCAP") ? `~${o.src}` : "";
  return `${v}${px ? " " + px : ""}${d}`;
}

/* ---------- MAKRO: son değerler + yaklaşan yayın takvimi (FRED) ---------- */
const FRED_WATCH = { CPIAUCSL: "TÜFE", FEDFUNDS: "Fed faizi", UNRATE: "İşsizlik", PCEPILFE: "Çekirdek PCE" };
async function fredCalendar(env, keys, days, backDays, maxRep, limit) {
  const key = apiKeyOf(env, keys, "fred");
  if (!key) return [];
  const from = trShift(-(backDays || 0)), to = trShift(days || 8);
  try {
    const r = await fetch(`https://api.stlouisfed.org/fred/releases/dates?api_key=${encodeURIComponent(key)}&file_type=json&realtime_start=${from}&realtime_end=${to}&include_release_dates_with_no_data=true&limit=1000`, { headers: ua() });
    if (!r.ok) return [];
    const j = await r.json();
    /* V-11.1 (#8): kapsam genişletildi — istihdam başvuruları, sanayi üretimi, konut, güven endeksi. */
    const want = /consumer price|employment situation|personal income|gross domestic|fomc|producer price|retail sales|jobless claims|industrial production|housing starts|consumer sentiment|consumer confidence/i;
    const seen = {};
    const rows = (j.release_dates || [])
      .filter(x => x && x.release_name && x.date && want.test(x.release_name))
      .filter(x => { const k = x.date + x.release_name; if (seen[k]) return false; seen[k] = 1; return true; });
    /* V-10.0: include_release_dates_with_no_data=true olduğu için bazı yayınlar (ör. FOMC)
       pencerenin HER gününde görünüyor. Aynı ad 3'ten fazla tarihte çıkıyorsa bu gerçek bir
       takvim olayı değil, günlük gürültüdür — komple elenir. Uydurma tarih göstermeyiz. */
    const cnt = {};
    rows.forEach(x => { cnt[x.release_name] = (cnt[x.release_name] || 0) + 1; });
    const rep = maxRep || 3;      // V-11.2: uzun pencerede eşik ölçeklenir (varsayılan eski davranış)
    return rows
      .filter(x => cnt[x.release_name] <= rep)
      .sort((a, b) => a.date < b.date ? -1 : 1)
      .slice(0, limit || 12);
  } catch (e) { return []; }
}
function macroLines(cal, date) {
  return cal.filter(x => x.date === date).map(x => "• " + x.release_name);
}

/* ---------- V-11.1 (#8): FRED son değerleri (günde bir KV önbelleği) ---------- */
async function fredLatest(env, keys, budget) {
  const key = apiKeyOf(env, keys, "fred");
  if (!key) return [];
  const today = trShift(0);
  try {
    const c = JSON.parse((await env.PORTFOLIO.get("fred:latest")) || "null");
    if (c && c.date === today && Array.isArray(c.rows)) return c.rows;
  } catch (e) {}
  const rows = [];
  for (const id of Object.keys(FRED_WATCH)) {
    if (budget && !budget.take()) break;
    try {
      const r = await fetch(`https://api.stlouisfed.org/fred/series/observations?series_id=${id}&api_key=${encodeURIComponent(key)}&file_type=json&sort_order=desc&limit=1`, { headers: ua() });
      if (!r.ok) continue;
      const j = await r.json();
      const o = (j.observations || [])[0];
      if (o && o.value != null && o.value !== ".") rows.push({ lab: FRED_WATCH[id], v: o.value, d: o.date });
    } catch (e) {}
  }
  if (rows.length) { try { await env.PORTFOLIO.put("fred:latest", JSON.stringify({ date: today, rows })); } catch (e) {} }
  return rows;
}
/* ============================================================
   V-11.3: KÜMELENMİŞ İÇERİDEN ALIM  ·  SEC EDGAR Form 4
   ------------------------------------------------------------
   Kaynak zinciri (hepsi ücretsiz ve resmî):
     1) sec.gov/files/company_tickers.json      → ticker→CIK  (KV'de saklanır)
     2) data.sec.gov/submissions/CIK##########.json → son Form 4 dosyalamaları
     3) Archives/.../<accession>/<doc>.xml      → işlem kodu / adet / fiyat / kişi
   Kural (mekanik, yorum yok): pencere içinde **işlem kodu P** (açık piyasa alımı) yapan
   BİRBİRİNDEN FARKLI kişi sayısı ≥ 2 ise "kümelenme" sayılır. Satış (S) hiç sayılmaz.
   Tahmin/uydurma yok: dosyalanmamış şey görünmez, ulaşılamayan sembol "—" kalır.
   SEC dakikada ~600 istek kabul eder ve User-Agent zorunludur.
   ============================================================ */
const INSIDER_BATCH = 4;          // çağrı başına sembol (50 subrequest sınırı)
const INSIDER_MAXDOC = 14;        // çağrı başına indirilecek en fazla Form 4 belgesi
/* SEC User-Agent zorunlu. env.SEC_UA ile değiştirilebilir (SEC "Ad e-posta" biçimi ister). */
function secUA(env) { return { "User-Agent": (env && env.SEC_UA) || "PORT App port-app@users.noreply.github.com", "Accept-Encoding": "gzip" }; }
function insiderUniverse(keys) {
  const closed = p => (+p.qty || 0) <= 1e-6;
  const a = (keys.positions || []).filter(p => p && !p.manual && !p.cg && !closed(p));
  const b = (keys.watch || []).filter(p => p && !p.manual && !p.cg);
  return [...new Set(a.concat(b).map(p => String(p.t || "").toUpperCase()).filter(Boolean))];
}
/* V-11.4: dosya ~1,1 MB. JSON.parse + 10.000 kayıtlık nesne kurmak Worker'ın CPU
   sınırını aşıyordu ve harita sessizce boş kalıyordu (tabloda her sembol "CIK bulunamadı").
   Artık metin olarak tek geçişte taranıp YALNIZ gereken semboller alınıyor; KV'de de
   yalnız o küçük harita saklanıyor. Başarısızlık sessiz kalmaz, sebep döner. */
const CIK_SOURCES = [
  "https://www.sec.gov/files/company_tickers.json",
  "https://www.sec.gov/files/company_tickers_exchange.json"
];
async function cikMap(env, need, diag) {
  let map = {};
  try { map = JSON.parse((await env.PORTFOLIO.get("sec:cik")) || "{}") || {}; } catch (e) {}
  const missing = (need || []).filter(t => !map[t]);
  if (!missing.length) { if (diag) diag.src = "kv"; return map; }
  const want = new Set(missing);
  let note = "";
  for (const u of CIK_SOURCES) {
    let body = "";
    try {
      const r = await fetch(u, { headers: secUA(env) });
      if (!r.ok) { note = "SEC " + r.status; continue; }
      body = await r.text();
    } catch (e) { note = "SEC erişilemedi"; continue; }
    if (diag) { diag.src = u; diag.status = 200; diag.len = body.length; diag.head = body.slice(0, 160); }
    let hit = 0;
    /* company_tickers.json  → "cik_str":320193,"ticker":"NVDA"
       company_tickers_exchange.json → satır dizisi: [320193,"NVIDIA","NVDA","Nasdaq"] */
    const re1 = /"cik_str"\s*:\s*(\d+)\s*,\s*"ticker"\s*:\s*"([^"]+)"/g;
    let m; while ((m = re1.exec(body))) { const t = m[2].toUpperCase(); if (want.has(t) && !map[t]) { map[t] = m[1]; hit++; } }
    if (!hit) {
      const re2 = /\[\s*(\d+)\s*,\s*"(?:[^"\\]|\\.)*"\s*,\s*"([^"]+)"/g;
      while ((m = re2.exec(body))) { const t = m[2].toUpperCase(); if (want.has(t) && !map[t]) { map[t] = m[1]; hit++; } }
    }
    if (diag) diag.hit = (diag.hit || 0) + hit;
    if (hit) { note = ""; }
    if (missing.every(t => map[t])) break;
  }
  try { await env.PORTFOLIO.put("sec:cik", JSON.stringify(map)); } catch (e) {}
  if (note && diag) diag.note = note;
  cikMap._note = note;
  return map;
}
function pad10(c) { return String(c).replace(/\D/g, "").padStart(10, "0"); }
/* Form 4 XML'inden açık piyasa ALIMLARINI çıkar (kod P + edinim A). Regex — Worker'da DOMParser yok. */
function parseForm4(xml) {
  const owners = [];
  const re = /<rptOwnerName>([^<]+)<\/rptOwnerName>/g;
  let m; while ((m = re.exec(xml))) owners.push(m[1].trim());
  const title = (/<officerTitle>([^<]+)<\/officerTitle>/.exec(xml) || [])[1] || "";
  const buys = [];
  const chunks = String(xml).split(/<nonDerivativeTransaction>/).slice(1);
  for (const c of chunks) {
    if (!/<transactionCode>\s*P\s*<\/transactionCode>/.test(c)) continue;
    const ad = /<transactionAcquiredDisposedCode>[\s\S]*?<value>\s*([AD])\s*<\/value>/.exec(c);
    if (ad && ad[1] !== "A") continue;
    const sh = parseFloat(((/<transactionShares>[\s\S]*?<value>([\d.,]+)<\/value>/.exec(c) || [])[1] || "0").replace(/,/g, "")) || 0;
    const pr = parseFloat(((/<transactionPricePerShare>[\s\S]*?<value>([\d.,]+)<\/value>/.exec(c) || [])[1] || "0").replace(/,/g, "")) || 0;
    const dt = ((/<transactionDate>[\s\S]*?<value>(\d{4}-\d{2}-\d{2})<\/value>/.exec(c) || [])[1] || "");
    if (sh <= 0) continue;
    buys.push({ sh, pr, dt, val: Math.round(sh * pr) });
  }
  return { owners, title, buys };
}
async function insiderOne(env, sym, cik, win, budget) {
  const out = { t: sym, cik, at: new Date().toISOString(), win, people: [], buys: [], filings: 0, n: 0, total: 0, last: "", cluster: false, note: "" };
  if (!cik) { out.note = cikMap._note ? ("CIK listesi inmedi · " + cikMap._note) : "CIK yok (ABD dışı / ETF olabilir)"; return out; }
  let sub = null;
  try {
    if (!budget.take()) { out.note = "bütçe doldu"; return out; }
    const r = await fetch("https://data.sec.gov/submissions/CIK" + pad10(cik) + ".json", { headers: secUA(env) });
    if (!r.ok) { out.note = "SEC " + r.status; return out; }
    sub = await r.json();
  } catch (e) { out.note = "SEC erişilemedi"; return out; }
  const rec = (sub && sub.filings && sub.filings.recent) || {};
  const form = rec.form || [], date = rec.filingDate || [], acc = rec.accessionNumber || [], doc = rec.primaryDocument || [];
  const from = trShift(-win);
  const hits = [];
  for (let i = 0; i < form.length; i++) {
    if (String(form[i]) !== "4") continue;
    if (!date[i] || date[i] < from) continue;
    hits.push({ acc: String(acc[i] || ""), doc: String(doc[i] || ""), d: date[i] });
  }
  out.filings = hits.length;
  const people = {};
  for (const f of hits.slice(0, INSIDER_MAXDOC)) {
    if (!budget.take()) { out.note = "bütçe doldu — kalan dosyalar sonraki taramada"; break; }
    const accNo = f.acc.replace(/-/g, "");
    const docPath = f.doc.replace(/^xsl[^/]*\//, "");     // xsl önekini at → ham XML
    const u = "https://www.sec.gov/Archives/edgar/data/" + String(cik).replace(/\D/g, "") + "/" + accNo + "/" + docPath;
    let xml = "";
    try { const r = await fetch(u, { headers: secUA(env) }); if (!r.ok) continue; xml = await r.text(); } catch (e) { continue; }
    if (!/<transactionCode>/.test(xml)) continue;
    const pf = parseForm4(xml);
    if (!pf.buys.length) continue;
    const who = (pf.owners[0] || "—").replace(/\s+/g, " ");
    const sum = pf.buys.reduce((s, b) => s + b.val, 0);
    const last = pf.buys.map(b => b.dt).sort().pop() || f.d;
    people[who] = people[who] || { who, title: pf.title, val: 0, sh: 0, last: "" };
    people[who].val += sum;
    people[who].sh += pf.buys.reduce((s, b) => s + b.sh, 0);
    if (last > people[who].last) people[who].last = last;
    out.buys.push({ who, d: last, val: sum, url: u });
    await sleep(120);                                    // SEC hız sınırına saygı
  }
  out.people = Object.values(people).sort((a, b) => b.val - a.val);
  out.n = out.people.length;
  out.total = out.people.reduce((s, p) => s + p.val, 0);
  out.last = out.people.map(p => p.last).sort().pop() || "";
  out.cluster = out.n >= 2;
  return out;
}
async function insiderScan(env, syms, win) {
  if (!env.PORTFOLIO) throw new Error("KV yok");
  cikMap._note = "";
  const map = await cikMap(env, syms);
  const budget = subBudget(44);
  const rows = [];
  let skipped = [];
  let cache = {};
  try { cache = JSON.parse((await env.PORTFOLIO.get("scan:insider")) || "{}") || {}; } catch (e) {}
  for (const t of syms) {
    if (budget.left() <= 2) { skipped.push(t); continue; }
    const o = await insiderOne(env, t, map[t] || "", win, budget);
    cache[t] = o; rows.push(o);
  }
  try { await env.PORTFOLIO.put("scan:insider", JSON.stringify(cache)); } catch (e) {}
  return { rows, scanned: rows.map(r => r.t), skipped };
}
async function insiderCacheRows(env) {
  try {
    const c = JSON.parse((await env.PORTFOLIO.get("scan:insider")) || "{}") || {};
    return Object.values(c).sort((a, b) => (b.n || 0) - (a.n || 0) || (b.total || 0) - (a.total || 0));
  } catch (e) { return []; }
}

/* ---------- V-11.2 (#3): ANLIK ÖNEMLİ HABER PUSH ----------
   Kaynak: Finnhub company-news (gerçek, kamuya açık). Başlık olduğu gibi gider, yorum eklenmez.
   · Her koşuda yalnız birkaç sembol taranır (KV imleci ile sırayla) → 50 subrequest sınırı korunur.
   · Aynı haber bir kez gider (KV "news:sent", gün bazlı).
   · 8 saatten eski haber gönderilmez — "anlık" olmayan şeyi anlık gibi göstermeyiz.
   · nfNews=0 ile kapatılır, nfNewsMax koşu başına üst sınırdır. */
const NEWS_SLICE = 5;
async function newsPushCheck(env, keys, cfg, push) {
  if (String(cfg.nfNews) === "0" || cfg.nfNews === false) return [];
  const fh = apiKeyOf(env, keys, "finnhub");
  if (!fh || !env.PORTFOLIO) return [];
  const closed = p => (+p.qty || 0) <= 1e-6;
  const syms = [...new Set((keys.positions || [])
    .filter(p => p && !p.manual && !p.cg && !closed(p))
    .map(p => String(p.t || "").toUpperCase()).filter(Boolean))];
  if (!syms.length) return [];
  let cur = 0;
  try { cur = parseInt((await env.PORTFOLIO.get("news:cursor")) || "0", 10) || 0; } catch (e) {}
  const pick = [];
  for (let i = 0; i < Math.min(NEWS_SLICE, syms.length); i++) pick.push(syms[(cur + i) % syms.length]);
  try { await env.PORTFOLIO.put("news:cursor", String((cur + NEWS_SLICE) % syms.length)); } catch (e) {}
  const today = trDateStr();
  let sent = {};
  try { const raw = await env.PORTFOLIO.get("news:sent"); if (raw) { const o = JSON.parse(raw); if (o && o.date === today) sent = o.map || {}; } } catch (e) {}
  const from = trShift(-1), to = trShift(0);
  const maxPer = Math.max(1, num(cfg.nfNewsMax) || 2);
  const out = [];
  let n = 0;
  for (const t of pick) {
    if (n >= maxPer) break;
    let j = null;
    try {
      const r = await fetch(`https://finnhub.io/api/v1/company-news?symbol=${encodeURIComponent(t)}&from=${from}&to=${to}&token=${encodeURIComponent(fh)}`, { headers: ua() });
      if (!r.ok) continue;
      j = await r.json();
    } catch (e) { continue; }
    const it = (Array.isArray(j) ? j : [])
      .filter(x => x && x.headline && x.datetime)
      .sort((a, b) => num(b.datetime) - num(a.datetime))[0];
    if (!it) continue;
    const ageH = (Date.now() / 1000 - num(it.datetime)) / 3600;
    if (!(ageH >= 0 && ageH <= 8)) continue;
    const kk = t + "|" + String(it.id || it.url || it.headline).slice(0, 48);
    if (sent[kk]) continue;
    sent[kk] = 1;
    const body = String(it.headline).slice(0, 170) + (it.source ? (" · " + it.source) : "");
    if (await push({ title: "📰 " + t, body, tag: "news-" + t, url: "/" })) { out.push("news " + t); n++; }
    await sleep(1100);
  }
  try { await env.PORTFOLIO.put("news:sent", JSON.stringify({ date: today, map: sent })); } catch (e) {}
  return out;
}
/* ---------- V-11.1 (#13): sembol haberleri — sabah mesajına toplu, ayrı push yok ---------- */
async function newsFor(env, keys, syms, budget) {
  const fh = apiKeyOf(env, keys, "finnhub");
  if (!fh || !syms.length) return [];
  const from = trShift(-1), to = trShift(0);
  const out = [];
  for (const t of syms) {
    if (budget && !budget.take()) break;
    try {
      const r = await fetch(`https://finnhub.io/api/v1/company-news?symbol=${encodeURIComponent(t)}&from=${from}&to=${to}&token=${encodeURIComponent(fh)}`, { headers: ua() });
      if (!r.ok) continue;
      const j = await r.json();
      const n = (Array.isArray(j) ? j : []).filter(x => x && x.headline)
        .sort((a, b) => num(b.datetime) - num(a.datetime))[0];
      if (n) out.push({ t, h: String(n.headline).slice(0, 90), src: n.source || "" });
    } catch (e) {}
  }
  return out;
}
/* Ek çağrılar için subrequest bütçesi (Cloudflare çağrı başına 50 alt-istek verir).
   Mesajın çekirdeği önce kurulur; ekler bütçe kalırsa gelir, kalmazsa sessizce atlanır. */
function subBudget(n) { let left = n; return { take() { if (left <= 0) return false; left--; return true; }, left() { return left; } }; }

/* ---------- V-11.1 (#7): target'a yaklaşan watchlist ---------- */
function nearTargets(keys, pctBand) {
  const band = pctBand || 5;
  const out = [];
  (keys.watch || []).forEach(w => {
    const p = num(w && w.price), t = num(w && w.target);
    if (!(p > 0) || !(t > 0)) return;
    const gap = (t - p) / p * 100;
    if (Math.abs(gap) <= band) out.push({ t: w.t, price: p, target: t, gap });
  });
  out.sort((a, b) => Math.abs(a.gap) - Math.abs(b.gap));
  return out.slice(0, 6);
}
/* ---------- V-11.1 (#7): bütçesi aşılmış gruplar (piyasa değeri > bütçe) ---------- */
function budgetOver(keys) {
  const B = keys.budgets || (keys.settings && keys.settings.budgets) || {};   // uygulama keys.budgets olarak yolluyor
  const out = [];
  Object.keys(B).forEach(g => {
    const bud = num(B[g]);
    if (!(bud > 0)) return;
    const v = groupValLive(keys, g);
    if (v == null || !(v > bud)) return;
    out.push({ g, v, bud, pct: (v - bud) / bud * 100 });
  });
  out.sort((a, b) => b.pct - a.pct);
  return out;
}
/* ---------- V-11.1 (#7): portföy vs SPX/BTC göreli fark (dünkü kapanış) ---------- */
function relPerf(dayPct, mkt) {
  const out = [];
  [["SPX", "SPX"], ["BTC", "BTC"]].forEach(([k, lab]) => {
    const o = mkt && mkt[k];
    if (!o || !isFinite(o.d)) { out.push(`${lab} —`); return; }
    out.push(`${lab} ${sg(dayPct - o.d, 2)} puan`);
  });
  return out;
}

/* ---------- YAPAY ZEKA YORUMU (yalnız verilen gerçek sayılardan) ---------- */
async function aiParagraph(env, keys, facts, kind) {
  const key = apiKeyOf(env, keys, "apiKey");
  if (!key) return "";
  const model = ((keys.apiKeys && keys.apiKeys.aiModel) || "claude-sonnet-5") + "";
  const system = "Türkçe yaz. Verilen veriler dışına ÇIKMA, veri uydurma. Metninde RAKAM/SAYI/YÜZDE YAZMA — sayılar mesajın üst kısmında zaten var; sen yalnızca nitel yorum yaz ('yükseldi', 'sert geriledi', 'yatay seyretti' gibi). Sembol adları yazılabilir. Yatırım tavsiyesi verme. Süslü başlık, madde işareti, markdown kullanma — düz metin, en fazla 3 kısa cümle.";
  /* V-11.0: zaman kayması düzeltmesi — performans verisi DÜNÜN kapanışına ait, bugüne dair
     yalnız takvim (earnings/makro) bilgisi var. Etiketler facts içinde de tarihli veriliyor. */
  const user = (kind === "weekly"
    ? "Aşağıdaki gerçek verilerle 3 cümlede özetle. ZAMAN KURALI: performans verileri (haftaPct, gruplar, kazananlar/kaybedenler, piyasa) GEÇEN HAFTAYA aittir — bunları 'bu hafta' diye anlatma. 'gelecekHafta' yalnız takvimdir, henüz gerçekleşmedi; onun için tahmin/sonuç yazma, sadece takip edilecek başlık olarak an. Sıra: geçen hafta portföyde ne oldu, neyin öne çıktığı, gelecek hafta nelere dikkat edilmeli.\n"
    : "Aşağıdaki gerçek verilerle 3 cümlede özetle. ZAMAN KURALI: performans verileri (dunKapanisPct, gruplar, dunKazananlar, dunKaybedenler, piyasa) DÜNÜN kapanışına aittir — bunları 'bugün' diye anlatma, 'dün' de. Bugüne dair elimizde yalnız takvim var (bugunEarnings/bugunMakro); bugünün piyasası henüz açılmadı, bugün için sonuç/yön yazma, sadece takip edilecek başlık olarak an. Sıra: dün portföyde ne oldu, bugün nelere dikkat edilmeli.\n") + JSON.stringify(facts);
  try {
    const r = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-api-key": key, "anthropic-version": "2023-06-01" },
      body: JSON.stringify({ model, max_tokens: 400, system, messages: [{ role: "user", content: user }] })
    });
    if (!r.ok) return "";
    const j = await r.json();
    return ((j.content || []).filter(c => c.type === "text").map(c => c.text).join(" ") || "").trim();
  } catch (e) { return ""; }
}

/* ---------- ORTAK: dönem performansı ---------- */
function histSorted(keys) { return (Array.isArray(keys.history) ? keys.history : []).slice().sort((a, b) => a.date < b.date ? -1 : 1); }
function recAt(keys, date) { let ref = null; for (const h of histSorted(keys)) { if (h.date <= date) ref = h; else break; } return ref; }
/* V-9.9: kayıtta grup kırılımı yoksa (eski kayıt ya da başka cihazın yazdığı satır)
   fiyat × adet × meta.group üzerinden yeniden hesapla — yüzdeler "—" kalmasın. */
function termOf(p) { return ((p && p.term) || "").trim() || "Hodl"; }
/* V-10.3: strateji (Hodl/Moonshot) kırılımı. Kayıtta hazır değer varsa onu kullanır,
   yoksa prices+qty+meta üçlüsünden hesaplar (meta.term eski kayıtlarda da var).
   Hiçbiri yoksa null döner → mesajda "—" görünür, sayı UYDURULMAZ. */
function groupValOf(rec, g, term) {
  if (!rec) return null;
  if (term) {
    const tv = rec.groups && rec.groups[g] && rec.groups[g].terms && num(rec.groups[g].terms[term]);
    if (tv) return tv;
    const pr = rec.prices || {}, qt = rec.qty || {}, mt = rec.meta || {};
    let sum = 0, found = false;
    Object.keys(pr).forEach(t => {
      const m = mt[t]; if (!m || m.group !== g) return;
      if (termOf(m) !== term) return;
      const q = num(qt[t]); if (!q) return;
      sum += q * num(pr[t]); found = true;
    });
    return found ? sum : null;
  }
  const gv = rec.groups && rec.groups[g] && num(rec.groups[g].val);
  if (gv) return gv;
  const pr = rec.prices || {}, qt = rec.qty || {}, mt = rec.meta || {};
  let sum = 0, found = false;
  Object.keys(pr).forEach(t => {
    const m = mt[t]; if (!m || m.group !== g) return;
    const q = num(qt[t]); if (!q) return;
    sum += q * num(pr[t]); found = true;
  });
  return found ? sum : null;
}
function groupValLive(keys, g, term) {
  const ps = (keys.positions || []).filter(p => p && num(p.qty) > 1e-6 && groupOf(p) === g && (!term || termOf(p) === term));
  if (!ps.length) return null;
  return ps.reduce((a, p) => a + (p.lev ? num(p.qty) * (num(p.price) - num(p.cost)) : num(p.qty) * num(p.price)), 0);
}
function termsOfGroup(keys, g) {
  const set = new Set((keys.positions || []).filter(p => p && num(p.qty) > 1e-6 && groupOf(p) === g).map(termOf));
  const pref = ["Hodl", "Moonshot"];
  return [...pref.filter(x => set.has(x)), ...[...set].filter(x => !pref.includes(x)).sort()];
}
function groupPctBetween(a, b, g, keysForLive, term) {
  const x = groupValOf(a, g, term);
  let y = groupValOf(b, g, term);
  if (y == null && keysForLive) y = groupValLive(keysForLive, g, term);   // son uç için canlı değere düş
  if (x == null || y == null || !x) return null;
  return (y - x) / x * 100;
}
function moversBetween(keys, refDate) {
  const ref = recAt(keys, refDate);
  const positions = (keys.positions || []).filter(p => p && p.t && num(p.qty) > 1e-6);
  if (!ref || !ref.prices) return null;
  const out = [];
  positions.forEach(p => { const rp = num(ref.prices[p.t]); if (rp > 0 && num(p.price) > 0) out.push({ t: p.t, pct: (num(p.price) - rp) / rp * 100 }); });
  out.sort((a, b) => b.pct - a.pct);
  return out;
}
/* V-10.9: Hizalama tamamen kaldırıldı (WhatsApp orantılı yazı tipinde boşlukla hizalama
   dağılıyordu). Artık başlıklar *bold*, her veri satırı kendi satırında:
     *Stocks (Hodl)* - (+3.78%)
     SPX 776.34 ~SPY (-0.20%)
   Tek değerli bölümler (Portföy/Trade) başlıkla aynı satırda kalır. */
function maskTot(v) { return "(0," + Math.round(num(v)) + ")"; }   // telefonda görene toplam belli olmasın
function head(lab, pct, inline) { return `*${lab}* - ${pct}${inline ? " " + inline : ""}`; }
/* Vekil (ETF) varsa aynı satırda birleşir: "XAG 60.01 ~SLV (+4.47%)". Veri yoksa "K —". */
function mkRef(k, o, lbl) {
  const nm = lbl || k;
  if (!o || !isFinite(o.v)) return [nm + " —"];
  const v = k === "MCAP" ? (o.v >= 1e12 ? (o.v / 1e12).toFixed(2) + "T" : (o.v / 1e9).toFixed(0) + "B")
    : (o.v >= 1000 ? Math.round(o.v).toLocaleString("en-US") : o.v.toFixed(2));
  const d = isFinite(o.d) ? ` (${sg(o.d, 2)}%)` : "";
  const proxy = (o.src && o.src !== k && k !== "MCAP") ? "~" + o.src : "";
  const st = o.stale ? "*" : "";                                   // V-10.4: son bilinen değer
  return [proxy ? `${nm} ${v}${st} ${proxy}${d}` : `${nm} ${v}${st}${d}`];
}
function pctList(arr) { return arr.map(x => `${x.t} ${sg(x.pct, 1)}%`).join(" · ") || "—"; }
function earningsOn(keys, date) {
  const all = [...(keys.positions || []), ...(keys.watch || [])].filter(x => x && x.earn === date);
  const seen = {}; const out = [];
  all.forEach(x => { const k = x.t.toUpperCase(); if (seen[k]) return; seen[k] = 1;
    out.push(x.t + (x.earnEps != null ? ` (EPS bek. ${x.earnEps})` : "")); });
  return out;
}

/* ---------- SABAH MESAJI (Pzt–Cum 07:30) ---------- */
async function buildMorning(env, send) {
  const st = await fetchState(env); const keys = st.keys || {};
  /* V-10.1: piyasa referansları ÖNCE. Cloudflare çağrı başına 50 subrequest veriyor;
     fetchLivePrices sembol başına 1 istek attığı için sıranın sonundaki referans düşüyordu. */
  const mkt = await marketRefs(env, keys);
  try { await fetchLivePrices(env, keys); } catch (e) {}
  const s = computeSummary(keys);
  const today = trShift(0), yest = trShift(-1);
  const recY = recAt(keys, yest), recP = recAt(keys, trShift(-2));
  const dayPct = (recY && recP && num(recP.total)) ? (num(recY.total) - num(recP.total)) / num(recP.total) * 100 : s.todayPLp;
  const gp = g => { const v = groupPctBetween(recP, recY, g, keys); return v == null ? "(—)" : "(" + sg(v, 2) + "%)"; };
  const gpT = (g, tm) => { const v = groupPctBetween(recP, recY, g, keys, tm || undefined); return v == null ? "(—)" : "(" + sg(v, 2) + "%)"; };
  const openPos = (keys.positions || []).filter(p => num(p.qty) > 1e-6);
  const pos = openPos.filter(p => num(p.day) > 0).length, neg = openPos.filter(p => num(p.day) < 0).length;
  const tradeExp = openPos.filter(p => (p.group === "Trade")).reduce((a, p) => a + num(p.qty) * num(p.price), 0);
  const cal = await fredCalendar(env, keys, 3);
  const eT = earningsOn(keys, today), eY = earningsOn(keys, trShift(1));
  const mT = macroLines(cal, today), mY = macroLines(cal, trShift(1));
  const d = trParts().d;
  const L = [];
  L.push("Günaydın Şakir ☀️");
  L.push(`Günün özeti · ${String(d.getUTCDate()).padStart(2, "0")}.${String(d.getUTCMonth() + 1).padStart(2, "0")}.${d.getUTCFullYear()}`);
  L.push("");
  L.push(head("Portföy", "(" + sg(dayPct, 2) + "%)", maskTot(recY ? num(recY.total) : s.grand)));
  L.push("");
  const stTerms = termsOfGroup(keys, "Stock");
  const stRefs = [mkRef("SPX", mkt.SPX), mkRef("NDX", mkt.NDX)];
  (stTerms.length ? stTerms : [null]).forEach((tm, i) => {
    L.push(head(tm ? `Stocks (${tm})` : "Stocks", gpT("Stock", tm)));
    L.push(...(stRefs[i] || []));
    L.push("");
  });
  for (let i = stTerms.length; i < stRefs.length; i++) { L.push(...stRefs[i]); L.push(""); }
  L.push(head("Commodity", gp("Commodity")));
  L.push(...mkRef("XAU", mkt.XAU));
  L.push(...mkRef("XAG", mkt.XAG));
  L.push("");
  L.push(head("Crypto", gp("Crypto")));
  L.push(...mkRef("BTC", mkt.BTC));
  L.push(...mkRef("MCAP", mkt.MCAP, "Toplam"));
  L.push("");
  L.push(head("Trade", gp("Trade"), "Exposure " + usd(tradeExp)));
  if (Object.keys(mkt).some(k => mkt[k] && mkt[k].stale)) L.push("* son bilinen değer");
  L.push("");
  L.push(`🟢 ${pctList((s.gainers || []).map(p => ({ t: p.t, pct: num(p.day) })))}`);
  L.push(`🔴 ${pctList((s.losers || []).map(p => ({ t: p.t, pct: num(p.day) })))}`);
  L.push(`📊 ${openPos.length} üründen · ${pos} pozitif · ${neg} negatif`);
  /* V-11.1 (#7): target yakını · bütçe aşımı · göreli performans */
  const nt = nearTargets(keys, 5), bo = budgetOver(keys), rp = relPerf(dayPct, mkt);
  L.push(`⚖️ Portföy vs ${rp.join(" · ")}`);
  L.push("");
  if (nt.length) {
    L.push("*Target yakını* (±%5)");
    nt.forEach(x => L.push(`${x.t} ${x.price.toFixed(2)} → ${x.target.toFixed(2)} (${sg(x.gap, 1)}%)`));
    L.push("");
  }
  if (bo.length) {
    L.push("*Bütçe aşımı*");
    bo.forEach(x => L.push(`${x.g} ${usd(x.v)} / ${usd(x.bud)} (${sg(x.pct, 1)}%)`));
    L.push("");
  }
  /* V-11.1 (#13): dünkü hareket edenlerin haber başlıkları — bütçe kalırsa */
  const budget = subBudget(8);
  const newsSyms = [...new Set([...(s.gainers || []), ...(s.losers || [])].map(x => x && x.t).filter(Boolean))].slice(0, 4);
  const news = await newsFor(env, keys, newsSyms, budget);
  if (news.length) {
    L.push("*Haber*");
    news.forEach(n => L.push(`${n.t}: ${n.h}${n.src ? " · " + n.src : ""}`));
    L.push("");
  }
  /* V-11.1 (#8): makro son değerler */
  const fl = await fredLatest(env, keys, budget);
  if (fl.length) {
    L.push("*Makro son değerler*");
    fl.forEach(x => L.push(`${x.lab} ${x.v} (${x.d})`));
    L.push("");
  }
  const ai = await aiParagraph(env, keys, {
    tarihler: { dun: yest, bugun: today },
    veriZamani: "performans verileri " + yest + " kapanışına aittir; bugün (" + today + ") için yalnız takvim var",
    dunKapanisPct: +dayPct.toFixed(2), portfoy: Math.round(s.grand),
    gruplar: { Stock: gp("Stock"), Commodity: gp("Commodity"), Crypto: gp("Crypto"), Trade: gp("Trade") },
    dunKazananlar: (s.gainers || []).map(p => p.t + " " + sg(num(p.day), 1) + "%"),
    dunKaybedenler: (s.losers || []).map(p => p.t + " " + sg(num(p.day), 1) + "%"),
    piyasa: Object.fromEntries(Object.keys(mkt).map(k => [k, mfmt(k, mkt[k])])),
    bugunEarnings: eT, yarinEarnings: eY, bugunMakro: mT, yarinMakro: mY,
    targetYakini: nt.map(x => x.t + " " + sg(x.gap, 1) + "%"),
    butceAsimi: bo.map(x => x.g + " " + sg(x.pct, 1) + "%"),
    dunGoreli: rp,
    haberBasliklari: news.map(n => n.t + ": " + n.h),
    makroSonDegerler: fl.map(x => x.lab + " " + x.v + " (" + x.d + ")")
  }, "daily");
  if (ai) { L.push("──────────"); L.push(""); L.push("*Günlük AI Yorum*"); L.push(ai); }
  L.push("");
  L.push("*Bugünün önemli olayları*");
  if (eT.length) L.push("• Earnings: " + eT.join(", "));
  if (mT.length) L.push(...mT);
  if (!eT.length && !mT.length) L.push("• Takvimde kayıtlı önemli olay yok");
  L.push("");
  L.push("*Yarının önemli olayları*");
  if (eY.length) L.push("• Earnings: " + eY.join(", "));
  if (mY.length) L.push(...mY);
  if (!eY.length && !mY.length) L.push("• Takvimde kayıtlı önemli olay yok");
  const text = L.join("\n");
  if (send) await sendWA(env, text);
  return text;
}

/* ---------- HAFTALIK MESAJ (Pazar 19:00) ---------- */
async function buildWeeklyMsg(env, send) {
  const st = await fetchState(env); const keys = st.keys || {};
  const mkt = await marketRefs(env, keys);                    // V-10.1: referanslar önce (subrequest bütçesi)
  try { await fetchLivePrices(env, keys); } catch (e) {}
  const s = computeSummary(keys);
  const w0 = trShift(-7), today = trShift(0);
  const recNow = recAt(keys, today), recW = recAt(keys, w0);
  const weekPct = (recNow && recW && num(recW.total)) ? (num(recNow.total) - num(recW.total)) / num(recW.total) * 100 : (s.lb7 ? s.lb7.pct : 0);
  const gp = g => { const v = groupPctBetween(recW, recNow, g, keys); return v == null ? "(—)" : "(" + sg(v, 2) + "%)"; };
  const gpT = (g, tm) => { const v = groupPctBetween(recW, recNow, g, keys, tm || undefined); return v == null ? "(—)" : "(" + sg(v, 2) + "%)"; };
  const mv = moversBetween(keys, w0) || [];
  const openPos = (keys.positions || []).filter(p => num(p.qty) > 1e-6);
  const up = mv.filter(x => x.pct > 0), dn = mv.filter(x => x.pct < 0);
  const tradeExp = openPos.filter(p => p.group === "Trade").reduce((a, p) => a + num(p.qty) * num(p.price), 0);
  const cal = await fredCalendar(env, keys, 9, 7);          // geçen hafta + gelecek hafta
  const evWeek = [], evNext = [];
  for (let i = -7; i <= 7; i++) {
    const dt = trShift(i);
    const e = earningsOn(keys, dt), m = macroLines(cal, dt);
    const line = [...(e.length ? ["Earnings: " + e.join(", ")] : []), ...m.map(x => x.replace("• ", ""))];
    if (line.length) (i <= 0 ? evWeek : evNext).push(`• ${dt.slice(8)}.${dt.slice(5, 7)} — ${line.join(" · ")}`);
  }
  const d = trParts().d;
  const L = [];
  L.push("Mutlu Pazarlar Şakir 🌇");
  L.push(`Haftanın görünümü · ${String(d.getUTCDate()).padStart(2, "0")}.${String(d.getUTCMonth() + 1).padStart(2, "0")}.${d.getUTCFullYear()}`);
  L.push("");
  L.push(head("Portföy", "(" + sg(weekPct, 2) + "%)", maskTot(s.grand)));
  L.push("");
  const stTerms = termsOfGroup(keys, "Stock");
  const stRefs = [mkRef("SPX", mkt.SPX), mkRef("NDX", mkt.NDX)];
  (stTerms.length ? stTerms : [null]).forEach((tm, i) => {
    L.push(head(tm ? `Stocks (${tm})` : "Stocks", gpT("Stock", tm)));
    L.push(...(stRefs[i] || []));
    L.push("");
  });
  for (let i = stTerms.length; i < stRefs.length; i++) { L.push(...stRefs[i]); L.push(""); }
  L.push(head("Commodity", gp("Commodity")));
  L.push(...mkRef("XAU", mkt.XAU));
  L.push(...mkRef("XAG", mkt.XAG));
  L.push("");
  L.push(head("Crypto", gp("Crypto")));
  L.push(...mkRef("BTC", mkt.BTC));
  L.push(...mkRef("MCAP", mkt.MCAP, "Toplam"));
  L.push("");
  L.push(head("Trade", gp("Trade"), "Exposure " + usd(tradeExp)));
  if (Object.keys(mkt).some(k => mkt[k] && mkt[k].stale)) L.push("* son bilinen değer");
  L.push("");
  L.push(`🟢 ${pctList(up.slice(0, 3))}`);
  L.push(`🔴 ${pctList(dn.slice(-3).reverse())}`);
  L.push(`📊 ${mv.length} üründen · ${up.length} pozitif · ${dn.length} negatif`);
  if (s.lb30) L.push(`📈 30 gün: (${sg(s.lb30.pct)}%)`);
  const ai = await aiParagraph(env, keys, {
    haftaPct: +weekPct.toFixed(2), portfoy: Math.round(s.grand),
    gruplar: { Stock: gp("Stock"), Commodity: gp("Commodity"), Crypto: gp("Crypto"), Trade: gp("Trade") },
    gecenHaftaninKazananlari: up.slice(0, 3).map(x => x.t + " " + sg(x.pct, 1) + "%"),
    gecenHaftaninKaybedenleri: dn.slice(-3).map(x => x.t + " " + sg(x.pct, 1) + "%"),
    gecenHafta: evWeek,
    piyasa: Object.fromEntries(Object.keys(mkt).map(k => [k, mfmt(k, mkt[k])])),
    gelecekHafta: evNext
  }, "weekly");
  if (ai) { L.push("──────────"); L.push(""); L.push("*Haftalık AI Yorum*"); L.push(ai); }
  L.push("");
  L.push("*Haftanın önemli olayları*");
  if (evWeek.length) L.push(...evWeek.slice(0, 8)); else L.push("• Kayıtlı olay yok");
  L.push("");
  L.push("*Gelecek haftanın önemli olayları*");
  if (evNext.length) L.push(...evNext.slice(0, 10)); else L.push("• Takvimde kayıtlı önemli olay yok");
  const text = L.join("\n");
  if (send) await sendWA(env, text);
  return text;
}
