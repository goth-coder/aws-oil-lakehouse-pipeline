import { useCallback, useEffect, useRef, useState } from "react";
import { MonthlyBarChart } from "./components/MonthlyBarChart";
import { PriceChart } from "./components/PriceChart";
import { RefreshButton } from "./components/RefreshButton";
import {
  fetchDashboardData,
  pollStatus,
  triggerPipeline,
} from "./services/pipelineService";
import { THEME } from "./theme";
import type { DashboardData } from "./types";

const POLL_INTERVAL_MS = 15_000;
const POLL_MAX_ATTEMPTS = 60; // 60 × 15 s = 15 min (Glue jobs need warm-up time)
const LS_KEY = "oil_dashboard_v1";
const LS_TTL_MS = 60 * 60 * 1000;

function lsLoad(): DashboardData | null {
  try {
    const raw = localStorage.getItem(LS_KEY);
    if (!raw) return null;
    const { data, ts } = JSON.parse(raw) as { data: DashboardData; ts: number };
    if (Date.now() - ts > LS_TTL_MS) return null;
    return data;
  } catch { return null; }
}
function lsSave(data: DashboardData): void {
  try { localStorage.setItem(LS_KEY, JSON.stringify({ data, ts: Date.now() })); } catch {}
}
function lsClear(): void {
  try { localStorage.removeItem(LS_KEY); } catch {}
}

function usd(v: number | null | undefined): string {
  if (v == null) return "N/A";
  return `$${v.toFixed(2)}`;
}

interface SideKpiProps { label: string; value: string; delta?: number }
function SideKpi({ label, value, delta }: SideKpiProps) {
  const positive = delta !== undefined && delta >= 0;
  return (
    <div style={{
      backgroundColor: THEME.background,
      borderRadius: 8,
      padding: "12px 14px",
      marginBottom: 10,
      borderLeft: `3px solid ${THEME.border}`,
    }}>
      <div style={{ color: THEME.textSecondary, fontSize: 10, textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 4 }}>
        {label}
      </div>
      <div style={{ color: THEME.textPrimary, fontSize: 20, fontWeight: 700 }}>{value}</div>
      {delta !== undefined && (
        <div style={{ fontSize: 12, fontWeight: 600, marginTop: 3, color: positive ? THEME.positive : THEME.negative }}>
          {positive ? "▲" : "▼"} {Math.abs(delta).toFixed(2)}%
        </div>
      )}
    </div>
  );
}

export default function App() {
  const [dashboardData, setDashboardData] = useState<DashboardData | null>(null);
  const [loading, setLoading] = useState(false);
  const [initialLoading, setInitialLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const attemptsRef = useRef(0);

  const stopPolling = useCallback(() => {
    if (pollRef.current !== null) { clearInterval(pollRef.current); pollRef.current = null; }
    attemptsRef.current = 0;
  }, []);

  const startPipelinePolling = useCallback((jobRunId: string | null) => {
    attemptsRef.current = 0;
    pollRef.current = setInterval(async () => {
      attemptsRef.current += 1;
      if (attemptsRef.current > POLL_MAX_ATTEMPTS) {
        stopPolling();
        // Job may have finished even if we stopped polling — attempt a final fetch.
        try {
          const fresh = await fetchDashboardData();
          lsSave(fresh); setDashboardData(fresh);
        } catch { /* ignore */ }
        setError("Polling timeout. The job may still be running; data shown may be stale.");
        setLoading(false);
        return;
      }
      try {
        if (jobRunId) {
          const status = await pollStatus(jobRunId);
          if (status.status === "SUCCEEDED") {
            stopPolling();
            const fresh = await fetchDashboardData();
            lsSave(fresh); setDashboardData(fresh); setLoading(false);
          } else if (status.status === "FAILED") {
            stopPolling(); setError(status.error ?? "Pipeline failed. Check Glue logs."); setLoading(false);
          }
        } else {
          try {
            const fresh = await fetchDashboardData();
            lsSave(fresh); setDashboardData(fresh); stopPolling(); setLoading(false);
          } catch { /* not ready yet */ }
        }
      } catch (e) { stopPolling(); setError((e as Error).message); setLoading(false); }
    }, POLL_INTERVAL_MS);
  }, [stopPolling]);

  useEffect(() => {
    const cached = lsLoad();
    if (cached) {
      setDashboardData(cached); setInitialLoading(false);
      fetchDashboardData().then((f) => { lsSave(f); setDashboardData(f); }).catch(() => {});
      return stopPolling;
    }
    fetchDashboardData()
      .then((d) => { lsSave(d); setDashboardData(d); setInitialLoading(false); })
      .catch(() => {
        setInitialLoading(false); setLoading(true);
        triggerPipeline()
          .then(({ jobRunId }) => startPipelinePolling(jobRunId))
          .catch(() => startPipelinePolling(null));
      });
    return stopPolling;
  }, [stopPolling, startPipelinePolling]);

  const handleRefresh = useCallback(async () => {
    if (loading) return;
    setLoading(true); setError(null); lsClear();
    let jobRunId: string | null = null;
    try { const r = await triggerPipeline(); jobRunId = r.jobRunId; } catch {}
    startPipelinePolling(jobRunId);
  }, [loading, startPipelinePolling]);

  const kpis = dashboardData?.kpis;

  return (
    <div style={{ display: "flex", minHeight: "100vh", backgroundColor: THEME.background }}>

      {/* ── Left sidebar ─────────────────────────────────────────────── */}
      <aside style={{
        width: 220,
        minHeight: "100vh",
        backgroundColor: THEME.surface,
        borderRight: `1px solid ${THEME.border}`,
        display: "flex",
        flexDirection: "column",
        padding: "28px 16px",
        flexShrink: 0,
        position: "sticky",
        top: 0,
        alignSelf: "flex-start",
        height: "100vh",
        overflowY: "auto",
      }}>
        {/* Logo */}
        <div style={{ marginBottom: 24 }}>
          <div style={{ fontSize: 36, lineHeight: 1, marginBottom: 8 }}>🛢</div>
          <h1 style={{ color: THEME.textPrimary, fontSize: 15, fontWeight: 700, lineHeight: 1.3, margin: 0 }}>
            Oil Price<br />Dashboard
          </h1>
          <p style={{ color: THEME.textSecondary, fontSize: 11, marginTop: 6, lineHeight: 1.5 }}>
            Brent Crude · IPEA<br />AWS Glue · Athena

          </p>
        </div>

        {/* KPI cards */}
        {kpis ? (
          <>
            <SideKpi label="Current Price" value={usd(kpis.currentPrice)} delta={kpis.deltaPercent} />
            <SideKpi label="7d Moving Avg" value={usd(kpis.ma7d)} />
            <SideKpi label="52W High" value={usd(kpis.high52w)} />
            <SideKpi label="52W Low" value={usd(kpis.low52w)} />
          </>
        ) : (
          <div style={{ color: THEME.textSecondary, fontSize: 12, marginBottom: 12 }}>
            {loading ? "Loading..." : "No data"}
          </div>
        )}

        <div style={{ flex: 1 }} />

        {/* Error */}
        {error && (
          <div style={{ backgroundColor: "#3b0000", borderRadius: 6, padding: "10px 12px", marginBottom: 14, color: THEME.negative, fontSize: 11, lineHeight: 1.5 }}>
            {error}
          </div>
        )}

        <RefreshButton onPress={handleRefresh} loading={loading} />

        {dashboardData?.queryId && (
          <div style={{ marginTop: 14, color: THEME.textSecondary, fontSize: 9, fontFamily: "monospace", opacity: 0.5 }}
               title="Athena query execution ID">
            {dashboardData.queryId.slice(0, 8)}
          </div>
        )}
      </aside>

      {/* ── Main content ─────────────────────────────────────────────── */}
      <main style={{ flex: 1, padding: "28px 32px", minWidth: 0 }}>
        {initialLoading ? (
          <div style={{ display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", height: "60vh", gap: 16, color: THEME.textSecondary }}>
            <span className="spinner" style={{ width: 32, height: 32, borderWidth: 3 }} />
            <span style={{ fontSize: 14 }}>Loading data...</span>
          </div>
        ) : dashboardData ? (
          <>
            <PriceChart series={dashboardData.series} />
            <MonthlyBarChart monthlyAvg={dashboardData.monthlyAvg} />
          </>
        ) : (
          <div style={{ textAlign: "center", color: THEME.textSecondary, paddingTop: "20vh", fontSize: 14, lineHeight: 1.8 }}>
            {loading ? "Running pipeline, please wait..." : "No data available."}
          </div>
        )}
      </main>
    </div>
  );
}
