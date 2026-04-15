import type { DashboardData, PipelineStatus } from "../types";

export const API_BASE_URL = import.meta.env.VITE_API_BASE_URL as string;

async function _fetchJson<T>(
  url: string,
  options?: RequestInit,
): Promise<T> {
  const response = await fetch(url, options);
  const json = await response.json();

  if (!response.ok) {
    const message =
      (json as { error?: string }).error ??
      `HTTP ${response.status} ${response.statusText}`;
    throw new Error(message);
  }

  return json as T;
}

/** POST /pipeline/run — trigger scraping + Glue Job start */
export async function triggerPipeline(): Promise<{ jobRunId: string }> {
  return _fetchJson<{ jobRunId: string }>(`${API_BASE_URL}/pipeline/run`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({}),
  });
}

/** GET /pipeline/status/{jobRunId} — poll Glue Job status */
export async function pollStatus(jobRunId: string): Promise<PipelineStatus> {
  return _fetchJson<PipelineStatus>(
    `${API_BASE_URL}/pipeline/status/${encodeURIComponent(jobRunId)}`,
  );
}

/** GET /dashboard/data — fetch KPIs, series and monthly averages */
export async function fetchDashboardData(): Promise<DashboardData> {
  return _fetchJson<DashboardData>(`${API_BASE_URL}/dashboard/data`);
}
