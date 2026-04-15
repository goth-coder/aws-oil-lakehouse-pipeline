export interface ChartPoint {
  date: string;                 // "YYYY-MM-DD"
  price_usd: number;            // USD/barril
  moving_avg_7d: number | null; // null nos primeiros 6 registros da série
}

export interface KpiData {
  currentPrice: number;         // Athena: MAX(price_usd) WHERE date = MAX(date)
  deltaPercent: number;         // Lambda: (currentPrice / prevPrice - 1) * 100
  ma7d: number | null;          // Athena: moving_avg_7d do dia mais recente
  high52w: number;              // Athena: MAX(price_usd) WHERE date >= -365 dias
  low52w: number;               // Athena: MIN(price_usd) WHERE date >= -365 dias
}

export interface MonthlyAvg {
  month: string; // "YYYY-MM" — Athena: CONCAT(year, '-', LPAD(month,2,'0'))
  avg: number;   // Athena: AVG(price_usd) GROUP BY year, month
}

export interface DashboardData {
  kpis: KpiData;
  series: ChartPoint[];
  monthlyAvg: MonthlyAvg[];
  queryId?: string; // Athena execution ID used to build this payload
}

export interface PipelineStatus {
  status: "RUNNING" | "SUCCEEDED" | "FAILED";
  error?: string;
}
