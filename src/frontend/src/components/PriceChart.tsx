import { useMemo, useState } from "react";
import {
  Area,
  CartesianGrid,
  ComposedChart,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { THEME } from "../theme";
import type { ChartPoint } from "../types";

type Period = "1M" | "6M" | "1Y" | "5Y" | "All";
const PERIODS: Period[] = ["1M", "6M", "1Y", "5Y", "All"];

function cutoffDate(period: Period): Date | null {
  const now = new Date();
  switch (period) {
    case "1M":
      return new Date(now.getFullYear(), now.getMonth() - 1, now.getDate());
    case "6M":
      return new Date(now.getFullYear(), now.getMonth() - 6, now.getDate());
    case "1Y":
      return new Date(now.getFullYear() - 1, now.getMonth(), now.getDate());
    case "5Y":
      return new Date(now.getFullYear() - 5, now.getMonth(), now.getDate());
    case "All":
      return null;
  }
}

interface PriceChartProps {
  series: ChartPoint[];
}

export function PriceChart({ series }: PriceChartProps) {
  const [period, setPeriod] = useState<Period>("1Y");

  const data = useMemo(() => {
    const cut = cutoffDate(period);
    return cut ? series.filter((p) => new Date(p.date) >= cut) : series;
  }, [series, period]);

  return (
    <div style={{ marginBottom: 32 }}>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          marginBottom: 12,
          flexWrap: "wrap",
          gap: 8,
        }}
      >
        <span
          style={{ color: THEME.textPrimary, fontSize: 14, fontWeight: 600 }}
        >
          Historical Price (USD/barrel)
        </span>
        <div style={{ display: "flex", gap: 6 }}>
          {PERIODS.map((p) => (
            <button
              key={p}
              onClick={() => setPeriod(p)}
              style={{
                padding: "4px 12px",
                borderRadius: 16,
                border: "none",
                cursor: "pointer",
                backgroundColor: period === p ? THEME.accent : THEME.border,
                color: period === p ? "#ffffff" : THEME.textSecondary,
                fontSize: 12,
                fontWeight: period === p ? 600 : 400,
                transition: "background-color 0.15s",
              }}
            >
              {p}
            </button>
          ))}
        </div>
      </div>

      <ResponsiveContainer width="100%" height={260}>
        <ComposedChart
          data={data}
          margin={{ top: 5, right: 16, bottom: 5, left: 10 }}
        >
          <defs>
            <linearGradient id="priceGrad" x1="0" y1="0" x2="0" y2="1">
              <stop
                offset="5%"
                stopColor={THEME.accent}
                stopOpacity={0.15}
              />
              <stop
                offset="95%"
                stopColor={THEME.accent}
                stopOpacity={0}
              />
            </linearGradient>
          </defs>

          <CartesianGrid
            strokeDasharray="3 3"
            stroke={THEME.border}
            vertical={false}
          />
          <XAxis
            dataKey="date"
            tick={{ fill: THEME.textSecondary, fontSize: 10 }}
            tickLine={false}
            axisLine={{ stroke: THEME.border }}
            interval="preserveStartEnd"
            minTickGap={60}
          />
          <YAxis
            tick={{ fill: THEME.textSecondary, fontSize: 10 }}
            tickLine={false}
            axisLine={{ stroke: THEME.border }}
            domain={["auto", "auto"]}
            tickFormatter={(v: number) => `$${v}`}
            width={55}
          />
          <Tooltip
            contentStyle={{
              backgroundColor: THEME.surface,
              border: `1px solid ${THEME.border}`,
              borderRadius: 8,
              fontSize: 12,
            }}
            labelStyle={{ color: THEME.textSecondary, marginBottom: 4 }}
            formatter={(value: unknown, name: string) => [
              `$${Number(value).toFixed(2)}`,
              name === "price_usd" ? "Price" : "MA 7d",
            ]}
          />

          <Area
            dataKey="price_usd"
            fill="url(#priceGrad)"
            stroke={THEME.accent}
            strokeOpacity={0.4}
            strokeWidth={1.5}
            dot={false}
            isAnimationActive={false}
          />
          <Line
            dataKey="moving_avg_7d"
            stroke={THEME.accentLight}
            strokeWidth={2}
            dot={false}
            connectNulls={false}
            isAnimationActive={false}
          />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
}
