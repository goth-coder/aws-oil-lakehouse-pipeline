import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { THEME } from "../theme";
import type { MonthlyAvg } from "../types";

const MONTH_ABBR: Record<string, string> = {
  "01": "Jan", "02": "Feb", "03": "Mar", "04": "Apr",
  "05": "May", "06": "Jun", "07": "Jul", "08": "Aug",
  "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dec",
};

interface MonthlyBarChartProps {
  monthlyAvg: MonthlyAvg[];
}

export function MonthlyBarChart({ monthlyAvg }: MonthlyBarChartProps) {
  const data = monthlyAvg.map((m) => ({
    ...m,
    label: MONTH_ABBR[m.month.split("-")[1] ?? ""] ?? m.month,
  }));

  return (
    <div style={{ marginBottom: 32 }}>
      <div
        style={{
          color: THEME.textPrimary,
          fontSize: 14,
          fontWeight: 600,
          marginBottom: 12,
        }}
      >
        Monthly Average — Last 12 months
      </div>

      <ResponsiveContainer width="100%" height={200}>
        <BarChart
          data={data}
          margin={{ top: 5, right: 16, bottom: 5, left: 10 }}
        >
          <CartesianGrid
            strokeDasharray="3 3"
            stroke={THEME.border}
            vertical={false}
          />
          <XAxis
            dataKey="label"
            tick={{ fill: THEME.textSecondary, fontSize: 11 }}
            tickLine={false}
            axisLine={{ stroke: THEME.border }}
          />
          <YAxis
            tick={{ fill: THEME.textSecondary, fontSize: 11 }}
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
            formatter={(value: unknown) => [
              `$${Number(value).toFixed(2)}`,
              "Avg",
            ]}
          />
          <Bar
            dataKey="avg"
            fill={THEME.accent}
            radius={[4, 4, 0, 0]}
            barSize={28}
            isAnimationActive={false}
          />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
