import { THEME } from "../theme";

interface KpiCardProps {
  label: string;
  value: string;
  delta?: number;
}

export function KpiCard({ label, value, delta }: KpiCardProps) {
  const hasDelta = delta !== undefined && delta !== null;
  const isPositive = hasDelta && delta >= 0;

  return (
    <div
      style={{
        backgroundColor: THEME.surface,
        border: `1px solid ${THEME.border}`,
        borderRadius: 12,
        padding: "16px 20px",
        flex: "1 1 180px",
        minWidth: 160,
      }}
    >
      <div
        style={{
          color: THEME.textSecondary,
          fontSize: 12,
          marginBottom: 6,
          textTransform: "uppercase",
          letterSpacing: "0.04em",
        }}
      >
        {label}
      </div>
      <div
        style={{ color: THEME.textPrimary, fontSize: 22, fontWeight: "bold" }}
      >
        {value}
      </div>
      {hasDelta && (
        <div
          style={{
            color: isPositive ? THEME.positive : THEME.negative,
            fontSize: 13,
            marginTop: 6,
          }}
        >
          {isPositive ? "↑" : "↓"} {Math.abs(delta).toFixed(2)}%
        </div>
      )}
    </div>
  );
}
