import type { KpiData } from "../types";
import { KpiCard } from "./KpiCard";

interface KpiRowProps {
  kpis: KpiData;
}

function usd(v: number | null | undefined): string {
  if (v === null || v === undefined) return "N/A";
  return `$${v.toFixed(2)}`;
}

export function KpiRow({ kpis }: KpiRowProps) {
  return (
    <div style={{ display: "flex", flexWrap: "wrap", gap: 12, marginBottom: 28 }}>
      <KpiCard
        label="Preço Atual"
        value={usd(kpis.currentPrice)}
        delta={kpis.deltaPercent}
      />
      <KpiCard label="Média Móvel 7d" value={usd(kpis.ma7d)} />
      <KpiCard label="Máx. 52 sem." value={usd(kpis.high52w)} />
      <KpiCard label="Mín. 52 sem." value={usd(kpis.low52w)} />
    </div>
  );
}
