import { THEME } from "../theme";

interface RefreshButtonProps {
  onPress: () => void;
  loading: boolean;
}

export function RefreshButton({ onPress, loading }: RefreshButtonProps) {
  return (
    <button
      onClick={onPress}
      disabled={loading}
      style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        gap: 10,
        backgroundColor: THEME.accent,
        color: "#ffffff",
        border: "none",
        borderRadius: 8,
        padding: "12px 32px",
        fontSize: 15,
        fontWeight: 600,
        cursor: loading ? "not-allowed" : "pointer",
        opacity: loading ? 0.65 : 1,
        transition: "opacity 0.2s",
        alignSelf: "flex-start",
      }}
    >
      {loading && <span className="spinner" />}
      {loading ? "Processing..." : "Refresh Data"}
    </button>
  );
}
