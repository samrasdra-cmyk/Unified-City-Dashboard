interface Props {
  label: string;
  value: string;
}

export default function KPICard({ label, value }: Props) {
  return (
    <div className="kpi-card">
      <div className="label">{label}</div>
      <div className="value">{value}</div>
    </div>
  );
}
