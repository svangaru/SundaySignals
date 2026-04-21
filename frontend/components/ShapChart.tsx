import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Cell,
  ReferenceLine,
  ResponsiveContainer,
} from 'recharts'
import { FEATURE_LABELS } from '@/lib/types'

interface Props {
  shap: Record<string, number>
}

function label(key: string): string {
  return FEATURE_LABELS[key] ?? key.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase())
}

export default function ShapChart({ shap }: Props) {
  const entries = Object.entries(shap)
    .map(([feature, value]) => ({ feature, value, label: label(feature) }))
    .sort((a, b) => Math.abs(b.value) - Math.abs(a.value))
    .slice(0, 12)

  const domain = Math.max(...entries.map((e) => Math.abs(e.value))) * 1.1

  return (
    <ResponsiveContainer width="100%" height={entries.length * 30 + 40}>
      <BarChart
        layout="vertical"
        data={entries}
        margin={{ top: 4, right: 16, left: 120, bottom: 4 }}
      >
        <CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="#f0f0f0" />
        <XAxis
          type="number"
          domain={[-domain, domain]}
          tick={{ fontSize: 11 }}
          tickFormatter={(v) => v.toFixed(2)}
        />
        <YAxis
          type="category"
          dataKey="label"
          tick={{ fontSize: 12 }}
          width={116}
        />
        <Tooltip
          formatter={(value: number) => [value.toFixed(3), 'SHAP value']}
          labelFormatter={(l) => `Feature: ${l}`}
        />
        <ReferenceLine x={0} stroke="#d1d5db" />
        <Bar dataKey="value" radius={[0, 3, 3, 0]}>
          {entries.map((entry) => (
            <Cell
              key={entry.feature}
              fill={entry.value >= 0 ? '#22c55e' : '#ef4444'}
              fillOpacity={0.8}
            />
          ))}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  )
}
