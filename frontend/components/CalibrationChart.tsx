'use client'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ReferenceLine,
} from 'recharts'
import type { ModelPerformance } from '@/lib/types'

interface Props {
  rows: ModelPerformance[]
}

interface ChartRow {
  split: string
  'AUC-ROC': number
  'P@20': number
  'Comp Acc.': number
}

export default function CalibrationChart({ rows }: Props) {
  if (rows.length === 0) return null

  const data: ChartRow[] = rows.map((r) => ({
    split:        `${r.test_season}`,
    'AUC-ROC':    parseFloat((r.auc_roc         ?? 0).toFixed(3)),
    'P@20':       parseFloat((r.precision_at_20  ?? 0).toFixed(3)),
    'Comp Acc.':  parseFloat((r.comp_accuracy    ?? 0).toFixed(3)),
  }))

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-5">
      <p className="text-xs text-gray-400 mb-4">Metrics by test season (0–1 scale)</p>
      <ResponsiveContainer width="100%" height={220}>
        <BarChart data={data} margin={{ top: 4, right: 16, left: 0, bottom: 4 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
          <XAxis dataKey="split" tick={{ fontSize: 12 }} />
          <YAxis domain={[0, 1]} tick={{ fontSize: 11 }} />
          <Tooltip formatter={(v: number) => v.toFixed(3)} />
          <Legend wrapperStyle={{ fontSize: 12 }} />
          <ReferenceLine y={0.5} stroke="#d1d5db" strokeDasharray="4 2" label={{ value: '0.5 baseline', fontSize: 10, fill: '#9ca3af' }} />
          <Bar dataKey="AUC-ROC"   fill="#6366f1" fillOpacity={0.85} radius={[3, 3, 0, 0]} />
          <Bar dataKey="P@20"      fill="#22c55e" fillOpacity={0.85} radius={[3, 3, 0, 0]} />
          <Bar dataKey="Comp Acc." fill="#f59e0b" fillOpacity={0.85} radius={[3, 3, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
