import type { ModelPerformance } from '@/lib/types'

interface Props {
  rows: ModelPerformance[]
}

function fmt(n: number | null, decimals = 3): string {
  if (n == null) return '—'
  return n.toFixed(decimals)
}

function aucColor(auc: number | null): string {
  if (auc == null) return 'text-gray-400'
  if (auc >= 0.70) return 'text-green-700 font-semibold'
  if (auc >= 0.60) return 'text-amber-700 font-semibold'
  return 'text-red-600'
}

export default function AucTable({ rows }: Props) {
  return (
    <div className="overflow-x-auto rounded-xl border border-gray-200 bg-white">
      <table className="min-w-full text-sm">
        <thead>
          <tr className="border-b border-gray-100 bg-gray-50 text-left text-xs font-semibold uppercase tracking-wide text-gray-500">
            <th className="px-4 py-3">Split</th>
            <th className="px-4 py-3">AUC-ROC</th>
            <th className="px-4 py-3">P@20</th>
            <th className="px-4 py-3">Cal. Error</th>
            <th className="px-4 py-3">Comp Acc.</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-50">
          {rows.map((r) => (
            <tr key={`${r.model_type}-${r.test_season}`} className="hover:bg-gray-50">
              <td className="px-4 py-3 text-gray-700">
                Train ≤{r.train_max_season} → Test {r.test_season}
              </td>
              <td className={`px-4 py-3 font-mono ${aucColor(r.auc_roc)}`}>
                {fmt(r.auc_roc)}
              </td>
              <td className="px-4 py-3 font-mono text-gray-700">{fmt(r.precision_at_20)}</td>
              <td className="px-4 py-3 font-mono text-gray-700">{fmt(r.calibration_error)}</td>
              <td className="px-4 py-3 font-mono text-gray-700">{fmt(r.comp_accuracy)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
