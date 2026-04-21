import type { GetServerSideProps, NextPage } from 'next'
import { supabase } from '@/lib/supabase'
import type { ModelPerformance } from '@/lib/types'
import AucTable from '@/components/AucTable'
import CalibrationChart from '@/components/CalibrationChart'

interface Props {
  rows: ModelPerformance[]
}

export default function AccuracyPage({ rows }: Props) {
  const seasonal = rows.filter((r) => r.model_type === 'seasonal')
  const weekly   = rows.filter((r) => r.model_type === 'weekly')

  return (
    <div>
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-gray-900">Model Accuracy</h1>
        <p className="text-sm text-gray-500 mt-1">
          Walk-forward backtest results across three season splits
        </p>
      </div>

      {rows.length === 0 ? (
        <div className="text-center py-20 text-gray-400">
          No backtest results yet. Run <code className="font-mono text-sm">backtest.py</code> to populate data.
        </div>
      ) : (
        <div className="space-y-10">
          {seasonal.length > 0 && (
            <section>
              <h2 className="text-lg font-semibold text-gray-800 mb-3">Pre-Season Model</h2>
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <AucTable rows={seasonal} />
                <CalibrationChart rows={seasonal} />
              </div>
            </section>
          )}

          {weekly.length > 0 && (
            <section>
              <h2 className="text-lg font-semibold text-gray-800 mb-3">In-Season Model</h2>
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <AucTable rows={weekly} />
                <CalibrationChart rows={weekly} />
              </div>
            </section>
          )}
        </div>
      )}
    </div>
  )
}

export const getServerSideProps: GetServerSideProps<Props> = async () => {
  const { data } = await supabase
    .from('model_performance')
    .select('*')
    .order('test_season', { ascending: true })

  return { props: { rows: data ?? [] } }
}
