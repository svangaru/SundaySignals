import { useState, useRef } from 'react'
import { FEATURE_LABELS } from '@/lib/types'

interface Props {
  features: string[]
  onRescore: (weights: Record<string, number>) => void
  disabled?: boolean
}

const DEFAULT_WEIGHT = 1.0
const MIN_WEIGHT     = 0.0
const MAX_WEIGHT     = 2.0
const STEP           = 0.1

function label(key: string): string {
  return FEATURE_LABELS[key] ?? key.replace(/_/g, ' ')
}

export default function WeightSliders({ features, onRescore, disabled }: Props) {
  const [weights, setWeights] = useState<Record<string, number>>(
    Object.fromEntries(features.map((f) => [f, DEFAULT_WEIGHT]))
  )
  const [changed, setChanged] = useState(false)

  function handleChange(feature: string, value: number) {
    setWeights((prev) => ({ ...prev, [feature]: value }))
    setChanged(true)
  }

  function handleRescore() {
    setChanged(false)
    onRescore(weights)
  }

  function handleReset() {
    const reset = Object.fromEntries(features.map((f) => [f, DEFAULT_WEIGHT]))
    setWeights(reset)
    setChanged(false)
    onRescore(reset)
  }

  return (
    <div className="space-y-3">
      <div className="max-h-72 overflow-y-auto pr-1 space-y-2">
        {features.map((feature) => {
          const w = weights[feature] ?? DEFAULT_WEIGHT
          return (
            <div key={feature}>
              <div className="flex justify-between text-xs text-gray-600 mb-0.5">
                <span>{label(feature)}</span>
                <span className={`font-mono font-semibold ${w === 1 ? 'text-gray-400' : w > 1 ? 'text-green-600' : 'text-red-500'}`}>
                  {w.toFixed(1)}×
                </span>
              </div>
              <input
                type="range"
                min={MIN_WEIGHT}
                max={MAX_WEIGHT}
                step={STEP}
                value={w}
                disabled={disabled}
                onChange={(e) => handleChange(feature, parseFloat(e.target.value))}
                className="w-full h-1.5 accent-indigo-500 cursor-pointer disabled:cursor-not-allowed disabled:opacity-40"
              />
            </div>
          )
        })}
      </div>

      <div className="flex gap-2 pt-1">
        <button
          onClick={handleRescore}
          disabled={disabled || !changed}
          className="flex-1 bg-indigo-600 text-white text-xs font-semibold py-2 rounded-md hover:bg-indigo-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
        >
          {disabled ? 'Scoring…' : 'Re-score'}
        </button>
        <button
          onClick={handleReset}
          disabled={disabled}
          className="px-3 py-2 text-xs text-gray-500 border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-40 transition-colors"
        >
          Reset
        </button>
      </div>
    </div>
  )
}
