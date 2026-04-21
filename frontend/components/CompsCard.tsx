import type { Comp } from '@/lib/types'

interface Props {
  comps: Comp[]
}

export default function CompsCard({ comps }: Props) {
  return (
    <ul className="space-y-2">
      {comps.map((comp, i) => (
        <li
          key={`${comp.player_id}-${comp.season}`}
          className="flex items-center justify-between text-sm py-1.5 border-b border-gray-50 last:border-0"
        >
          <div className="flex items-center gap-2">
            <span className="text-gray-400 font-mono text-xs w-4">{i + 1}</span>
            <div>
              <span className="font-medium text-gray-800">{comp.player_name}</span>
              <span className="text-gray-400 ml-1.5 text-xs">{comp.season}</span>
            </div>
          </div>
          <div className="flex items-center gap-3">
            <span className="text-xs text-gray-400">{(comp.similarity * 100).toFixed(0)}% match</span>
            {comp.broke_out ? (
              <span className="text-xs font-semibold text-green-600 bg-green-50 px-1.5 py-0.5 rounded">
                Broke out
              </span>
            ) : (
              <span className="text-xs font-semibold text-red-500 bg-red-50 px-1.5 py-0.5 rounded">
                Did not
              </span>
            )}
          </div>
        </li>
      ))}
    </ul>
  )
}
