import type { NextApiRequest, NextApiResponse } from 'next'
import type { ScoreRequest, ScoreResponse } from '@/lib/types'

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse<ScoreResponse | { error: string }>
) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' })
  }

  const modalUrl = process.env.MODAL_INFERENCE_URL
  if (!modalUrl) {
    return res.status(500).json({ error: 'MODAL_INFERENCE_URL is not configured' })
  }

  const body: ScoreRequest = req.body

  if (!body.features || typeof body.features !== 'object') {
    return res.status(400).json({ error: 'features object is required' })
  }

  try {
    const upstream = await fetch(modalUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })

    if (!upstream.ok) {
      const text = await upstream.text()
      return res.status(upstream.status).json({ error: text })
    }

    const data: ScoreResponse = await upstream.json()
    return res.status(200).json(data)
  } catch (err) {
    console.error('Modal inference error:', err)
    return res.status(502).json({ error: 'Failed to reach inference service' })
  }
}
