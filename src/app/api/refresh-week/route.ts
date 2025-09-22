
import { requireTriggerToken } from '@/src/lib/utils';

export async function POST(req: Request) {
  const unauthorized = requireTriggerToken(req.headers);
  if (unauthorized) return unauthorized;

  // TODO: refresh odds/news/injuries/DVP sources and upsert tables
  return new Response(JSON.stringify({ ok: true }), { status: 200 });
}
