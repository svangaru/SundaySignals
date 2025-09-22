
import { requireTriggerToken } from '@/lib/utils';

export async function POST(req: Request) {
  const unauthorized = requireTriggerToken(req.headers);
  if (unauthorized) return unauthorized;

  const { season, week } = await req.json().catch(() => ({ season: undefined, week: undefined }));
  // TODO: compute EV over replacement and upsert waiver_suggestions
  return new Response(JSON.stringify({ ok: true, season, week }), { status: 200 });
}
