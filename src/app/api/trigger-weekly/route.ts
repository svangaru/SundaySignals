
import { requireTriggerToken } from '@/src/lib/utils';

export async function POST(req: Request) {
  const unauthorized = requireTriggerToken(req.headers);
  if (unauthorized) return unauthorized;

  const { season, week } = await req.json().catch(() => ({ season: undefined, week: undefined }));

  // TODO: Kick Modal pipeline via webhook or separate scheduler call.
  return new Response(JSON.stringify({ ok: true, season, week }), { status: 200 });
}
