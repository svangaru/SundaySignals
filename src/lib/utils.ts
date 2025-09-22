
export function requireTriggerToken(headers: Headers) {
  const token = headers.get('x-trigger-token');
  if (!token || token !== process.env.TRIGGER_TOKEN) {
    return new Response('Unauthorized', { status: 401 });
  }
  return null;
}
