/** Absolute webhook URLs for the editor origin and optional Cloudflare public hostname. */
export function buildWebhookTriggerUrls(
  slug: string,
  publicHostname: string | undefined | null,
): { localUrl: string; publicUrl: string | null } {
  const trimmed = slug.trim();
  if (!trimmed) {
    return { localUrl: "", publicUrl: null };
  }
  const path = `/api/webhooks/${encodeURIComponent(trimmed)}`;
  const localUrl =
    typeof window !== "undefined" ? new URL(path, window.location.origin).href : path;
  const host = String(publicHostname ?? "").trim();
  if (!host.length) {
    return { localUrl, publicUrl: null };
  }
  const clean = host.replace(/^https?:\/\//, "").split("/")[0];
  return { localUrl, publicUrl: `https://${clean}${path}` };
}
