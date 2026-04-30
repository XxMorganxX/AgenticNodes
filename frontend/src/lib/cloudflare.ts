import type { CloudflareConfig } from "./types";

function resolveApiBaseUrl(): string {
  const configured = import.meta.env.VITE_API_BASE_URL;
  if (typeof configured === "string" && configured.trim().length > 0) {
    return configured.trim().replace(/\/$/, "");
  }
  if (import.meta.env.DEV) {
    return "";
  }
  return "http://127.0.0.1:8000";
}

const API_BASE_URL = resolveApiBaseUrl();

async function readFetchErrorMessage(response: Response, fallback: string): Promise<string> {
  const raw = await response.text();
  if (!raw.trim()) {
    return fallback;
  }
  try {
    const parsed = JSON.parse(raw) as { detail?: unknown };
    if (typeof parsed.detail === "string") {
      return parsed.detail;
    }
  } catch {
    // not JSON
  }
  return raw.trim() || fallback;
}

export async function fetchCloudflareConfig(): Promise<CloudflareConfig> {
  const response = await fetch(`${API_BASE_URL}/api/editor/integrations/cloudflare`, {
    cache: "no-store",
  });
  if (!response.ok) {
    throw new Error(await readFetchErrorMessage(response, "Failed to load Cloudflare configuration."));
  }
  return (await response.json()) as CloudflareConfig;
}

export async function saveCloudflareConfig(payload: {
  tunnel_token_env_var?: string;
  public_hostname?: string;
}): Promise<CloudflareConfig> {
  const response = await fetch(`${API_BASE_URL}/api/editor/integrations/cloudflare`, {
    method: "PUT",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    throw new Error(await readFetchErrorMessage(response, "Failed to save Cloudflare configuration."));
  }
  return (await response.json()) as CloudflareConfig;
}

export async function clearCloudflareConfig(): Promise<CloudflareConfig> {
  const response = await fetch(`${API_BASE_URL}/api/editor/integrations/cloudflare`, {
    method: "DELETE",
  });
  if (!response.ok) {
    throw new Error(await readFetchErrorMessage(response, "Failed to clear Cloudflare configuration."));
  }
  return (await response.json()) as CloudflareConfig;
}
