export function shareURLWithPassword(rawURL: string, rawPassword?: string) {
  const url = rawURL.trim();
  const password = rawPassword?.trim();
  if (!url || !password) return url;

  try {
    const parsed = new URL(url);
    parsed.searchParams.set("pwd", password);
    return parsed.toString();
  } catch {
    const separator = url.includes("?") ? "&" : "?";
    return `${url}${separator}pwd=${encodeURIComponent(password)}`;
  }
}

export function shareCopyText(url: string, password?: string) {
  return shareURLWithPassword(url, password);
}
