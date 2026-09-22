export type BrowserTab = {
  id: string;
  url: string;
  title: string;
  status_code: number | null;
};

export type SavedBrowserSession = {
  id: string;
  state: "closed" | "stopped" | "starting" | "running" | "error" | "stopping";
  headless?: boolean;
  desktop_available?: boolean;
  pinned: boolean;
  retained_until: number;
  label: string;
  execution_id: string | null;
  request_id: string | null;
  lease_id: string | null;
  domain: string | null;
  generation: string | null;
  saved_at: string | null;
  error: string | null;
  tabs: BrowserTab[];
};

export type BrowserInspection = {
  url: string;
  title: string;
  status_code: number | null;
  access_problem: "blocked" | "captcha" | null;
  checked_at: string;
};
