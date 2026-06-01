import { useEffect, useMemo, useState, type ReactNode } from "react";
import { CheckCircle2, KeyRound, Play, Plus, RefreshCw, Save, XCircle } from "lucide-react";
import { Alert, AlertDescription } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { Label } from "~/components/ui/label";
import { Separator } from "~/components/ui/separator";
import { Switch } from "~/components/ui/switch";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "~/components/ui/table";
import { Textarea } from "~/components/ui/textarea";
import { api, errorMessage } from "~/lib/api";
import type { LLMProvider, LLMProviderInput, LLMProviderTestResponse } from "~/types/api";

type ProviderForm = {
  slug: string;
  displayName: string;
  baseURL: string;
  model: string;
  apiKey: string;
  enabled: boolean;
  isDefault: boolean;
  translation: boolean;
  domainDiscovery: boolean;
};

const emptyForm: ProviderForm = {
  slug: "",
  displayName: "",
  baseURL: "",
  model: "",
  apiKey: "",
  enabled: false,
  isDefault: false,
  translation: true,
  domainDiscovery: true,
};

export function LLMProviderManagement() {
  const [providers, setProviders] = useState<LLMProvider[]>([]);
  const [selectedID, setSelectedID] = useState<string>("");
  const [form, setForm] = useState<ProviderForm>(emptyForm);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [testPrompt, setTestPrompt] = useState("Reply with one short sentence confirming that this LLM provider works.");
  const [testMaxTokens, setTestMaxTokens] = useState("128");
  const [testTimeoutSeconds, setTestTimeoutSeconds] = useState("30");
  const [testResult, setTestResult] = useState<LLMProviderTestResponse | null>(null);

  const selectedProvider = useMemo(
    () => providers.find((provider) => provider.id === selectedID) ?? null,
    [providers, selectedID],
  );

  useEffect(() => {
    void loadProviders();
  }, []);

  async function loadProviders() {
    setLoading(true);
    setError(null);
    try {
      const response = await api.getLLMProviders();
      setProviders(response.providers);
      if (!selectedID && response.providers.length > 0) {
        selectProvider(response.providers[0]);
      }
    } catch (err) {
      setError(errorMessage(err, "Failed to load LLM providers"));
    } finally {
      setLoading(false);
    }
  }

  function selectProvider(provider: LLMProvider) {
    setSelectedID(provider.id);
    setForm({
      slug: provider.slug,
      displayName: provider.display_name,
      baseURL: provider.base_url,
      model: provider.model,
      apiKey: "",
      enabled: provider.enabled,
      isDefault: provider.is_default,
      translation: Boolean(provider.capabilities?.translation ?? true),
      domainDiscovery: Boolean(provider.capabilities?.domain_discovery ?? true),
    });
    setMessage(null);
    setError(null);
    setTestResult(null);
  }

  function startNewProvider() {
    setSelectedID("");
    setForm(emptyForm);
    setMessage(null);
    setError(null);
    setTestResult(null);
  }

  async function saveProvider() {
    setSaving(true);
    setError(null);
    setMessage(null);
    try {
      const body = providerInputFromForm(form);
      const saved = selectedID
        ? await api.updateLLMProvider(selectedID, body)
        : await api.createLLMProvider(body);
      setMessage("Provider saved");
      setForm((current) => ({ ...current, apiKey: "" }));
      const response = await api.getLLMProviders();
      setProviders(response.providers);
      const nextSelected = response.providers.find((provider) => provider.id === saved.id);
      if (nextSelected) selectProvider(nextSelected);
    } catch (err) {
      setError(errorMessage(err, "Failed to save LLM provider"));
    } finally {
      setSaving(false);
    }
  }

  async function setAsDefault() {
    if (!selectedProvider) return;
    setSaving(true);
    setError(null);
    setMessage(null);
    try {
      const saved = await api.setDefaultLLMProvider(selectedProvider.id);
      setMessage("Default provider updated");
      const response = await api.getLLMProviders();
      setProviders(response.providers);
      const nextSelected = response.providers.find((provider) => provider.id === saved.id);
      if (nextSelected) selectProvider(nextSelected);
    } catch (err) {
      setError(errorMessage(err, "Failed to set default provider"));
    } finally {
      setSaving(false);
    }
  }

  async function runProviderTest() {
    if (!selectedProvider) return;
    setTesting(true);
    setTestResult(null);
    setError(null);
    try {
      const result = await api.testLLMProvider(selectedProvider.id, {
        prompt: testPrompt.trim(),
        max_tokens: Number(testMaxTokens) || 128,
        timeout_seconds: Number(testTimeoutSeconds) || 30,
      });
      setTestResult(result);
    } catch (err) {
      setTestResult({
        status: "failed",
        provider_id: selectedProvider.id,
        provider_slug: selectedProvider.slug,
        model: selectedProvider.model,
        response_text: "",
        duration_ms: 0,
        error: { code: "request_failed", message: errorMessage(err, "Provider test failed") },
      });
    } finally {
      setTesting(false);
    }
  }

  return (
    <div className="flex flex-col gap-5">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-xl font-semibold">LLM Providers</h1>
          <p className="text-sm text-muted-foreground">Manage provider records and verify access from Corpscout.</p>
        </div>
        <div className="flex gap-2">
          <Button variant="outline" onClick={loadProviders} disabled={loading}>
            <RefreshCw className="size-4" />
            Refresh
          </Button>
          <Button onClick={startNewProvider}>
            <Plus className="size-4" />
            New provider
          </Button>
        </div>
      </div>

      {error && (
        <Alert variant="destructive">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}
      {message && (
        <Alert>
          <AlertDescription>{message}</AlertDescription>
        </Alert>
      )}

      <div className="grid gap-5 xl:grid-cols-[minmax(0,1.15fr)_minmax(420px,0.85fr)]">
        <section className="rounded-lg border">
          <div className="flex items-center justify-between border-b px-4 py-3">
            <h2 className="text-sm font-semibold">Providers</h2>
            <Badge variant="outline">{providers.length}</Badge>
          </div>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Model</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Key</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {providers.map((provider) => (
                <TableRow
                  key={provider.id}
                  data-state={provider.id === selectedID ? "selected" : undefined}
                  className="cursor-pointer"
                  onClick={() => selectProvider(provider)}
                >
                  <TableCell>
                    <div className="flex flex-col gap-1">
                      <span className="font-medium">{provider.display_name}</span>
                      <span className="text-xs text-muted-foreground">{provider.slug}</span>
                    </div>
                  </TableCell>
                  <TableCell>{provider.model}</TableCell>
                  <TableCell>
                    <div className="flex gap-1">
                      <Badge variant={provider.enabled ? "default" : "outline"}>
                        {provider.enabled ? "Enabled" : "Disabled"}
                      </Badge>
                      {provider.is_default && <Badge variant="secondary">Default</Badge>}
                    </div>
                  </TableCell>
                  <TableCell>
                    {provider.has_api_key ? (
                      <span className="inline-flex items-center gap-1 text-xs">
                        <KeyRound className="size-3" />
                        Stored
                      </span>
                    ) : (
                      <span className="text-xs text-muted-foreground">None</span>
                    )}
                  </TableCell>
                </TableRow>
              ))}
              {!loading && providers.length === 0 && (
                <TableRow>
                  <TableCell colSpan={4} className="h-24 text-center text-muted-foreground">
                    No providers
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </section>

        <section className="flex flex-col gap-5">
          <div className="rounded-lg border">
            <div className="border-b px-4 py-3">
              <h2 className="text-sm font-semibold">{selectedID ? "Edit Provider" : "New Provider"}</h2>
            </div>
            <div className="grid gap-4 p-4">
              <div className="grid gap-4 sm:grid-cols-2">
                <Field label="Display name" id="llm-display-name">
                  <Input
                    id="llm-display-name"
                    value={form.displayName}
                    onChange={(event) => setForm({ ...form, displayName: event.target.value })}
                  />
                </Field>
                <Field label="Slug" id="llm-slug">
                  <Input
                    id="llm-slug"
                    value={form.slug}
                    disabled={Boolean(selectedID)}
                    onChange={(event) => setForm({ ...form, slug: event.target.value })}
                  />
                </Field>
              </div>

              <Field label="Base URL" id="llm-base-url">
                <Input
                  id="llm-base-url"
                  value={form.baseURL}
                  placeholder="https://api.example.com"
                  onChange={(event) => setForm({ ...form, baseURL: event.target.value })}
                />
              </Field>

              <div className="grid gap-4 sm:grid-cols-2">
                <Field label="Model" id="llm-model">
                  <Input
                    id="llm-model"
                    value={form.model}
                    onChange={(event) => setForm({ ...form, model: event.target.value })}
                  />
                </Field>
                <Field label="API key" id="llm-api-key">
                  <Input
                    id="llm-api-key"
                    type="password"
                    value={form.apiKey}
                    placeholder={selectedProvider?.has_api_key ? "Stored key unchanged" : ""}
                    onChange={(event) => setForm({ ...form, apiKey: event.target.value })}
                  />
                </Field>
              </div>

              <div className="grid gap-3 sm:grid-cols-2">
                <ToggleRow label="Enabled" checked={form.enabled} onCheckedChange={(enabled) => setForm({ ...form, enabled })} />
                <ToggleRow label="Default" checked={form.isDefault} onCheckedChange={(isDefault) => setForm({ ...form, isDefault })} />
                <ToggleRow label="Translation" checked={form.translation} onCheckedChange={(translation) => setForm({ ...form, translation })} />
                <ToggleRow label="Domain discovery" checked={form.domainDiscovery} onCheckedChange={(domainDiscovery) => setForm({ ...form, domainDiscovery })} />
              </div>

              <div className="flex justify-end gap-2">
                {selectedProvider && !selectedProvider.is_default && (
                  <Button variant="outline" onClick={setAsDefault} disabled={saving}>
                    <CheckCircle2 className="size-4" />
                    Set default
                  </Button>
                )}
                <Button onClick={saveProvider} disabled={saving}>
                  <Save className="size-4" />
                  {saving ? "Saving..." : "Save provider"}
                </Button>
              </div>
            </div>
          </div>

          <div className="rounded-lg border">
            <div className="border-b px-4 py-3">
              <h2 className="text-sm font-semibold">Test Provider</h2>
            </div>
            <div className="grid gap-4 p-4">
              <Field label="Prompt" id="llm-test-prompt">
                <Textarea
                  id="llm-test-prompt"
                  value={testPrompt}
                  className="min-h-28"
                  onChange={(event) => setTestPrompt(event.target.value)}
                />
              </Field>
              <div className="grid gap-4 sm:grid-cols-2">
                <Field label="Max tokens" id="llm-test-max-tokens">
                  <Input
                    id="llm-test-max-tokens"
                    type="number"
                    min={1}
                    value={testMaxTokens}
                    onChange={(event) => setTestMaxTokens(event.target.value)}
                  />
                </Field>
                <Field label="Timeout seconds" id="llm-test-timeout">
                  <Input
                    id="llm-test-timeout"
                    type="number"
                    min={1}
                    value={testTimeoutSeconds}
                    onChange={(event) => setTestTimeoutSeconds(event.target.value)}
                  />
                </Field>
              </div>

              <div className="flex justify-end">
                <Button onClick={runProviderTest} disabled={!selectedProvider || testing}>
                  <Play className="size-4" />
                  {testing ? "Running..." : "Run test"}
                </Button>
              </div>

              {testResult && (
                <>
                  <Separator />
                  <div className="grid gap-3">
                    <div className="flex items-center gap-2">
                      {testResult.status === "succeeded" ? (
                        <CheckCircle2 className="size-4 text-emerald-600" />
                      ) : (
                        <XCircle className="size-4 text-destructive" />
                      )}
                      <Badge variant={testResult.status === "succeeded" ? "default" : "destructive"}>
                        {testResult.status}
                      </Badge>
                      <span className="text-xs text-muted-foreground">{testResult.duration_ms} ms</span>
                    </div>
                    {testResult.response_text && (
                      <pre className="max-h-72 overflow-auto rounded-md bg-muted p-3 text-sm whitespace-pre-wrap">
                        {testResult.response_text}
                      </pre>
                    )}
                    {testResult.error && (
                      <Alert variant="destructive">
                        <AlertDescription>
                          {testResult.error.code}: {testResult.error.message}
                        </AlertDescription>
                      </Alert>
                    )}
                  </div>
                </>
              )}
            </div>
          </div>
        </section>
      </div>
    </div>
  );
}

function Field({ id, label, children }: { id: string; label: string; children: ReactNode }) {
  return (
    <div className="flex flex-col gap-2">
      <Label htmlFor={id}>{label}</Label>
      {children}
    </div>
  );
}

function ToggleRow({
  label,
  checked,
  onCheckedChange,
}: {
  label: string;
  checked: boolean;
  onCheckedChange: (checked: boolean) => void;
}) {
  return (
    <div className="flex h-10 items-center justify-between rounded-md border px-3">
      <span className="text-sm">{label}</span>
      <Switch checked={checked} onCheckedChange={onCheckedChange} />
    </div>
  );
}

function providerInputFromForm(form: ProviderForm): LLMProviderInput {
  return {
    slug: form.slug.trim(),
    display_name: form.displayName.trim(),
    provider_type: "openai_compatible",
    base_url: form.baseURL.trim(),
    model: form.model.trim(),
    api_key: form.apiKey.trim() || undefined,
    enabled: form.enabled,
    is_default: form.isDefault,
    capabilities: {
      translation: form.translation,
      domain_discovery: form.domainDiscovery,
    },
    metadata: {},
  };
}
