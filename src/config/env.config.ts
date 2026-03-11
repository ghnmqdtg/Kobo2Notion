export interface EnvironmentConfig {
  SQLITE_SOURCE: string;
  NOTION_API_KEY: string;
  NOTION_DATABASE_ID: string;
  LLM_PROVIDER: string;
  LLM_API_KEY: string;
  LLM_API_KEY_GOOGLE: string;
  LLM_API_KEY_OPENAI: string;
  LLM_API_KEY_ANTHROPIC: string;
  LLM_MODEL: string;
  SUMMARIZE_ENABLED: boolean;
  SUMMARIZE_LANGUAGE: string;
  THEME: string;
}

// Providing defaults for missing values (or simply empty strings)
const defaultConfig: EnvironmentConfig = {
  SQLITE_SOURCE:
    process.platform === "darwin"
      ? "/Volumes/KOBOeReader/.kobo/KoboReader.sqlite"
      : "",
  NOTION_API_KEY: "",
  NOTION_DATABASE_ID: "",
  LLM_PROVIDER: "",
  LLM_API_KEY: "",
  LLM_API_KEY_GOOGLE: "",
  LLM_API_KEY_OPENAI: "",
  LLM_API_KEY_ANTHROPIC: "",
  LLM_MODEL: "",
  SUMMARIZE_ENABLED: false,
  SUMMARIZE_LANGUAGE: "zh",
  THEME: "light",
};

const providerKeyMap: Record<string, string> = {
  google: "LLM_API_KEY_GOOGLE",
  openai: "LLM_API_KEY_OPENAI",
  anthropic: "LLM_API_KEY_ANTHROPIC",
};

const initConfig = (): EnvironmentConfig => {
  // Backwards compat: map legacy GEMINI_API / GEMINI_MODEL env vars
  const legacyProvider = process.env.GEMINI_API ? "google" : "";
  const legacyApiKey = process.env.GEMINI_API || "";
  const legacyModel = process.env.GEMINI_MODEL || "";

  const provider =
    process.env.LLM_PROVIDER || legacyProvider || defaultConfig.LLM_PROVIDER;

  const perProviderKeys = {
    LLM_API_KEY_GOOGLE:
      process.env.LLM_API_KEY_GOOGLE || (legacyApiKey && provider === "google" ? legacyApiKey : "") || defaultConfig.LLM_API_KEY_GOOGLE,
    LLM_API_KEY_OPENAI:
      process.env.LLM_API_KEY_OPENAI || defaultConfig.LLM_API_KEY_OPENAI,
    LLM_API_KEY_ANTHROPIC:
      process.env.LLM_API_KEY_ANTHROPIC || defaultConfig.LLM_API_KEY_ANTHROPIC,
  };

  // Derive active API key: explicit LLM_API_KEY > per-provider key > legacy key
  const perProviderEnvKey = providerKeyMap[provider];
  const activeKey =
    process.env.LLM_API_KEY ||
    (perProviderEnvKey ? perProviderKeys[perProviderEnvKey] : "") ||
    legacyApiKey ||
    defaultConfig.LLM_API_KEY;

  const config: EnvironmentConfig = {
    SQLITE_SOURCE: process.env.SQLITE_SOURCE || defaultConfig.SQLITE_SOURCE,
    NOTION_API_KEY: process.env.NOTION_API || defaultConfig.NOTION_API_KEY,
    NOTION_DATABASE_ID:
      process.env.NOTION_DB || defaultConfig.NOTION_DATABASE_ID,
    LLM_PROVIDER: provider,
    LLM_API_KEY: activeKey,
    ...perProviderKeys,
    LLM_MODEL:
      process.env.LLM_MODEL || legacyModel || defaultConfig.LLM_MODEL,
    SUMMARIZE_ENABLED:
      process.env.SUMMARIZE_ENABLED === "true" ||
      defaultConfig.SUMMARIZE_ENABLED,
    SUMMARIZE_LANGUAGE:
      process.env.SUMMARIZE_LANGUAGE || defaultConfig.SUMMARIZE_LANGUAGE,
    THEME: process.env.THEME || defaultConfig.THEME,
  };

  const missingKeys = Object.entries(config)
    .filter(([_, value]) => !value)
    .map(([key]) => key);

  if (missingKeys.length > 0) {
    console.warn(
      `Incomplete config. Missing values for: ${missingKeys.join(", ")}. ` +
      "Display settings page to collect these values.",
    );
  }

  return config;
};

export const env = initConfig();
