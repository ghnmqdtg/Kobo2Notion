export interface EnvironmentConfig {
  SQLITE_SOURCE: string;
  NOTION_API_KEY: string;
  NOTION_DATABASE_ID: string;
  GEMINI_API_KEY: string;
  GEMINI_MODEL: string;
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
  GEMINI_API_KEY: "",
  GEMINI_MODEL: "gemini-2.0-flash",
  SUMMARIZE_ENABLED: false,
  SUMMARIZE_LANGUAGE: "zh",
  THEME: "light",
};

const initConfig = (): EnvironmentConfig => {
  const config: EnvironmentConfig = {
    SQLITE_SOURCE: process.env.SQLITE_SOURCE || defaultConfig.SQLITE_SOURCE,
    NOTION_API_KEY: process.env.NOTION_API || defaultConfig.NOTION_API_KEY,
    NOTION_DATABASE_ID:
      process.env.NOTION_DB || defaultConfig.NOTION_DATABASE_ID,
    GEMINI_API_KEY: process.env.GEMINI_API || defaultConfig.GEMINI_API_KEY,
    GEMINI_MODEL: process.env.GEMINI_MODEL || defaultConfig.GEMINI_MODEL,
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
