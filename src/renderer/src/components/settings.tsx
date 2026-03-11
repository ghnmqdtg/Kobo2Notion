import { useState, useEffect } from "react";
import { Input } from "@/components/ui/input";
import { PasswordInput } from "@/components/ui/password-input";
import { Separator } from "@/components/ui/separator";
import { Button } from "@/components/ui/button";
import { Switch } from "@/components/ui/switch";
import { useToast } from "@/hooks/use-toast";
import { ToastAction } from "@/components/ui/toast";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { FolderOpen } from "lucide-react";
import { ScrollArea } from "@/components/ui/scroll-area";

interface SettingsValues {
  SQLITE_SOURCE: string;
  NOTION_API: string;
  NOTION_DB: string;
  LLM_PROVIDER: string;
  LLM_API_KEY: string;
  LLM_MODEL: string;
  SUMMARIZE_ENABLED: boolean;
  SUMMARIZE_LANGUAGE: string;
}

const modelsByProvider: Record<string, string[]> = {
  google: ["gemini-2.5-flash", "gemini-2.5-pro", "gemini-2.0-flash"],
  openai: ["gpt-4o", "gpt-4o-mini", "gpt-4.1", "gpt-4.1-mini"],
  anthropic: ["claude-sonnet-4-5-20250514", "claude-haiku-4-5-20251001"],
};

const providerLabels: Record<string, string> = {
  google: "Google Gemini",
  openai: "OpenAI",
  anthropic: "Anthropic Claude",
};

export function Settings() {
  const [values, setValues] = useState<SettingsValues>({
    SQLITE_SOURCE: window.env.SQLITE_SOURCE || "",
    NOTION_API: window.env.NOTION_API_KEY || "",
    NOTION_DB: window.env.NOTION_DATABASE_ID || "",
    LLM_PROVIDER: window.env.LLM_PROVIDER || "",
    LLM_API_KEY: window.env.LLM_API_KEY || "",
    LLM_MODEL: window.env.LLM_MODEL || "",
    SUMMARIZE_ENABLED: window.env.SUMMARIZE_ENABLED || false,
    SUMMARIZE_LANGUAGE: window.env.SUMMARIZE_LANGUAGE || "en",
  });
  const [isSaving, setIsSaving] = useState(false);
  const [isFirstTime, setIsFirstTime] = useState(true);
  const { toast } = useToast();

  useEffect(() => {
    // Check if it's first time setup
    setIsFirstTime(
      !values.SQLITE_SOURCE && !values.NOTION_API && !values.NOTION_DB,
    );
  }, []);

  const validateSqlitePath = (path: string): boolean => {
    return path.toLowerCase().includes("koboreader.sqlite");
  };

  const handleChange = (key: string, value: string | boolean) => {
    setValues((prev) => ({ ...prev, [key]: value }));
  };

  const handleProviderChange = (provider: string) => {
    const models = modelsByProvider[provider] || [];
    setValues((prev) => ({
      ...prev,
      LLM_PROVIDER: provider,
      LLM_MODEL: models[0] || "",
    }));
  };

  const handleSummarizeToggle = (enabled: boolean) => {
    setValues((prev) => ({
      ...prev,
      SUMMARIZE_ENABLED: enabled,
      LLM_PROVIDER: enabled ? prev.LLM_PROVIDER || "google" : prev.LLM_PROVIDER,
      LLM_MODEL: enabled
        ? prev.LLM_MODEL || modelsByProvider[prev.LLM_PROVIDER || "google"]?.[0] || ""
        : prev.LLM_MODEL,
    }));
  };

  const isValid = () => {
    const requiredFields = [
      values.SQLITE_SOURCE,
      values.NOTION_API,
      values.NOTION_DB,
    ];

    if (values.SUMMARIZE_ENABLED) {
      requiredFields.push(values.LLM_API_KEY);
    }

    return requiredFields.every((field) => field.trim() !== "");
  };

  const handleSave = async () => {
    setIsSaving(true);
    try {
      const entries = Object.entries(values).map(([key, value]) => ({
        key,
        value: typeof value === "boolean" ? value.toString() : value,
      }));
      await window.api.updateEnvValue(entries).then(() => {
        toast({
          title: "Settings saved successfully",
          description: "Please restart the app to apply changes",
          variant: "default",
        });

        setTimeout(() => {
          // Reload the page after env values are updated
          window.location.reload();
        }, 500);
      });
    } catch (error) {
      toast({
        title: "Failed to save settings",
        description: "Please check your settings and try again",
        variant: "destructive",
      });
      console.error("Error saving settings:", error);
    } finally {
      setIsSaving(false);
    }
  };

  const handleFilePick = async () => {
    try {
      const filePath = await window.api.openFileDialog();
      if (filePath) {
        if (!validateSqlitePath(filePath)) {
          toast({
            title: "Invalid file path",
            description: "Must be KoboReader.sqlite",
            action: <ToastAction altText="Try again"> Try again</ToastAction>,
          });
          return;
        }
        handleChange("SQLITE_SOURCE", filePath);
      }
    } catch (error) {
      console.error("Error picking file:", error);
      toast({
        title: "Failed to select file",
        description: "Please try again",
        variant: "destructive",
      });
    }
  };

  const availableModels = modelsByProvider[values.LLM_PROVIDER] || [];

  return (
    <>
      <ScrollArea className="h-[calc(100vh-8rem)]">
        <div className="flex justify-between items-center p-4 pb-0">
          <h1 className="text-2xl font-bold">Settings</h1>
        </div>
        <div className="p-4 flex justify-center mt-4 md:mt-8 lg:mt-12 2xl:mt-24">
          <div className="grid gap-6 w-full lg:w-1/2 xl:w-2/5">
            <div className="space-y-2">
              <label className="text-md font-medium">
                Kobo Highlights File Path
              </label>
              <div className="flex space-x-2">
                <Input
                  type="text"
                  value={values.SQLITE_SOURCE}
                  placeholder="/Volumes/KOBOeReader/.kobo/KoboReader.sqlite"
                  className={
                    !validateSqlitePath(values.SQLITE_SOURCE) &&
                      values.SQLITE_SOURCE
                      ? "border-destructive"
                      : ""
                  }
                  readOnly
                />
                <Button
                  variant="outline"
                  size="icon"
                  onClick={handleFilePick}
                  title="Choose file"
                >
                  <FolderOpen className="h-4 w-4" />
                </Button>
              </div>
              {values.SQLITE_SOURCE &&
                !validateSqlitePath(values.SQLITE_SOURCE) && (
                  <p className="text-sm text-destructive">
                    File must be KoboReader.sqlite
                  </p>
                )}
            </div>
            <Separator />
            <div className="space-y-2">
              <label className="text-md font-medium">Notion API Key</label>
              <PasswordInput
                value={values.NOTION_API}
                onChange={(e) => handleChange("NOTION_API", e.target.value)}
              />
            </div>
            <div className="space-y-2">
              <label className="text-md font-medium">Notion Database ID</label>
              <PasswordInput
                value={values.NOTION_DB}
                onChange={(e) => handleChange("NOTION_DB", e.target.value)}
              />
            </div>
            <Separator />
            <div className="space-y-4">
              <div className="flex items-center justify-between">
                <label className="text-md font-medium">
                  Summarize Bookmarks
                </label>
                <Switch
                  checked={values.SUMMARIZE_ENABLED}
                  onCheckedChange={handleSummarizeToggle}
                />
              </div>

              {values.SUMMARIZE_ENABLED && (
                <>
                  <div className="space-y-2">
                    <label className="text-md font-medium">Provider</label>
                    <Select
                      value={values.LLM_PROVIDER}
                      onValueChange={handleProviderChange}
                    >
                      <SelectTrigger>
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        {Object.entries(providerLabels).map(([value, label]) => (
                          <SelectItem key={value} value={value}>
                            {label}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>

                  <div className="space-y-2">
                    <label className="text-md font-medium">Model</label>
                    <Select
                      value={values.LLM_MODEL}
                      onValueChange={(value) =>
                        handleChange("LLM_MODEL", value)
                      }
                    >
                      <SelectTrigger>
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        {availableModels.map((model) => (
                          <SelectItem key={model} value={model}>
                            {model}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>

                  <div className="space-y-2">
                    <label className="text-md font-medium">
                      Summary Language
                    </label>
                    <Select
                      value={values.SUMMARIZE_LANGUAGE}
                      onValueChange={(value) =>
                        handleChange("SUMMARIZE_LANGUAGE", value)
                      }
                    >
                      <SelectTrigger>
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="en">English</SelectItem>
                        <SelectItem value="zh">
                          繁體中文 Traditional Chinese
                        </SelectItem>
                      </SelectContent>
                    </Select>
                  </div>

                  <div className="space-y-2">
                    <label className="text-md font-medium">
                      API Key
                    </label>
                    <PasswordInput
                      value={values.LLM_API_KEY}
                      onChange={(e) =>
                        handleChange("LLM_API_KEY", e.target.value)
                      }
                    />
                  </div>
                </>
              )}
            </div>

            <Separator />
            <div className="flex justify-center space-x-2">
              {!isFirstTime && (
                <Button
                  variant="outline"
                  className="w-full text-md font-bold"
                  onClick={() => window.location.reload()}
                >
                  Cancel
                </Button>
              )}
              <Button
                className="w-full text-md font-bold"
                onClick={handleSave}
                disabled={!isValid() || isSaving}
              >
                {isSaving ? "Saving..." : "Save"}
              </Button>
            </div>
          </div>
        </div>
      </ScrollArea>
    </>
  );
}
