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

interface SettingsValues {
  SQLITE_SOURCE: string;
  NOTION_API: string;
  NOTION_DB: string;
  GEMINI_API: string;
  SUMMARIZE_ENABLED: boolean;
  GEMINI_MODEL: string;
  SUMMARIZE_LANGUAGE: string;
}

export function Settings() {
  const [values, setValues] = useState<SettingsValues>({
    SQLITE_SOURCE: window.env.SQLITE_SOURCE || "",
    NOTION_API: window.env.NOTION_API_KEY || "",
    NOTION_DB: window.env.NOTION_DATABASE_ID || "",
    GEMINI_API: window.env.GEMINI_API_KEY || "",
    SUMMARIZE_ENABLED: window.env.SUMMARIZE_ENABLED || false,
    GEMINI_MODEL: window.env.GEMINI_MODEL || "gemini-1.5-flash",
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

  const handleSummarizeToggle = (enabled: boolean) => {
    setValues((prev) => ({
      ...prev,
      SUMMARIZE_ENABLED: enabled,
      GEMINI_MODEL: enabled ? "gemini-1.5-flash" : "",
      // Reset API key if disabled
      GEMINI_API: enabled ? prev.GEMINI_API : "",
    }));
  };

  const isValid = () => {
    const requiredFields = [
      values.SQLITE_SOURCE,
      values.NOTION_API,
      values.NOTION_DB,
    ];

    if (values.SUMMARIZE_ENABLED) {
      requiredFields.push(values.GEMINI_API);
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
      await window.api.updateEnvValue(entries);
      toast({
        title: "Settings saved successfully",
        description: "Please restart the app to apply changes",
        variant: "default",
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

  return (
    <>
      <div className="flex justify-between items-center p-4 pb-0">
        <h1 className="text-2xl font-bold">Settings</h1>
      </div>
      <div className="p-4 flex justify-center">
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
              <label className="text-md font-medium">Summarize Bookmarks</label>
              <Switch
                checked={values.SUMMARIZE_ENABLED}
                onCheckedChange={handleSummarizeToggle}
              />
            </div>

            {values.SUMMARIZE_ENABLED && (
              <>
                <div className="space-y-2">
                  <label className="text-md font-medium">Model</label>
                  <Select
                    value={values.GEMINI_MODEL}
                    onValueChange={(value) =>
                      handleChange("GEMINI_MODEL", value)
                    }
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="gemini-1.5-flash">
                        Gemini-1.5-flash
                      </SelectItem>
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
                  <label className="text-md font-medium">Gemini API Key</label>
                  <PasswordInput
                    value={values.GEMINI_API}
                    onChange={(e) => handleChange("GEMINI_API", e.target.value)}
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
    </>
  );
}
