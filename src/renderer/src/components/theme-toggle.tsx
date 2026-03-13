import { Moon, Sun } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useTheme } from "@/components/theme-provider";

export function ThemeToggle({
  disabled,
}: {
  disabled: boolean;
}): React.JSX.Element {
  const { theme, setTheme } = useTheme();

  const handleThemeChange = async (): Promise<void> => {
    const newTheme = theme === "light" ? "dark" : "light";
    try {
      await window.api
        .updateEnvValue([{ key: "THEME", value: newTheme }])
        .then(() => {
          setTheme(newTheme);
        });
    } catch (err) {
      console.error("Failed to update theme:", err);
    }
  };

  return (
    <Button
      variant="ghost"
      size="icon"
      onClick={handleThemeChange}
      disabled={disabled}
    >
      {theme === "light" ? (
        <Sun className="h-5 w-5" />
      ) : (
        <Moon className="h-5 w-5" />
      )}
      <span className="sr-only">Toggle theme</span>
    </Button>
  );
}
