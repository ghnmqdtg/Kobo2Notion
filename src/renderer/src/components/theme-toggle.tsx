import { Moon, Sun } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useTheme } from "@/components/theme-provider";

export function ThemeToggle() {
  const { theme, setTheme } = useTheme();

  const handleThemeChange = async () => {
    const newTheme = theme === "light" ? "dark" : "light";
    try {
      await window.api.updateEnvValue([{ key: "THEME", value: newTheme }]).then(() => {
        setTheme(newTheme);
      });
    } catch (error) {
      console.error("Failed to update theme:", error);
    }
  };

  return (
    <Button variant="ghost" size="icon" onClick={handleThemeChange}>
      {theme === "light" ? (
        <Sun className="h-5 w-5" />
      ) : (
        <Moon className="h-5 w-5" />
      )}
      <span className="sr-only">Toggle theme</span>
    </Button>
  );
}
