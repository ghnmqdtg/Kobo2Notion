import { Settings } from "lucide-react";
import { Button } from "@/components/ui/button";
import { ThemeToggle } from "./theme-toggle";
import { Logo } from "./logo";

interface NavbarProps {
  onSettingsClick: () => void;
  isFirstTime?: boolean;
  isExporting?: boolean;
  isCanceling?: boolean;
  isChecking?: boolean;
}

export function Navbar({ onSettingsClick, isFirstTime, isExporting, isCanceling, isChecking }: NavbarProps) {
  const isDisabled = isFirstTime || isExporting || isCanceling || isChecking;
  const getTooltipText = () => {
    if (isFirstTime) return "Please complete the initial setup first";
    if (isChecking) return "Please wait while checking existing pages";
    if (isExporting || isCanceling) return "Please wait until the export is complete";
    return "Settings";
  };

  return (
    <nav className="border-b">
      <div className="flex h-16 items-center px-4">
        <Logo />
        <div className="ml-auto flex items-center space-x-2">
          <ThemeToggle disabled={isDisabled} />
          <Button
            variant="ghost"
            size="icon"
            onClick={onSettingsClick}
            disabled={isDisabled}
            title={getTooltipText()}
          >
            <Settings className="h-5 w-5" />
          </Button>
        </div>
      </div>
    </nav>
  );
}
