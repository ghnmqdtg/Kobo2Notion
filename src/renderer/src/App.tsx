import { useState, useEffect } from "react";
import { Navbar } from "@/components/navbar";
import { Books } from "@/components/books";
import { Settings } from "@/components/settings";
import { Toaster } from "@/components/ui/toaster";
import { ThemeProvider } from "@/components/theme-provider";

function App(): JSX.Element {
  const [showSettings, setShowSettings] = useState(false);
  const [isFirstTime, setIsFirstTime] = useState(true);
  const [isExporting, setIsExporting] = useState(false);
  const [isCanceling, setIsCanceling] = useState(false);

  useEffect(() => {
    // Check if all required env values are set
    const requiredEnvs = [
      "SQLITE_SOURCE",
      "NOTION_API_KEY",
      "NOTION_DATABASE_ID",
    ];

    const missingEnvs = requiredEnvs.filter((key) => !window.env[key]);
    const isFirstTimeSetup = missingEnvs.length > 0;

    setIsFirstTime(isFirstTimeSetup);
    if (isFirstTimeSetup) {
      setShowSettings(true);
    }
  }, []);

  return (
    <ThemeProvider>
      <div className="h-screen flex flex-col w-full">
        <Navbar
          onSettingsClick={() => setShowSettings(!showSettings)}
          isFirstTime={isFirstTime}
          isExporting={isExporting}
          isCanceling={isCanceling}
        />
        <main className="flex-1">
          <div className="container mx-auto h-full">
            {showSettings ? (
              <Settings />
            ) : (
              <Books
                onExportStateChange={(exporting: boolean, canceling: boolean) => {
                  setIsExporting(exporting);
                  setIsCanceling(canceling);
                }}
              />
            )}
          </div>
        </main>
        <Toaster />
      </div>
    </ThemeProvider>
  );
}

export default App;
