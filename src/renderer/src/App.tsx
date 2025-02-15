import { useState, useEffect } from 'react';
import { Navbar } from '@/components/navbar';
import { Books } from '@/components/books';
import { Settings } from '@/components/settings';
import { Toaster } from "@/components/ui/toaster";
import { ThemeProvider } from '@/components/theme-provider';

function App(): JSX.Element {
  const [showSettings, setShowSettings] = useState(false);

  useEffect(() => {
    // Check if all required env values are set
    const requiredEnvs = [
      'SQLITE_SOURCE',
      'NOTION_API_KEY',
      'NOTION_DATABASE_ID',
      'GEMINI_API_KEY'
    ];

    const missingEnvs = requiredEnvs.filter(key => !window.env[key]);
    console.log('missingEnvs', missingEnvs);
    if (missingEnvs.length > 0) {
      setShowSettings(true);
    }
  }, []);

  return (
    <ThemeProvider>
      <div className="h-screen flex flex-col w-full">
        <Navbar onSettingsClick={() => setShowSettings(!showSettings)} />
        <main className="flex-1 overflow-auto">
          <div className="container mx-auto h-full">
            {showSettings ? <Settings /> : <Books />}
          </div>
        </main>
        <Toaster />
      </div>
    </ThemeProvider>
  );
}

export default App;
