import { useState } from 'react';
import { Navbar } from '@/components/navbar';
import { Books } from '@/components/books';
import { Settings } from '@/components/settings';

function App(): JSX.Element {
  const [showSettings, setShowSettings] = useState(false);

  return (
    <div className="h-screen flex flex-col w-full">
      <Navbar onSettingsClick={() => setShowSettings(!showSettings)} />
      <main className="flex-1 overflow-hidden">
        <div className="container mx-auto h-full">
          {showSettings ? <Settings /> : <Books />}
        </div>
      </main>
    </div>
  );
}

export default App;
