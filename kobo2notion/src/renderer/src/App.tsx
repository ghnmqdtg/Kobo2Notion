import { Button } from "@/components/ui/button";
import { ThemeProvider } from "@/components/theme-provider";
import { ModeToggle } from "@/components/mode-toggle";

function App(): JSX.Element {

  return (
    <ThemeProvider>
      <h1 className="text-3xl font-bold underline">Hello world!</h1>
      <div>
        <ModeToggle />
      </div>
    </ThemeProvider>
  );
}

export default App;
