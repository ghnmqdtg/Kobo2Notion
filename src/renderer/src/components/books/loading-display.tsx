import { Loader2 } from "lucide-react";

export function LoadingDisplay(): React.JSX.Element {
  return (
    <div className="flex flex-col items-center justify-center h-full space-y-4">
      <Loader2 className="h-8 w-8 animate-spin text-primary" />
      <p className="text-lg text-muted-foreground">Loading books...</p>
    </div>
  );
}
