import { Button } from '@/components/ui/button';
import { AlertCircle, Loader2 } from 'lucide-react';

interface ErrorDisplayProps {
  error: string;
  onRetry: () => void;
  retryCount: number;
  maxRetries: number;
}

export function ErrorDisplay({
  error,
  onRetry,
  retryCount,
  maxRetries
}: ErrorDisplayProps): React.JSX.Element {
  const [title, ...lines] = error.split('\n');
  const isRetrying = retryCount < maxRetries;

  return (
    <div className="flex h-full flex-col items-center justify-center gap-8 px-6">
      <div className="flex max-w-md flex-col items-center gap-4 text-center">
        <div className="flex size-14 items-center justify-center rounded-full bg-destructive/10">
          <AlertCircle className="size-7 text-destructive" />
        </div>
        <div className="space-y-1.5">
          <h2 className="text-lg font-semibold">{title}</h2>
          {lines.length > 0 && (
            <p className="whitespace-pre-line text-sm text-muted-foreground">
              {lines.join('\n')}
            </p>
          )}
        </div>
      </div>
      <Button variant="outline" onClick={onRetry} disabled={isRetrying}>
        {isRetrying ? (
          <>
            <Loader2 className="animate-spin" />
            Retrying... ({retryCount}/{maxRetries})
          </>
        ) : (
          'Retry'
        )}
      </Button>
    </div>
  );
}
