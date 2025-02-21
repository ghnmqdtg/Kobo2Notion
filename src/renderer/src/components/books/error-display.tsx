import { Button } from "@/components/ui/button";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { AlertCircle, Loader2 } from "lucide-react";

interface ErrorDisplayProps {
    error: string;
    onRetry: () => void;
    retryCount: number;
    maxRetries: number;
}

export function ErrorDisplay({ error, onRetry, retryCount, maxRetries }: ErrorDisplayProps) {
    return (
        <div className="flex flex-col items-center justify-center h-full space-y-6">
            <Alert variant="destructive" className="max-w-lg rounded-md">
                <div className="flex items-center space-x-3">
                    <AlertCircle className="h-6 w-6" />
                    <AlertDescription className="text-lg whitespace-pre-line">
                        {error}
                    </AlertDescription>
                </div>
            </Alert>
            <Button
                className="text-md font-bold"
                onClick={onRetry}
                variant="outline"
                disabled={retryCount < maxRetries}
            >
                {retryCount < maxRetries ? (
                    <>
                        <Loader2 className="h-4 w-4 animate-spin" />
                        <span className="ml-1">
                            Retrying... ({retryCount}/{maxRetries})
                        </span>
                    </>
                ) : (
                    "Retry"
                )}
            </Button>
        </div>
    );
} 