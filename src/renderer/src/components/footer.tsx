import { Button } from '@/components/ui/button';
import { Progress } from '@/components/ui/progress';
import { useNetworkState } from '@uidotdev/usehooks';
import { AlertCircle, Loader2 } from 'lucide-react';

interface FooterProps {
    selectedCount: number;
    totalSelected: number;
    isExporting: boolean;
    currentBook?: string;
    currentStep?: string;
    onExport: () => void;
}

export function Footer({
    selectedCount,
    totalSelected,
    isExporting,
    currentBook,
    currentStep,
    onExport
}: FooterProps) {
    const isOnline = useNetworkState();

    return (
        <div className="w-4/5 fixed bottom-0 bg-background backdrop-blur-none border-t">
            <div className="container mx-auto px-4 h-16 flex items-center justify-between">
                <div className="flex flex-col gap-2 flex-1 mr-4">
                    {!isOnline.online ? (
                        <div className="flex items-center space-x-2 text-destructive/95">
                            <AlertCircle className="h-4 w-4" />
                            <span className="text-md font-bold">Network is not available. Please check your network settings.</span>
                        </div>
                    ) : isExporting ? (
                        <>
                            <div className="flex justify-between text-sm text-muted-foreground">
                                <span>{currentBook}</span>
                                <span>{currentStep}</span>
                            </div>
                            <Progress value={(selectedCount / totalSelected) * 100} />
                        </>
                    ) : (
                        <div className="text-md font-bold">
                            {selectedCount > 0
                                ? `${selectedCount} book${selectedCount > 1 ? 's' : ''} selected`
                                : 'Select books to export'
                            }
                        </div>
                    )}
                </div>
                <Button
                    className={`text-md font-bold ${!isOnline.online ? 'bg-destructive/60' : ''}`}
                    onClick={onExport}
                    disabled={selectedCount === 0 || isExporting || !isOnline.online}
                >
                    {!isOnline.online
                        ? 'No Network Connection ;('
                        : isExporting
                            ? <div className="flex items-center space-x-2">
                                <Loader2 className="h-4 w-4 animate-spin" />
                                <span>Exporting...</span>
                            </div>
                            : 'Export to Notion'
                    }
                </Button>
            </div>
        </div>
    );
} 